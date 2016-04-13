/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import io.hops.exception.StorageException;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.util.HopYarnAPIUtilities;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;

public class TestRecoverLeafCSQueue {

  private static final Log LOG = LogFactory.getLog(TestRecoverLeafCSQueue.class);

  private final RecordFactory recordFactory
          = RecordFactoryProvider.getRecordFactory(null);

  RMContext rmContext;
  CapacityScheduler cs;
  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;

  CSQueue root;
  Map<String, CSQueue> queues = new HashMap<String, CSQueue>();

  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";
  final static float DELTA = 0.0001f;

  private final ResourceCalculator resourceCalculator
          = new DefaultResourceCalculator();

  @Before
  public void setUp() throws Exception {
    CapacityScheduler spyCs = new CapacityScheduler();
    cs = spy(spyCs);
    rmContext = TestUtils.getMockRMContext();

    csConf
            = new CapacitySchedulerConfiguration();
    csConf.setBoolean("yarn.scheduler.capacity.user-metrics.enable", true);
    final String newRoot = "root" + System.currentTimeMillis();
    setupQueueConfiguration(csConf, newRoot);
    YarnConfiguration conf = new YarnConfiguration();
    cs.setConf(conf);

    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMStorageFactory.getConnector().formatStorage();

    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).
            thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
            thenReturn(Resources.createResource(16 * GB, 32));
    when(csContext.getClusterResources()).
            thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getApplicationComparator()).
            thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getQueueComparator()).
            thenReturn(CapacityScheduler.queueComparator);
    when(csContext.getResourceCalculator()).
            thenReturn(resourceCalculator);
    RMContainerTokenSecretManager containerTokenSecretManager
            = new RMContainerTokenSecretManager(conf, rmContext);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
            containerTokenSecretManager);

    root
            = CapacityScheduler.parseQueue(csContext, csConf, null,
                    CapacitySchedulerConfiguration.ROOT,
                    queues, queues,
                    TestUtils.spyHook, null);

    cs.reinitialize(csConf, rmContext, null);
    RMStorageFactory.setConfiguration(conf);
    YarnAPIStorageFactory.setConfiguration(conf);

  }

  private static final String A = "a";
  private static final String B = "b";
  private static final String C = "c";
  private static final String C1 = "c1";
  private static final String D = "d";
  private static final String E = "e";

  private void setupQueueConfiguration(
          CapacitySchedulerConfiguration conf,
          final String newRoot) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{newRoot});
    conf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100);
    conf.setAcl(CapacitySchedulerConfiguration.ROOT,
            QueueACL.SUBMIT_APPLICATIONS, " ");

    final String Q_newRoot = CapacitySchedulerConfiguration.ROOT + "." + newRoot;
    conf.setQueues(Q_newRoot, new String[]{A, B, C, D, E});
    conf.setCapacity(Q_newRoot, 100);
    conf.setMaximumCapacity(Q_newRoot, 100);
    conf.setAcl(Q_newRoot, QueueACL.SUBMIT_APPLICATIONS, " ");

    final String Q_A = Q_newRoot + "." + A;
    conf.setCapacity(Q_A, 8.5f);
    conf.setMaximumCapacity(Q_A, 20);
    conf.setAcl(Q_A, QueueACL.SUBMIT_APPLICATIONS, "*");

    final String Q_B = Q_newRoot + "." + B;
    conf.setCapacity(Q_B, 80);
    conf.setMaximumCapacity(Q_B, 99);
    conf.setAcl(Q_B, QueueACL.SUBMIT_APPLICATIONS, "*");

    final String Q_C = Q_newRoot + "." + C;
    conf.setCapacity(Q_C, 1.5f);
    conf.setMaximumCapacity(Q_C, 10);
    conf.setAcl(Q_C, QueueACL.SUBMIT_APPLICATIONS, " ");

    conf.setQueues(Q_C, new String[]{C1});

    final String Q_C1 = Q_C + "." + C1;
    conf.setCapacity(Q_C1, 100);

    final String Q_D = Q_newRoot + "." + D;
    conf.setCapacity(Q_D, 9);
    conf.setMaximumCapacity(Q_D, 11);
    conf.setAcl(Q_D, QueueACL.SUBMIT_APPLICATIONS, "user_d");

    final String Q_E = Q_newRoot + "." + E;
    conf.setCapacity(Q_E, 1);
    conf.setMaximumCapacity(Q_E, 1);
    conf.setAcl(Q_E, QueueACL.SUBMIT_APPLICATIONS, "user_e");
  }

  private static ApplicationId getApplicationId(int id) {
    return ApplicationId.newInstance(123456, id);
  }

  private void persistAppInfo(final SchedulerApplication application)
          throws IOException {
    LightWeightRequestHandler setVersionHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
              @Override
              public Object performTask() throws StorageException {
                connector.beginTransaction();
                connector.writeLock();
                SchedulerApplicationDataAccess sappDA
                = (SchedulerApplicationDataAccess) RMStorageFactory
                .getDataAccess(SchedulerApplicationDataAccess.class);
                List<SchedulerApplication> toAdd
                = new ArrayList<SchedulerApplication>();
                toAdd.add(application);
                sappDA.addAll(toAdd);
                connector.commit();
                return null;
              }
            };
    setVersionHandler.handle();
  }

}
