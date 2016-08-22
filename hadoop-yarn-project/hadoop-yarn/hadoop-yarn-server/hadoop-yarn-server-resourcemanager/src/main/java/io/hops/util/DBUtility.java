/*
 * Copyright 2016 Apache Software Foundation.
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
package io.hops.util;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.NodeId;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class DBUtility {

  public static void removeContainersToClean(final Set<ContainerId> containers,
          final NodeId nodeId) throws IOException {
    LightWeightRequestHandler removeContainerToClean
            = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ContainerIdToCleanDataAccess ctcDA
                = (ContainerIdToCleanDataAccess) RMStorageFactory
                .getDataAccess(ContainerIdToCleanDataAccess.class);
        List<io.hops.metadata.yarn.entity.ContainerId> containersToClean
                = new ArrayList<io.hops.metadata.yarn.entity.ContainerId>();
        for (ContainerId cid : containers) {
          containersToClean.add(new io.hops.metadata.yarn.entity.ContainerId(
                  nodeId.toString(), cid.toString()));
        }
        ctcDA.removeAll(containers);
        connector.commit();
        return null;
      }
    };
    removeContainerToClean.handle();
  }

  public static void removeFinishedApplications(
          final List<ApplicationId> finishedApplications, final NodeId nodeId)
          throws IOException {
    LightWeightRequestHandler removeFinishedApplication
            = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        FinishedApplicationsDataAccess faDA
                = (FinishedApplicationsDataAccess) RMStorageFactory
                .getDataAccess(FinishedApplicationsDataAccess.class);
        List<FinishedApplications> finishedApps
                = new ArrayList<FinishedApplications>();
        for (ApplicationId appId : finishedApplications) {
          finishedApps.add(new FinishedApplications(nodeId.toString(), appId.
                  toString()));
        }
        faDA.removeAll(finishedApps);
        connector.commit();
        return null;
      }
    };
    removeFinishedApplication.handle();
  }

  public static void addFinishedApplication(final ApplicationId appId,
          final NodeId nodeId) throws
          IOException {
    LightWeightRequestHandler addFinishedApplication
            = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        FinishedApplicationsDataAccess faDA
                = (FinishedApplicationsDataAccess) RMStorageFactory
                .getDataAccess(FinishedApplicationsDataAccess.class);
        faDA.add(new FinishedApplications(nodeId.toString(), appId.toString()));
        connector.commit();
        return null;
      }
    };
    addFinishedApplication.handle();
  }

  public static void addContainerToClean(final ContainerId containerId,
          final NodeId nodeId) throws
          IOException {
    LightWeightRequestHandler addContainerToClean
            = new LightWeightRequestHandler(
                    YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ContainerIdToCleanDataAccess ctcDA
                = (ContainerIdToCleanDataAccess) RMStorageFactory
                .getDataAccess(ContainerIdToCleanDataAccess.class);
        ctcDA.add(
                new io.hops.metadata.yarn.entity.ContainerId(nodeId.toString(),
                        containerId));
        connector.commit();
        return null;
      }
    };
    addContainerToClean.handle();
  }
}
