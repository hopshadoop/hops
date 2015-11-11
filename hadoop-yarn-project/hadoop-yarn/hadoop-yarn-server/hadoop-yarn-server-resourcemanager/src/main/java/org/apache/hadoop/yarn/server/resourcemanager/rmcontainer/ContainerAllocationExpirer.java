/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ContainerAllocationExpirer
    extends AbstractLivelinessMonitor<ContainerId> {

  private final EventHandler dispatcher;
  private final RMContext rmContext;

  public ContainerAllocationExpirer(Dispatcher d, RMContext rmContext) {
    super(ContainerAllocationExpirer.class.getName(), new SystemClock());
    this.dispatcher = d.getEventHandler();
    this.rmContext = rmContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    int expireIntvl =
        conf.getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
    setExpireInterval(expireIntvl);
    setMonitorInterval(expireIntvl / 3);
    super.serviceInit(conf);
  }

  @Override
  protected void expire(ContainerId containerId) {
    try {
      TransactionState ts = rmContext.getTransactionStateManager().
              getCurrentTransactionStatePriority(-1,
                      "ContainerAllocationExpirer");
      dispatcher.handle(new ContainerExpiredSchedulerEvent(containerId, ts));
      ts.decCounter(TransactionState.TransactionType.INIT);
    } catch (IOException ex) {
      Logger.getLogger(ContainerAllocationExpirer.class.getName())
          .log(Level.SEVERE, null, ex);
    }
  }
}
