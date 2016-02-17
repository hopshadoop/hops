/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import io.hops.ha.common.TransactionState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

@Private
@Unstable
public class FSSchedulerNode extends SchedulerNode {

  private static final Log LOG = LogFactory.getLog(FSSchedulerNode.class);

  private AppSchedulable reservedAppSchedulable;

  public FSSchedulerNode(RMNode node, boolean usePortForNodeName) {
    super(node, usePortForNodeName);
  }

  // Until we implement RECOVERY for Fair Scheduler
  // Normally FSSchedulerNode and FiCaSchedulerNode should use the same
  // signature for allocateContainer inherited from SchedulerNode,
  // but FSSchedulerNode currently does not support recovery
  public synchronized void allocateContainer(ApplicationId applicationId,
                                             RMContainer rmContainer) {
    allocateContainer(applicationId, rmContainer, null);
  }

  // Until we implement RECOVERY for Fair Scheduler
  // Normally FSSchedulerNode and FiCaSchedulerNode should use the same
  // signature for releaseContainer inherited from SchedulerNode,
  // but FSSchedulerNode currently does not support recovery
  public synchronized void releaseContainer(Container container) {
    releaseContainer(container, null);
  }

  // Until we implement RECOVERY for Fair Scheduler
  // Normally FSSchedulerNode and FiCaSchedulerNode should use the same
  // signature for reserveResource inherited from SchedulerNode,
  // but FSSchedulerNode currently does not support recovery
  public synchronized void reserveResource(SchedulerApplicationAttempt application,
                                           Priority priority, RMContainer container) {
    reserveResource(application, priority, container, null);
  }

  @Override
  public synchronized void reserveResource(SchedulerApplicationAttempt application,
      Priority priority, RMContainer container, TransactionState transactionState) {
    // Check if it's already reserved
    RMContainer reservedContainer = getReservedContainer();
    if (reservedContainer != null) {
      // Sanity check
      if (!container.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container +
            " on node " + container.getReservedNode() +
            " when currently" + " reserved resource " + reservedContainer +
            " on node " + reservedContainer.getReservedNode());
      }
      
      // Cannot reserve more than one application on a given node!
      if (!reservedContainer.getContainer().getId()
          .getApplicationAttemptId().equals(
              container.getContainer().getId()
                  .getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container +
            " for application " + application.getApplicationId() +
            " when currently" +
            " reserved container " + reservedContainer +
            " on node " + this);
      }

      LOG.info("Updated reserved container " +
          container.getContainer().getId() + " on node " +
          this + " for application " + application);
    } else {
      LOG.info(
          "Reserved container " + container.getContainer().getId() +
              " on node " + this + " for application " + application);
    }
    setReservedContainer(container, transactionState);
    this.reservedAppSchedulable = ((FSSchedulerApp) application).getAppSchedulable();
  }

  // Until we implement RECOVERY for Fair Scheduler
  // Normally FSSchedulerNode and FiCaSchedulerNode should use the same
  // signature for unreserveResource inherited from SchedulerNode,
  // but FSSchedulerNode currently does not support recovery
  public synchronized void unreserveResource(SchedulerApplicationAttempt application) {
    unreserveResource(application, null);
  }

  @Override
  public synchronized void unreserveResource(SchedulerApplicationAttempt application,
                                             TransactionState transactionState) {
    // Cannot unreserve for wrong application...
    ApplicationAttemptId reservedApplication =
        reservedContainer.getContainer().getId().getApplicationAttemptId();
    if (!reservedApplication.equals(application.getApplicationAttemptId())) {
      throw new IllegalStateException("Trying to unreserve " +
          " for application " + application.getApplicationId() +
          " when currently reserved " +
          " for application " + reservedApplication.getApplicationId() +
          " on node " + this);
    }
    
    setReservedContainer(null, transactionState);
    this.reservedAppSchedulable = null;
  }

  public synchronized AppSchedulable getReservedAppSchedulable() {
    return reservedAppSchedulable;
  }

  // Until we implement RECOVERY for Fair Scheduler
  // Normally FSSchedulerNode and FiCaSchedulerNode should use the same
  // signature for applyDeltaOnAvailableResource inherited from SchedulerNode,
  // but FSSchedulerNode currently does not support recovery
  public synchronized void applyDeltaOnAvailableResource(Resource deltaResource) {
    applyDeltaOnAvailableResource(deltaResource);
  }
}
