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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import io.hops.ha.common.TransactionState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractYarnScheduler
        <T extends SchedulerApplicationAttempt, N extends SchedulerNode>
        extends AbstractService
    implements ResourceScheduler {

  private static final Log LOG = LogFactory.getLog(AbstractYarnScheduler.class);

  // Nodes in the cluster, indexed by NodeId
  // Recovered
  protected Map<NodeId, N> nodes =
          new ConcurrentHashMap<NodeId, N>();

  // Whole capacity of the cluster
  // Recovered
  protected Resource clusterResource = Resource.newInstance(0, 0);

  protected Resource minimumAllocation; // from config
  protected Resource maximumAllocation; // from config

  protected RMContext rmContext;
  protected Map<ApplicationId, SchedulerApplication<T>> applications;
      //recovered in FIFO scheduler
  protected final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();//recovered
  protected static final Allocation EMPTY_ALLOCATION =
      new Allocation(EMPTY_CONTAINER_LIST, Resources.createResource(0), null,
          null, null);//recovered
  
   /**
   * Construct the service.
   *
   * @param name service name
   */
  public AbstractYarnScheduler(String name) {
    super(name);
  }


  public synchronized List<Container> getTransferredContainers(
      ApplicationAttemptId currentAttempt) {
    ApplicationId appId = currentAttempt.getApplicationId();
    SchedulerApplication<T> app = applications.get(appId);
    List<Container> containerList = new ArrayList<Container>();
    RMApp appImpl = this.rmContext.getRMApps().get(appId);
    if (appImpl.getApplicationSubmissionContext().getUnmanagedAM()) {
      return containerList;
    }
    Collection<RMContainer> liveContainers =
        app.getCurrentAppAttempt().getLiveContainers();
    ContainerId amContainerId =
        rmContext.getRMApps().get(appId).getCurrentAppAttempt()
            .getMasterContainer().getId();
    for (RMContainer rmContainer : liveContainers) {
      if (!rmContainer.getContainerId().equals(amContainerId)) {
        containerList.add(rmContainer.getContainer());
      }
    }
    return containerList;
  }

  //for testing
  public Map<ApplicationId, SchedulerApplication<T>> getSchedulerApplications() {
    return applications;
  }

  @Override
  public Resource getClusterResource() {
    return clusterResource;
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return maximumAllocation;
  }

  public T getApplicationAttempt(
          ApplicationAttemptId applicationAttemptId) {
    SchedulerApplication<T> app =
            applications.get(applicationAttemptId.getApplicationId());
    return app == null ? null : app.getCurrentAppAttempt();
  }

  @Override
  public SchedulerAppReport getSchedulerAppInfo(
          ApplicationAttemptId appAttemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
    if (attempt == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request for appInfo of unknown attempt " + appAttemptId);
      }
      return null;
    }
    return new SchedulerAppReport(attempt);
  }

  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
          ApplicationAttemptId appAttemptId) {
    SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
    if (attempt == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request for appInfo of unknown attempt " + appAttemptId);
      }
      return null;
    }
    return attempt.getResourceUsageReport();
  }

  public T getCurrentAttemptForContainer(
          ContainerId containerId) {
    return getApplicationAttempt(containerId.getApplicationAttemptId());
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    SchedulerApplicationAttempt attempt =
            getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    N node = nodes.get(nodeId);
    return node == null ? null : new SchedulerNodeReport(node);
  }

  @Override
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException {
    throw new YarnException(getClass().getSimpleName() +
        " does not support moving apps between queues");
  }
  /**
   * Recover resource request back from RMContainer when a container is 
   * preempted before AM pulled the same. If container is pulled by
   * AM, then RMContainer will not have resource request to recover.
   * @param rmContainer
     * @param ts
   */
  protected void recoverResourceRequestForContainer(RMContainer rmContainer, 
          TransactionState ts) {
    List<ResourceRequest> requests = rmContainer.getResourceRequests();

    // If container state is moved to ACQUIRED, request will be empty.
    if (requests == null) {
      return;
    }
    // Add resource request back to Scheduler.
    SchedulerApplicationAttempt schedulerAttempt 
        = getCurrentAttemptForContainer(rmContainer.getContainerId());
    if (schedulerAttempt != null) {
      schedulerAttempt.recoverResourceRequests(requests, ts);
    }
  }

}
