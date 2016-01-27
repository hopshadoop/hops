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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.server.utils.Lock.NoLock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public class LeafQueue implements CSQueue {

  private static final Log LOG = LogFactory.getLog(LeafQueue.class);

  private final String queueName;   //recovered 
  private CSQueue parent;    //from conf
  private float capacity;   //from conf
  private float absoluteCapacity;   //from conf
  private float maximumCapacity;  // from conf
  private float absoluteMaxCapacity;  //from conf
  private float absoluteUsedCapacity = 0.0f;   //recovered
  private int userLimit;  //from conf
  private float userLimitFactor;  //from conf

  private int maxApplications; //from conf
  private int maxApplicationsPerUser; //from conf

  private float maxAMResourcePerQueuePercent;  //from conf
  private int maxActiveApplications; //Based on absolute max capacity
  private int maxActiveAppsUsingAbsCap; //Based on absolute capacity
  private int maxActiveApplicationsPerUser;  //Based on maxActiveAppsUsingAbsCap

  private int nodeLocalityDelay; //from config

  private Resource usedResources = Resources.createResource(0, 0);  // recoverd
  private float usedCapacity = 0.0f;   //recovered
  private volatile int numContainers;   //recovered

  Set<FiCaSchedulerApp> activeApplications; //recovered
  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap
          = new HashMap<ApplicationAttemptId, FiCaSchedulerApp>();   //recovered

  Set<FiCaSchedulerApp> pendingApplications; //recoverd

  private final Resource minimumAllocation;     //from config
  private final Resource maximumAllocation;     //from config
  private final float minimumAllocationFactor;   //from config

  private RMContainerTokenSecretManager containerTokenSecretManager;  //from config

  private Map<String, User> users = new HashMap<String, User>();  //recovered

  private final QueueMetrics metrics;  //TORECOVER

  private QueueInfo queueInfo;   //recovered

  private QueueState state;  //from conf

  private Map<QueueACL, AccessControlList> acls
          = new HashMap<QueueACL, AccessControlList>();  //from conf

  private final RecordFactory recordFactory
          = RecordFactoryProvider.getRecordFactory(null);  // no need

  private CapacitySchedulerContext scheduler;       // no need

  private final ActiveUsersManager activeUsersManager;  //recovered

  private final ResourceCalculator resourceCalculator;  // no need

  
  public void recover(RMStateStore.RMState state) {
    io.hops.metadata.yarn.entity.capacity.CSQueue hopCSQueue = state.
            getAllCSQueues().get(this.getQueuePath());

    absoluteUsedCapacity = hopCSQueue.getAbsoluteUsedCapacity();
    usedResources.setMemory(hopCSQueue.getUsedResourceMemory());
    usedResources.setVirtualCores(hopCSQueue.getUsedResourceVCores());
    usedCapacity = hopCSQueue.getUsedCapacity();
    numContainers = hopCSQueue.getNumContainers();

  }

  //TORECOVER: change this to implement recoverable
  public void recoverApp(FiCaSchedulerApp appAttempt, RMStateStore.RMState state)
          throws AccessControlException {
    if (!appAttempt.isStopped()) {
      applicationAttemptMap.
              put(appAttempt.getApplicationAttemptId(), appAttempt);

      if (appAttempt.isPending()) {
        pendingApplications.add(appAttempt);
      } else {
        activeApplications.add(appAttempt);
      }
      // Inform the parent queue
      try {
        getParent().submitApplication(appAttempt.getApplicationId(),
                appAttempt.getUser(), appAttempt.getQueueName(), null);
      } catch (AccessControlException ace) {
        LOG.info("Failed to submit application to parent-queue: "
                + getParent().getQueuePath(), ace);
        throw ace;
      }
      User user = getUser(appAttempt.getUser(), null);
      user.recover(state.getAllCSLeafQueueUserInfo().get(appAttempt.getUser()));
    }
  }
  
  public LeafQueue(CapacitySchedulerContext cs, String queueName, 
          CSQueue parent, CSQueue old, TransactionState transactionState) {
    this.scheduler = cs;
    this.queueName = queueName;
    this.parent = parent;

    this.resourceCalculator = cs.getResourceCalculator();

    // must be after parent and queueName are initialized
    this.metrics = old != null ? old.getMetrics() : QueueMetrics
        .forQueue(getQueuePath(), parent,
            cs.getConfiguration().getEnableUserMetrics(), cs.getConf());
    this.activeUsersManager = new ActiveUsersManager(metrics);
    this.minimumAllocation = cs.getMinimumResourceCapability();
    this.maximumAllocation = cs.getMaximumResourceCapability();
    this.minimumAllocationFactor = Resources.ratio(resourceCalculator,
                    Resources.subtract(maximumAllocation, minimumAllocation),
                    maximumAllocation);
    this.containerTokenSecretManager = cs.getContainerTokenSecretManager();

    float capacity =
        (float) cs.getConfiguration().getCapacity(getQueuePath()) / 100;
    float absoluteCapacity = parent.getAbsoluteCapacity() * capacity;

    float maximumCapacity =
        (float) cs.getConfiguration().getMaximumCapacity(getQueuePath()) / 100;
    float absoluteMaxCapacity =
        CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);

    int userLimit = cs.getConfiguration().getUserLimit(getQueuePath());
    float userLimitFactor =
        cs.getConfiguration().getUserLimitFactor(getQueuePath());

    int maxApplications =
        cs.getConfiguration().getMaximumApplicationsPerQueue(getQueuePath());
    if (maxApplications < 0) {
      int maxSystemApps = cs.getConfiguration().getMaximumSystemApplications();
      maxApplications = (int) (maxSystemApps * absoluteCapacity);
    }
    maxApplicationsPerUser =
        (int) (maxApplications * (userLimit / 100.0f) * userLimitFactor);

    float maxAMResourcePerQueuePercent = cs.getConfiguration()
            .getMaximumApplicationMasterResourcePerQueuePercent(getQueuePath());
    int maxActiveApplications = CSQueueUtils
        .computeMaxActiveApplications(resourceCalculator,
                    cs.getClusterResources(), this.minimumAllocation,
                    maxAMResourcePerQueuePercent, absoluteMaxCapacity);
    this.maxActiveAppsUsingAbsCap = CSQueueUtils
        .computeMaxActiveApplications(resourceCalculator,
                    cs.getClusterResources(), this.minimumAllocation,
                    maxAMResourcePerQueuePercent, absoluteCapacity);
    int maxActiveApplicationsPerUser = CSQueueUtils
        .computeMaxActiveApplicationsPerUser(maxActiveAppsUsingAbsCap,
            userLimit, userLimitFactor);

    this.queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    this.queueInfo.setQueueName(queueName);
    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());

    QueueState state = cs.getConfiguration().getState(getQueuePath());

    Map<QueueACL, AccessControlList> acls =
        cs.getConfiguration().getAcls(getQueuePath());

    setupQueueConfigs(cs.getClusterResources(), capacity, absoluteCapacity,
        maximumCapacity, absoluteMaxCapacity, userLimit, userLimitFactor,
        maxApplications, maxAMResourcePerQueuePercent, maxApplicationsPerUser,
        maxActiveApplications, maxActiveApplicationsPerUser, state, acls,
        cs.getConfiguration().getNodeLocalityDelay(), transactionState);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "LeafQueue:" + " name=" + queueName + ", fullname=" + getQueuePath());
    }

    Comparator<FiCaSchedulerApp> applicationComparator =
        cs.getApplicationComparator();
    this.pendingApplications =
        new TreeSet<FiCaSchedulerApp>(applicationComparator);
    this.activeApplications =
        new TreeSet<FiCaSchedulerApp>(applicationComparator);
  }

  private synchronized void setupQueueConfigs(Resource clusterResource,
      float capacity, float absoluteCapacity, float maximumCapacity,
      float absoluteMaxCapacity, int userLimit, float userLimitFactor,
          int maxApplications, float maxAMResourcePerQueuePercent,
          int maxApplicationsPerUser, int maxActiveApplications,
          int maxActiveApplicationsPerUser, QueueState state,
          Map<QueueACL, AccessControlList> acls, int nodeLocalityDelay, 
          TransactionState transactionState) {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absCapacity = getParent().getAbsoluteCapacity() * capacity;
    CSQueueUtils.checkAbsoluteCapacities(getQueueName(), absCapacity,
            absoluteMaxCapacity);

    this.capacity = capacity;
    this.absoluteCapacity = absCapacity;

    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absoluteMaxCapacity;

    this.userLimit = userLimit;
    this.userLimitFactor = userLimitFactor;

    this.maxApplications = maxApplications;
    this.maxAMResourcePerQueuePercent = maxAMResourcePerQueuePercent;
    this.maxApplicationsPerUser = maxApplicationsPerUser;

    this.maxActiveApplications = maxActiveApplications;
    this.maxActiveApplicationsPerUser = maxActiveApplicationsPerUser;

    this.state = state;

    this.acls = acls;

    this.queueInfo.setCapacity(this.capacity);
    this.queueInfo.setMaximumCapacity(this.maximumCapacity);
    this.queueInfo.setQueueState(this.state);

    this.nodeLocalityDelay = nodeLocalityDelay;

    StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }

    // Update metrics
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
        clusterResource, minimumAllocation, null);

    if (transactionState != null) {
      ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
              this.getQueuePath(), this);
    }
    
    LOG.info("Initializing " + queueName + "\n" +
        "capacity = " + capacity +
        " [= (float) configuredCapacity / 100 ]" + "\n" +
        "asboluteCapacity = " + absoluteCapacity +
        " [= parentAbsoluteCapacity * capacity ]" + "\n" +
        "maxCapacity = " + maximumCapacity +
        " [= configuredMaxCapacity ]" + "\n" +
        "absoluteMaxCapacity = " + absoluteMaxCapacity +
        " [= 1.0 maximumCapacity undefined, " +
        "(parentAbsoluteMaxCapacity * maximumCapacity) / 100 otherwise ]" +
        "\n" +
        "userLimit = " + userLimit +
        " [= configuredUserLimit ]" + "\n" +
        "userLimitFactor = " + userLimitFactor +
        " [= configuredUserLimitFactor ]" + "\n" +
        "maxApplications = " + maxApplications +
        " [= configuredMaximumSystemApplicationsPerQueue or" +
        " (int)(configuredMaximumSystemApplications * absoluteCapacity)]" +
        "\n" +
        "maxApplicationsPerUser = " + maxApplicationsPerUser +
        " [= (int)(maxApplications * (userLimit / 100.0f) * " +
        "userLimitFactor) ]" + "\n" +
        "maxActiveApplications = " + maxActiveApplications +
        " [= max(" +
        "(int)ceil((clusterResourceMemory / minimumAllocation) * " +
        "maxAMResourcePerQueuePercent * absoluteMaxCapacity)," +
        "1) ]" + "\n" +
        "maxActiveAppsUsingAbsCap = " + maxActiveAppsUsingAbsCap +
        " [= max(" +
        "(int)ceil((clusterResourceMemory / minimumAllocation) *" +
        "maxAMResourcePercent * absoluteCapacity)," +
        "1) ]" + "\n" +
        "maxActiveApplicationsPerUser = " + maxActiveApplicationsPerUser +
        " [= max(" +
        "(int)(maxActiveApplications * (userLimit / 100.0f) * " +
        "userLimitFactor)," +
        "1) ]" + "\n" +
        "usedCapacity = " + usedCapacity +
        " [= usedResourcesMemory / " +
        "(clusterResourceMemory * absoluteCapacity)]" + "\n" +
        "absoluteUsedCapacity = " + absoluteUsedCapacity +
        " [= usedResourcesMemory / clusterResourceMemory]" + "\n" +
        "maxAMResourcePerQueuePercent = " + maxAMResourcePerQueuePercent +
        " [= configuredMaximumAMResourcePercent ]" + "\n" +
        "minimumAllocationFactor = " + minimumAllocationFactor +
        " [= (float)(maximumAllocationMemory - minimumAllocationMemory) / " +
        "maximumAllocationMemory ]" + "\n" +
        "numContainers = " + numContainers +
        " [= currentNumContainers ]" + "\n" +
        "state = " + state +
        " [= configuredState ]" + "\n" +
        "acls = " + aclsString +
        " [= configuredAcls ]" + "\n" +
        "nodeLocalityDelay = " + nodeLocalityDelay + "\n");
  }

  @Override
  public synchronized float getCapacity() {
    return capacity;
  }

  @Override
  public synchronized float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  @Override
  public synchronized float getMaximumCapacity() {
    return maximumCapacity;
  }

  @Override
  public synchronized float getAbsoluteMaximumCapacity() {
    return absoluteMaxCapacity;
  }

  @Override
  public synchronized float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity;
  }

  @Override
  public synchronized CSQueue getParent() {
    return parent;
  }

  @Override
  public synchronized void setParent(CSQueue newParentQueue) {
    this.parent = (ParentQueue) newParentQueue;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public String getQueuePath() {
    return getParent().getQueuePath() + "." + getQueueName();
  }

  /**
   * Used only by tests.
   */
  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  /**
   * Used only by tests.
   */
  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }

  /**
   * Used only by tests.
   */
  @Private
  public float getMinimumAllocationFactor() {
    return minimumAllocationFactor;
  }

  /**
   * Used only by tests.
   */
  @Private
  public float getMaxAMResourcePerQueuePercent() {
    return maxAMResourcePerQueuePercent;
  }

  public int getMaxApplications() {
    return maxApplications;
  }

  public synchronized int getMaxApplicationsPerUser() {
    return maxApplicationsPerUser;
  }

  public synchronized int getMaximumActiveApplications() {
    return maxActiveApplications;
  }

  public synchronized int getMaximumActiveApplicationsPerUser() {
    return maxActiveApplicationsPerUser;
  }

  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }

  @Override
  public synchronized float getUsedCapacity() {
    return usedCapacity;
  }

  @Override
  public synchronized Resource getUsedResources() {
    return usedResources;
  }

  @Override
  public List<CSQueue> getChildQueues() {
    return null;
  }

  @Override
  public synchronized void setUsedCapacity(float usedCapacity,
          TransactionState transactionState) {
    this.usedCapacity = usedCapacity;
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
              this.getQueuePath(), this);
    }
  }

  @Override
  public synchronized void setAbsoluteUsedCapacity(float absUsedCapacity,
          TransactionState transactionState) {
    this.absoluteUsedCapacity = absUsedCapacity;
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
              this.getQueuePath(), this);
    }
  }

  /**
   * Set maximum capacity - used only for testing.
   *
   * @param maximumCapacity new max capacity
   */
  synchronized void setMaxCapacity(float maximumCapacity,
          TransactionState transactionState) {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absMaxCapacity = CSQueueUtils
        .computeAbsoluteMaximumCapacity(maximumCapacity, getParent());
    CSQueueUtils.checkAbsoluteCapacities(getQueueName(), absoluteCapacity,
            absMaxCapacity);

    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absMaxCapacity;
    if (transactionState != null) {
      ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
              this.getQueuePath(), this);
    }
  }

  /**
   * Set user limit - used only for testing.
   *
   * @param userLimit new user limit
   */
  synchronized void setUserLimit(int userLimit) {
    this.userLimit = userLimit; 
  }

  /**
   * Set user limit factor - used only for testing.
   *
   * @param userLimitFactor new user limit factor
   */
  synchronized void setUserLimitFactor(int userLimitFactor) {
    this.userLimitFactor = userLimitFactor;
  }

  @Override
  public synchronized int getNumApplications() {
    return getNumPendingApplications() + getNumActiveApplications();
  }

  public synchronized int getNumPendingApplications() {
    return pendingApplications.size();
  }

  public synchronized int getNumActiveApplications() {
    return activeApplications.size();
  }

  @Private
  public synchronized int getNumApplications(String user, 
          TransactionState transactionState) {
    return getUser(user, transactionState).getTotalApplications();
  }

  @Private
  public synchronized int getNumPendingApplications(String user, 
          TransactionState transactionState) {
    return getUser(user, transactionState).getPendingApplications();
  }

  @Private
  public synchronized int getNumActiveApplications(String user, 
          TransactionState transactionState) {
    return getUser(user, transactionState).getActiveApplications();
  }

  public synchronized int getNumContainers() {
    return numContainers;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }

  @Private
  public synchronized int getUserLimit() {
    return userLimit;
  }

  @Private
  public synchronized float getUserLimitFactor() {
    return userLimitFactor;
  }

  @Override
  public synchronized QueueInfo getQueueInfo(boolean includeChildQueues,
      boolean recursive) {
    queueInfo.setCurrentCapacity(usedCapacity);
    return queueInfo;
  }

  @Override
  public synchronized List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    QueueUserACLInfo userAclInfo =
        recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      }
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return Collections.singletonList(userAclInfo);
  }

  @Private
  public int getNodeLocalityDelay() {
    return nodeLocalityDelay;
  }

  public String toString() {
    return queueName + ": " +
        "capacity=" + capacity + ", " +
        "absoluteCapacity=" + absoluteCapacity + ", " +
        "usedResources=" + usedResources +
        "usedCapacity=" + getUsedCapacity() + ", " +
        "absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + ", " +
        "numApps=" + getNumApplications() + ", " +
        "numContainers=" + getNumContainers();
  }

  private synchronized User getUser(String userName, 
          TransactionState transactionState) {
    User user = users.get(userName);
    if (user == null) {
      user = new User();
      users.put(userName, user);
      // Add the attempt to our data-structures
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                addCSLeafUsers(userName);
      }
    }
    return user;
  }


  public synchronized void initializeApplicationLimits(Resource cluster) {

    this.maxActiveApplications
            = CSQueueUtils.computeMaxActiveApplications(
                    resourceCalculator,
                    cluster, minimumAllocation,
                    maxAMResourcePerQueuePercent, absoluteMaxCapacity);
    this.maxActiveAppsUsingAbsCap
            = CSQueueUtils.computeMaxActiveApplications(
                    resourceCalculator,
                    cluster, minimumAllocation,
                    maxAMResourcePerQueuePercent, absoluteCapacity);
    this.maxActiveApplicationsPerUser
            = CSQueueUtils.computeMaxActiveApplicationsPerUser(
                    maxActiveAppsUsingAbsCap, userLimit, userLimitFactor);

  }

  /**
   * @return an ArrayList of UserInfo objects who are active in this queue
   */
  public synchronized ArrayList<UserInfo> getUsers() {
    ArrayList<UserInfo> usersToReturn = new ArrayList<UserInfo>();
    for (Map.Entry<String, User> entry : users.entrySet()) {
      usersToReturn.add(new UserInfo(entry.getKey(),
          Resources.clone(entry.getValue().consumed),
          entry.getValue().getActiveApplications(),
              entry.getValue().getPendingApplications()));
    }
    return usersToReturn;
  }

  @Override
  public synchronized void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource , TransactionState transactionState)
          throws IOException {
    // Sanity check
    if (!(newlyParsedQueue instanceof LeafQueue) ||
        !newlyParsedQueue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() +
          " from " + newlyParsedQueue.getQueuePath());
    }

    LeafQueue newlyParsedLeafQueue = (LeafQueue) newlyParsedQueue;
    setupQueueConfigs(clusterResource, newlyParsedLeafQueue.capacity,
        newlyParsedLeafQueue.absoluteCapacity,
            newlyParsedLeafQueue.maximumCapacity,
            newlyParsedLeafQueue.absoluteMaxCapacity,
            newlyParsedLeafQueue.userLimit, newlyParsedLeafQueue.userLimitFactor,
            newlyParsedLeafQueue.maxApplications,
            newlyParsedLeafQueue.maxAMResourcePerQueuePercent,
            newlyParsedLeafQueue.getMaxApplicationsPerUser(),
            newlyParsedLeafQueue.getMaximumActiveApplications(),
            newlyParsedLeafQueue.getMaximumActiveApplicationsPerUser(),
            newlyParsedLeafQueue.state, newlyParsedLeafQueue.acls,
            newlyParsedLeafQueue.getNodeLocalityDelay(), transactionState);

    // queue metrics are updated, more resource may be available
    // activate the pending applications if possible
    activateApplications(transactionState);
  }

  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    // Check if the leaf-queue allows access
    synchronized (this) {
      if (acls.get(acl).isUserAllowed(user)) {
        return true;
      }
    }

    // Check if parent-queue allows access
    return getParent().hasAccess(acl, user);
  }

  @Override
  public void submitApplicationAttempt(FiCaSchedulerApp application,
          String userName, TransactionState transactionState) {
    // Careful! Locking order is important!
    synchronized (this) {
      User user = getUser(userName, transactionState);
      // Add the attempt to our data-structures
      addApplicationAttempt(application, user, transactionState);
    }

    metrics.submitAppAttempt(userName);
    getParent().submitApplicationAttempt(application, userName, transactionState);
  }

  @Override
  public void submitApplication(ApplicationId applicationId, String userName,
          String queue, TransactionState transactionState) 
          throws AccessControlException {
    // Careful! Locking order is important!

    // Check queue ACLs
    UserGroupInformation userUgi =
        UserGroupInformation.createRemoteUser(userName);
    if (!hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi) &&
        !hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
      throw new AccessControlException("User " + userName + " cannot submit" +
          " applications to queue " + getQueuePath());
    }

    User user = null;
    synchronized (this) {

      // Check if the queue is accepting jobs
      if (getState() != QueueState.RUNNING) {
        String msg = "Queue " + getQueuePath() +
            " is STOPPED. Cannot accept submission of application: " +
            applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for queues
      if (getNumApplications() >= getMaxApplications()) {
        String msg = "Queue " + getQueuePath() +
            " already has " + getNumApplications() + " applications," +
            " cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for the user on this queue
      user = getUser(userName, transactionState);
      if (user.getTotalApplications() >= getMaxApplicationsPerUser()) {
        String msg = "Queue " + getQueuePath() +
            " already has " + user.getTotalApplications() +
            " applications from user " + userName +
            " cannot accept submission of application: " + applicationId;
        LOG.info(msg);
        throw new AccessControlException(msg);
      }
    }

    // Inform the parent queue
    try {
      getParent().submitApplication(applicationId, userName, queue, 
              transactionState);
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application to parent-queue: " +
          getParent().getQueuePath(), ace);
      throw ace;
    }

    metrics.submitApp(userName);
  }

  private synchronized void activateApplications(
          TransactionState transactionState) {
    for (Iterator<FiCaSchedulerApp> i = pendingApplications.iterator();
         i.hasNext(); ) {
      FiCaSchedulerApp application = i.next();

      // Check queue limit
      if (getNumActiveApplications() >= getMaximumActiveApplications()) {
        break;
      }

      // Check user limit
      User user = getUser(application.getUser(), transactionState);
      if (user.getActiveApplications() < 
              getMaximumActiveApplicationsPerUser()) {
        user.activateApplication(application.getUser(), transactionState);
        activeApplications.add(application);
        i.remove();
        LOG.info("Application " + application.getApplicationId() +
            " from user: " + application.getUser() +
            " activated in queue: " + getQueueName());
      }
    }
  }

  private synchronized void addApplicationAttempt(FiCaSchedulerApp application,
          User user, TransactionState transactionState) {
    // Accept 
    user.submitApplication(application.getUser(), transactionState);
    pendingApplications.add(application);
    applicationAttemptMap
        .put(application.getApplicationAttemptId(), application);

    // Activate applications
    activateApplications(transactionState);

    LOG.info("Application added -" +
            " appId: " + application.getApplicationId() +
            " user: " + user + "," + " leaf-queue: " + getQueueName() +
            " #user-pending-applications: " + user.getPendingApplications() +
            " #user-active-applications: " + user.getActiveApplications() +
            " #queue-pending-applications: " + getNumPendingApplications() +
            " #queue-active-applications: " + getNumActiveApplications());
  }

  @Override
  public void finishApplication(ApplicationId application, String user) {
    // Inform the activeUsersManager
    activeUsersManager.deactivateApplication(user, application);
    // Inform the parent queue
    getParent().finishApplication(application, user);
  }

  @Override
  public void finishApplicationAttempt(FiCaSchedulerApp application,
          String queue, TransactionState transactionState) {
    // Careful! Locking order is important!
    synchronized (this) {
      removeApplicationAttempt(application, getUser(application.getUser(), 
              transactionState), transactionState);
    }
    getParent().finishApplicationAttempt(application, queue, transactionState);
  }

  public synchronized void removeApplicationAttempt(
          FiCaSchedulerApp application, User user,
          TransactionState transactionState) {
    boolean wasActive = activeApplications.remove(application);
    if (!wasActive) {
      pendingApplications.remove(application);
    }
    applicationAttemptMap.remove(application.getApplicationAttemptId());

    user.finishApplication(application.getUser(), wasActive, transactionState);
    if (user.getTotalApplications() == 0) {
      users.remove(application.getUser());
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                addCSLeafQueueUsersToRemove(application.getUser());
      }
    }

    // Check if we can activate more applications
    activateApplications(transactionState);

    LOG.info("Application removed -" +
            " appId: " + application.getApplicationId() +
            " user: " + application.getUser() +
            " queue: " + getQueueName() +
            " #user-pending-applications: " + user.getPendingApplications() +
            " #user-active-applications: " + user.getActiveApplications() +
            " #queue-pending-applications: " + getNumPendingApplications() +
            " #queue-active-applications: " + getNumActiveApplications());
  }

  private synchronized FiCaSchedulerApp getApplication(
          ApplicationAttemptId applicationAttemptId) {
    return applicationAttemptMap.get(applicationAttemptId);
  }

  private static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);

  private static final CSAssignment SKIP_ASSIGNMENT = new CSAssignment(true);

  @Override
  public synchronized CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, TransactionState transactionState) {

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "assignContainers: node=" + node.getNodeName() + " #applications=" +
              activeApplications.size());
    }

    // Check for reserved resources
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      FiCaSchedulerApp application =
          getApplication(reservedContainer.getApplicationAttemptId());
      synchronized (application) {
        return assignReservedContainer(application, node, reservedContainer,
                clusterResource, transactionState);
      }
    }

    // Try to assign containers to applications in order
    for (FiCaSchedulerApp application : activeApplications) {

      if (LOG.isDebugEnabled()) {
        LOG.debug("pre-assignContainers for application " +
            application.getApplicationId());
        application.showRequests();
      }

      synchronized (application) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
          continue;
        }

        // Schedule in priority order
        for (Priority priority : application.getPriorities()) {
          ResourceRequest anyRequest =
              application.getResourceRequest(priority, ResourceRequest.ANY);
          if (null == anyRequest) {
            continue;
          }

          // Required resource
          Resource required = anyRequest.getCapability();

          // Do we need containers at this 'priority'?
          if (!needContainers(application, priority, required)) {
            continue;
          }

          // Compute user-limit & set headroom
          // Note: We compute both user-limit & headroom with the highest 
          //       priority request as the target. 
          //       This works since we never assign lower priority requests
          //       before all higher priority ones are serviced.
          Resource userLimit = 
                 computeUserLimitAndSetHeadroom(application, clusterResource,
                  required, transactionState);

          // Check queue max-capacity limit
          if (!assignToQueue(clusterResource, required, transactionState)) {
            return NULL_ASSIGNMENT;
          }

          // Check user limit
          if (!assignToUser(clusterResource, application.getUser(),
                  userLimit, transactionState)) {
            break;
          }

          // Inform the application it is about to get a scheduling opportunity
          application.addSchedulingOpportunity(priority, transactionState);

          // Try to schedule
          CSAssignment assignment =
              assignContainersOnNode(clusterResource, node, application,
                          priority, null, transactionState);

          // Did the application skip this node?
          if (assignment.getSkipped()) {
            // Don't count 'skipped nodes' as a scheduling opportunity!
            application.subtractSchedulingOpportunity(priority, 
                    transactionState);
            continue;
          }

          // Did we schedule or reserve a container?
          Resource assigned = assignment.getResource();
          if (Resources
                  .greaterThan(resourceCalculator, clusterResource, assigned,
                          Resources.none())) {

            // Book-keeping 
            // Note: Update headroom to account for current allocation too...
            allocateResource(clusterResource, application, assigned,
                    transactionState);

            // Don't reset scheduling opportunities for non-local assignments
            // otherwise the app will be delayed for each non-local assignment.
            // This helps apps with many off-cluster requests schedule faster.
            if (assignment.getType() != NodeType.OFF_SWITCH) {
              application.resetSchedulingOpportunities(priority,
                      transactionState);
            }

            // Done
            return assignment;
          } else {
            // Do not assign out of order w.r.t priorities
            break;
          }
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("post-assignContainers for application " +
            application.getApplicationId());
      }
      application.showRequests();
    }

    return NULL_ASSIGNMENT;

  }

  private synchronized CSAssignment assignReservedContainer(
      FiCaSchedulerApp application, FiCaSchedulerNode node,
      RMContainer rmContainer, Resource clusterResource,
                  TransactionState transactionState) {
    // Do we still need this reservation?
    Priority priority = rmContainer.getReservedPriority();
    if (application.getTotalRequiredResources(priority) == 0) {
      // Release
      return new CSAssignment(application, rmContainer);
    }

    // Try to assign if we have sufficient resources
    assignContainersOnNode(clusterResource, node, application, priority,
            rmContainer, transactionState);

    // Doesn't matter... since it's already charged for at time of reservation
    // "re-reservation" is *free*
    return new CSAssignment(Resources.none(), NodeType.NODE_LOCAL);
  }

  private synchronized boolean assignToQueue(Resource clusterResource,
          Resource required, TransactionState transactionState) {
    // Check how of the cluster's absolute capacity we are currently using...
    float potentialNewCapacity = Resources
        .divide(resourceCalculator, clusterResource,
            Resources.add(usedResources, required), clusterResource);
    if (potentialNewCapacity > absoluteMaxCapacity) {
      LOG.info(getQueueName() +
          " usedResources: " + usedResources +
          " clusterResources: " + clusterResource +
          " currentCapacity " +
          Resources.divide(resourceCalculator, clusterResource, usedResources,
              clusterResource) +
          " required " + required +
          " potentialNewCapacity: " + potentialNewCapacity + " ( " +
          " max-capacity: " + absoluteMaxCapacity + ")");
      
      ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
              this.getQueuePath(), this);
      return false;
    }
    ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
            this.getQueuePath(), this);
    return true;
  }

  @Lock({LeafQueue.class, FiCaSchedulerApp.class})
  private Resource computeUserLimitAndSetHeadroom(FiCaSchedulerApp application, 
          Resource clusterResource, Resource required, 
          TransactionState transactionState) {

    String user = application.getUser();

    /**
     * Headroom is min((userLimit, queue-max-cap) - consumed)
     */
    
    Resource userLimit =                          // User limit
            computeUserLimit(application, clusterResource, required, transactionState);

    
    Resource queueMaxCap =                        // Queue Max-Capacity
        Resources.multiplyAndNormalizeDown(resourceCalculator, clusterResource,
            absoluteMaxCapacity, minimumAllocation);

    Resource userConsumed = getUser(user, transactionState).
            getConsumedResources();
    Resource headroom = Resources.subtract(Resources
            .min(resourceCalculator, clusterResource, userLimit, queueMaxCap),
                    userConsumed);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for user " + user + ": " +
          " userLimit=" + userLimit +
          " queueMaxCap=" + queueMaxCap +
          " consumed=" + userConsumed +
          " headroom=" + headroom);
    }

    application.setHeadroom(headroom, null);
    metrics.setAvailableResourcesToUser(user, headroom);

    return userLimit;
  }

  @Lock(NoLock.class)
  private Resource computeUserLimit(FiCaSchedulerApp application,
          Resource clusterResource, Resource required, 
          TransactionState transactionState) {
    // What is our current capacity? 
    // * It is equal to the max(required, queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   (usedResources + required) (which extra resources we are allocating)

    // Allow progress for queues with miniscule capacity
    final Resource queueCapacity = Resources
        .max(resourceCalculator, clusterResource, Resources
                .multiplyAndNormalizeUp(resourceCalculator, clusterResource,
                    absoluteCapacity, minimumAllocation), required);

    Resource currentCapacity = Resources
        .lessThan(resourceCalculator, clusterResource, usedResources,
            queueCapacity) ? queueCapacity :
        Resources.add(usedResources, required);
    
    ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
            this.getQueuePath(), this);
    // Never allow a single user to take more than the 
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than 
    // queue-hard-limit * ulMin
    
    final int activeUsers = activeUsersManager.getNumActiveUsers();

    Resource limit = Resources.roundUp(resourceCalculator, Resources
            .min(resourceCalculator, clusterResource, Resources
                    .max(resourceCalculator, clusterResource, Resources
                            .divideAndCeil(resourceCalculator, currentCapacity,
                                activeUsers), Resources
                            .divideAndCeil(resourceCalculator, Resources
                                    .multiplyAndRoundDown(currentCapacity,
                                        userLimit), 100)),
                Resources.multiplyAndRoundDown(queueCapacity, userLimitFactor)),
                    minimumAllocation);

    if (LOG.isDebugEnabled()) {
      String userName = application.getUser();
      LOG.debug("User limit computation for " + userName +
              " in queue " + getQueueName() +
              " userLimit=" + userLimit +
              " userLimitFactor=" + userLimitFactor +
              " required: " + required +
              " consumed: " + getUser(userName, transactionState).
                      getConsumedResources() +
              " limit: " + limit +
              " queueCapacity: " + queueCapacity +
              " qconsumed: " + usedResources +
              " currentCapacity: " + currentCapacity +
              " activeUsers: " + activeUsers +
              " clusterCapacity: " + clusterResource);
    }

    return limit;
  }

  private synchronized boolean assignToUser(Resource clusterResource,
          String userName, Resource limit, TransactionState transactionState) {

    User user = getUser(userName, transactionState);

    // Note: We aren't considering the current request since there is a fixed
    // overhead of the AM, but it's a > check, not a >= check, so...
    if (Resources.greaterThan(resourceCalculator, clusterResource,
            user.getConsumedResources(), limit)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("User " + userName + " in queue " + getQueueName() +
                " will exceed limit - " +
                " consumed: " + user.getConsumedResources() +
                " limit: " + limit);
      }
      return false;
    }

    return true;
  }

  boolean needContainers(FiCaSchedulerApp application, Priority priority,
          Resource required) {
    int requiredContainers = application.getTotalRequiredResources(priority);
    int reservedContainers = application.getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor =
          Resources.ratio(resourceCalculator, required, getMaximumAllocation());

      // Use percentage of node required to bias against large containers...
      // Protect against corner case where you need the whole node with
      // Math.min(nodeFactor, minimumAllocationFactor)
      starvation = (int) ((application.getReReservations(priority) /
          (float) reservedContainers) *
          (1.0f - (Math.min(nodeFactor, getMinimumAllocationFactor()))));

      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" +
            " app.#re-reserve=" + application.getReReservations(priority) +
            " reserved=" + reservedContainers +
            " nodeFactor=" + nodeFactor +
            " minAllocFactor=" + getMinimumAllocationFactor() +
            " starvation=" + starvation);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }

  private CSAssignment assignContainersOnNode(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, TransactionState transactionState) {

    Resource assigned = Resources.none();

    // Data-local
    ResourceRequest nodeLocalResourceRequest =
        application.getResourceRequest(priority, node.getNodeName());
    if (nodeLocalResourceRequest != null) {
      assigned =
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest,
              node, application, priority, reservedContainer, transactionState);
      if (Resources.greaterThan(resourceCalculator, clusterResource, assigned,
          Resources.none())) {
        return new CSAssignment(assigned, NodeType.NODE_LOCAL);
      }
    }

    // Rack-local
    ResourceRequest rackLocalResourceRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackLocalResourceRequest != null) {
      if (!rackLocalResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }

      assigned =
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest,
              node, application, priority, reservedContainer, transactionState);
      if (Resources.greaterThan(resourceCalculator, clusterResource, assigned,
          Resources.none())) {
        return new CSAssignment(assigned, NodeType.RACK_LOCAL);
      }
    }

    // Off-switch
    ResourceRequest offSwitchResourceRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchResourceRequest != null) {
      if (!offSwitchResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }

      return new CSAssignment(
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
              node, application, priority, reservedContainer, transactionState),
              NodeType.OFF_SWITCH);
    }

    return SKIP_ASSIGNMENT;
  }

  private Resource assignNodeLocalContainers(Resource clusterResource,
      ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
      RMContainer reservedContainer, TransactionState transactionState) {
    if (canAssign(application, priority, node, NodeType.NODE_LOCAL,
            reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
              nodeLocalResourceRequest, NodeType.NODE_LOCAL, reservedContainer,
              transactionState);
    }

    return Resources.none();
  }

  private Resource assignRackLocalContainers(Resource clusterResource,
      ResourceRequest rackLocalResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
          RMContainer reservedContainer, TransactionState transactionState) {
    if (canAssign(application, priority, node, NodeType.RACK_LOCAL,
            reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
              rackLocalResourceRequest, NodeType.RACK_LOCAL, reservedContainer,
              transactionState);
    }

    return Resources.none();
  }

  private Resource assignOffSwitchContainers(Resource clusterResource,
      ResourceRequest offSwitchResourceRequest, FiCaSchedulerNode node,
      FiCaSchedulerApp application, Priority priority,
          RMContainer reservedContainer, TransactionState transactionState) {
    if (canAssign(application, priority, node, NodeType.OFF_SWITCH,
            reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
              offSwitchResourceRequest, NodeType.OFF_SWITCH, reservedContainer,
              transactionState);
    }

    return Resources.none();
  }

  boolean canAssign(FiCaSchedulerApp application, Priority priority,
          FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // Clearly we need containers for this application...
    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }

      // 'Delay' off-switch
      ResourceRequest offSwitchRequest =
          application.getResourceRequest(priority, ResourceRequest.ANY);
      long missedOpportunities =
          application.getSchedulingOpportunities(priority);
      long requiredContainers = offSwitchRequest.getNumContainers();

      float localityWaitFactor = application
          .getLocalityWaitFactor(priority, scheduler.getNumClusterNodes());

      return ((requiredContainers * localityWaitFactor) < missedOpportunities);
    }

    // Check if we need containers on this rack 
    ResourceRequest rackLocalRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackLocalRequest == null || rackLocalRequest.getNumContainers() <= 0) {
      return false;
    }

    // If we are here, we do need containers on this rack for RACK_LOCAL req
    if (type == NodeType.RACK_LOCAL) {
      // 'Delay' rack-local just a little bit...
      long missedOpportunities =
          application.getSchedulingOpportunities(priority);
      return (Math.min(scheduler.getNumClusterNodes(), getNodeLocalityDelay()) <
          missedOpportunities);
    }

    // Check if we need containers on this host
    if (type == NodeType.NODE_LOCAL) {
      // Now check if we need containers on this host...
      ResourceRequest nodeLocalRequest =
          application.getResourceRequest(priority, node.getNodeName());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }

    return false;
  }

  private Container getContainer(RMContainer rmContainer,
      FiCaSchedulerApp application, FiCaSchedulerNode node, Resource capability,
      Priority priority, TransactionState ts) {
    return (rmContainer != null) ? rmContainer.getContainer() :
        createContainer(application, node, capability, priority, ts);
  }

  Container createContainer(FiCaSchedulerApp application,
      FiCaSchedulerNode node, Resource capability, Priority priority,
      TransactionState ts) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils
        .newContainerId(application.getApplicationAttemptId(),
            application.getNewContainerId(ts));

    // Create the container
    Container container = BuilderUtils
        .newContainer(containerId, nodeId, node.getRMNode().getHttpAddress(),
            capability, priority, null);

    return container;
  }

  private Resource assignContainer(Resource clusterResource,
      FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
          ResourceRequest request, NodeType type, RMContainer rmContainer,
          TransactionState transactionState) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "assignContainers: node=" + node.getNodeName() + " application=" +
              application.getApplicationId().getId() + " priority=" +
              priority.getPriority() + " request=" + request + " type=" + type);
    }
    Resource capability = request.getCapability();
    Resource available = node.getAvailableResource();
    Resource totalResource = node.getTotalResource();

    if (!Resources.fitsIn(capability, totalResource)) {
      LOG.warn("Node : " + node.getNodeID() +
          " does not have sufficient resource for request : " + request +
          " node total capability : " + node.getTotalResource());
      return Resources.none();
    }
    assert Resources.greaterThan(resourceCalculator, clusterResource, available,
        Resources.none());

    // Create the container if necessary
    Container container =
        getContainer(rmContainer, application, node, capability, priority,
                    transactionState);

    // something went wrong getting/creating the container 
    if (container == null) {
      LOG.warn("Couldn't get container for allocation!");
      return Resources.none();
    }

    // Can we allocate a container on this node?
    int availableContainers =
        resourceCalculator.computeAvailableContainers(available, capability);
    if (availableContainers > 0) {
      // Allocate...

      // Did we previously reserve containers at this 'priority'?
      if (rmContainer != null) {
        unreserve(application, priority, node, rmContainer, transactionState);
      }

      // Inform the application
      RMContainer allocatedContainer = application
          .allocate(type, node, priority, request, container, transactionState);

      // Does the application need this resource?
      if (allocatedContainer == null) {
        return Resources.none();
      }

      // Inform the node
      node.allocateContainer(application.getApplicationId(), allocatedContainer,
          transactionState);

      LOG.info("assignedContainer" +
          " application=" + application.getApplicationId() +
          " container=" + container +
          " containerId=" + container.getId() +
          " queue=" + this +
          " usedCapacity=" + getUsedCapacity() +
          " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
          " used=" + usedResources +
          " cluster=" + clusterResource);

      return container.getResource();
    } else {
      // Reserve by 'charging' in advance...
      reserve(application, priority, node, rmContainer, container,
              transactionState);

      LOG.info("Reserved container " +
          " application=" + application.getApplicationId() +
          " resource=" + request.getCapability() +
          " queue=" + this.toString() +
          " usedCapacity=" + getUsedCapacity() +
          " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
          " used=" + usedResources +
          " cluster=" + clusterResource);

      return request.getCapability();
    }
  }

  private void reserve(FiCaSchedulerApp application, Priority priority,
          FiCaSchedulerNode node, RMContainer rmContainer, Container container,
          TransactionState transactionState) {
    // Update reserved metrics if this is the first reservation
    if (rmContainer == null) {
      getMetrics()
          .reserveResource(application.getUser(), container.getResource());
    }

    // Inform the application 
    rmContainer = application
        .reserve(node, priority, rmContainer, container, transactionState);

    // Update the node
    node.reserveResource(application, priority, rmContainer, transactionState);
  }

  private boolean unreserve(FiCaSchedulerApp application, Priority priority,
          FiCaSchedulerNode node, RMContainer rmContainer,
          TransactionState transactionState) {
    // Done with the reservation?
    if (application.unreserve(node, priority, transactionState)) {
      node.unreserveResource(application, transactionState);

      // Update reserved metrics
      getMetrics().unreserveResource(application.getUser(),
          rmContainer.getContainer().getResource());
      return true;
    }
    return false;
  }

  @Override
  public void completedContainer(Resource clusterResource,
          FiCaSchedulerApp application, FiCaSchedulerNode node,
          RMContainer rmContainer, ContainerStatus containerStatus,
          RMContainerEventType event, CSQueue childQueue,
          TransactionState transactionState) {
    if (application != null) {

      boolean removed = false;

      // Careful! Locking order is important!
      synchronized (this) {

        Container container = rmContainer.getContainer();

        // Inform the application & the node
        // Note: It's safe to assume that all state changes to RMContainer
        // happen under scheduler's lock... 
        // So, this is, in effect, a transaction across application & node
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          removed =
              unreserve(application, rmContainer.getReservedPriority(), node,
                  rmContainer, transactionState);
        } else {
          removed = application
                  .containerCompleted(rmContainer, containerStatus, event,
                          transactionState);
          node.releaseContainer(container, transactionState);
        }

        // Book-keeping
        if (removed) {
          releaseResource(clusterResource, application,
                  container.getResource(), transactionState);
          LOG.info("completedContainer" +
              " container=" + container +
              " resource=" + container.getResource() +
              " queue=" + this +
              " usedCapacity=" + getUsedCapacity() +
              " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
              " used=" + usedResources +
              " cluster=" + clusterResource);
        }
      }

      if (removed) {
        // Inform the parent queue _outside_ of the leaf-queue lock
        getParent()
            .completedContainer(clusterResource, application, node, rmContainer,
                        null, event, this, transactionState);
      }
    }
  }

  synchronized void allocateResource(Resource clusterResource,
          FiCaSchedulerApp application, Resource resource,
          TransactionState transactionState) {
    // Update queue metrics
    Resources.addTo(usedResources, resource);
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
            clusterResource, minimumAllocation, transactionState);
    ++numContainers;

    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName, transactionState);
    user.assignContainer(application.getUser(), resource, transactionState);
    Resources.subtractFrom(application.getHeadroom(), resource); // headroom
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());

    if (LOG.isDebugEnabled()) {
      LOG.info(getQueueName() +
              " user=" + userName +
              " used=" + usedResources + " numContainers=" + numContainers +
              " headroom = " + application.getHeadroom() +
              " user-resources=" + user.getConsumedResources());
    }
    ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
            this.getQueuePath(), this);
  }

  synchronized void releaseResource(Resource clusterResource,
          FiCaSchedulerApp application, Resource resource,
          TransactionState transactionState) {
    // Update queue metrics
    Resources.subtractFrom(usedResources, resource);
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(),
            clusterResource, minimumAllocation, transactionState);
    --numContainers;

    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName, transactionState);
    user.releaseContainer(application.getUser(), resource, transactionState);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());

    LOG.info(getQueueName() +
        " used=" + usedResources + " numContainers=" + numContainers +
        " user=" + userName + " user-resources=" + user.getConsumedResources());
    
    ((TransactionStateImpl) transactionState).getCSQueueInfo().addCSQueue(
            this.getQueuePath(), this);
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource,
          TransactionState transactionState) {
    // Update queue properties
    maxActiveApplications = CSQueueUtils
        .computeMaxActiveApplications(resourceCalculator, clusterResource,
            minimumAllocation, maxAMResourcePerQueuePercent,
            absoluteMaxCapacity);
    maxActiveAppsUsingAbsCap = CSQueueUtils
        .computeMaxActiveApplications(resourceCalculator, clusterResource,
            minimumAllocation, maxAMResourcePerQueuePercent, absoluteCapacity);
    maxActiveApplicationsPerUser = CSQueueUtils
        .computeMaxActiveApplicationsPerUser(maxActiveAppsUsingAbsCap,
            userLimit, userLimitFactor);

    // Update metrics
    CSQueueUtils.updateQueueStatistics(resourceCalculator, this, getParent(), 
            clusterResource, minimumAllocation, transactionState);

        // queue metrics are updated, more resource may be available
    // activate the pending applications if possible
    activateApplications(transactionState);

    // Update application properties
    for (FiCaSchedulerApp application : activeApplications) {
      synchronized (application) {
        computeUserLimitAndSetHeadroom(application, clusterResource,
                Resources.none(), transactionState);
      }
    }
  }

  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }

  static class User {
    Resource consumed = Resources.createResource(0, 0);//recovered
    int pendingApplications = 0; //recovered
    int activeApplications = 0; //recovered

    public void recover(CSLeafQueueUserInfo hopUserInfo){
      consumed.setMemory(hopUserInfo.getConsumedMemory());
      consumed.setVirtualCores(hopUserInfo.getConsumedVCores());
      pendingApplications = hopUserInfo.getPendingApplications();
      activeApplications = hopUserInfo.getActiveApplications();
    }
    
    public Resource getConsumedResources() {
      return consumed;
    }

    public int getPendingApplications() {
      return pendingApplications;
    }

    public int getActiveApplications() {
      return activeApplications;
    }

    public int getTotalApplications() {
      return getPendingApplications() + getActiveApplications();
    }

    public synchronized void submitApplication(String userName,
            TransactionState transactionState) {
      ++pendingApplications;
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                getCSLeafUserInfo(userName).setPendingApplications(
                        pendingApplications);
      }
    }

    public synchronized void activateApplication(String userName,
            TransactionState transactionState) {
      --pendingApplications;
      ++activeApplications;
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                getCSLeafUserInfo(userName).setActiveApplications(
                        activeApplications);
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                getCSLeafUserInfo(userName).setPendingApplications(
                        pendingApplications);

      }
    }

    public synchronized void finishApplication(String userName,
            boolean wasActive, TransactionState transactionState) {
      if (wasActive) {
        --activeApplications;
        if (transactionState != null) {
          ((TransactionStateImpl) transactionState).getCSQueueInfo().
                  getCSLeafUserInfo(userName).setActiveApplications(
                          activeApplications);
        }
      } else {
        --pendingApplications;
        if (transactionState != null) {
          ((TransactionStateImpl) transactionState).getCSQueueInfo().
                  getCSLeafUserInfo(userName).setPendingApplications(
                          pendingApplications);
        }
      }
    }

    public synchronized void assignContainer(String userName, Resource resource,
            TransactionState transactionState) {
      Resources.addTo(consumed, resource);
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                getCSLeafUserInfo(userName).setConsumedMemory(consumed.
                        getMemory());
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                getCSLeafUserInfo(userName).setConsumedVCores(consumed.
                        getVirtualCores());
      }

    }

    public synchronized void releaseContainer(String userName, Resource resource,
            TransactionState transactionState) {
      Resources.subtractFrom(consumed, resource);
      if (transactionState != null) {
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                getCSLeafUserInfo(userName).setConsumedMemory(consumed.
                        getMemory());
        ((TransactionStateImpl) transactionState).getCSQueueInfo().
                getCSLeafUserInfo(userName).setConsumedVCores(consumed.
                        getVirtualCores());
      }
    }
  }

  @Override
  public void recoverContainer(Resource clusterResource,
          FiCaSchedulerApp application, Container container) {
    // Careful! Locking order is important! 
    synchronized (this) {
      allocateResource(clusterResource, application, container.getResource(),
              null);
    }
    getParent().recoverContainer(clusterResource, application, container);

  }

  /**
   * Obtain (read-only) collection of active applications.
   */
  public Set<FiCaSchedulerApp> getApplications() {
    // need to access the list of apps from the preemption monitor
    return activeApplications;
  }

  // return a single Resource capturing the overal amount of pending resources
  public Resource getTotalResourcePending() {
    Resource ret = BuilderUtils.newResource(0, 0);
    for (FiCaSchedulerApp f : activeApplications) {
      Resources.addTo(ret, f.getTotalPendingRequests());
    }
    return ret;
  }

  @Override
  public void collectSchedulerApplications(
          Collection<ApplicationAttemptId> apps) {
    for (FiCaSchedulerApp app : activeApplications) {
      apps.add(app.getApplicationAttemptId());
    }
  }

}
