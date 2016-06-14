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

import com.google.common.base.Splitter;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@InterfaceAudience.Private
@Metrics(context = "yarn")
public class QueueMetrics implements MetricsSource {
  @Metric("# of apps submitted")
  MutableCounterInt appsSubmitted; 
  @Metric("# of running apps")
  MutableGaugeInt appsRunning;
  @Metric("# of pending apps")
  MutableGaugeInt appsPending;
  @Metric("# of apps completed")
  MutableCounterInt appsCompleted;
  @Metric("# of apps killed")
  MutableCounterInt appsKilled;
  @Metric("# of apps failed")
  MutableCounterInt appsFailed;

  @Metric("Allocated memory in MB")
  MutableGaugeInt allocatedMB;
  @Metric("Allocated CPU in virtual cores")
  MutableGaugeInt allocatedVCores;
  @Metric("# of allocated containers")
  MutableGaugeInt allocatedContainers;
  @Metric("Aggregate # of allocated containers")
  MutableCounterLong aggregateContainersAllocated;
  @Metric("Aggregate # of released containers")
  MutableCounterLong aggregateContainersReleased;
  @Metric("Available memory in MB")
  MutableGaugeInt availableMB;
  @Metric("Available CPU in virtual cores")
  MutableGaugeInt availableVCores;
  @Metric("Pending memory allocation in MB")
  MutableGaugeInt pendingMB;
  @Metric("Pending CPU allocation in virtual cores")
  MutableGaugeInt pendingVCores;
  @Metric("# of pending containers")
  MutableGaugeInt pendingContainers;
  @Metric("# of reserved memory in MB")
  MutableGaugeInt reservedMB;
  @Metric("Reserved CPU in virtual cores")
  MutableGaugeInt reservedVCores;
  @Metric("# of reserved containers")
  MutableGaugeInt reservedContainers;
  @Metric("# of active users")
  MutableGaugeInt activeUsers;
  @Metric("# of active applications")
  MutableGaugeInt activeApplications;
  private final MutableGaugeInt[] runningTime;
  private TimeBucketMetrics<ApplicationId> runBuckets;

  static final Logger LOG = LoggerFactory.getLogger(QueueMetrics.class);
  static final MetricsInfo RECORD_INFO =
      info("QueueMetrics", "Metrics for the resource scheduler");
  protected static final MetricsInfo QUEUE_INFO =
      info("Queue", "Metrics by queue");
  static final MetricsInfo USER_INFO = info("User", "Metrics by user");
  static final Splitter Q_SPLITTER =
      Splitter.on('.').omitEmptyStrings().trimResults();

  final MetricsRegistry registry;
  final String queueName;
  final QueueMetrics parent;
  final MetricsSystem metricsSystem;
  private final Map<String, QueueMetrics> users;
  private final Configuration conf;

  protected QueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    registry = new MetricsRegistry(RECORD_INFO);
    this.queueName = queueName;
    this.parent = parent != null ? parent.getMetrics() : null;
    this.users = enableUserMetrics ? new HashMap<String, QueueMetrics>() : null;
    metricsSystem = ms;
    this.conf = conf;
    runningTime = buildBuckets(conf);
  }

  protected QueueMetrics tag(MetricsInfo info, String value) {
    registry.tag(info, value);
    return this;
  }

  protected static StringBuilder sourceName(String queueName) {
    StringBuilder sb = new StringBuilder(RECORD_INFO.name());
    int i = 0;
    for (String node : Q_SPLITTER.split(queueName)) {
      sb.append(",q").append(i++).append('=').append(node);
    }
    return sb;
  }

  public synchronized static QueueMetrics forQueue(String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    return forQueue(DefaultMetricsSystem.instance(), queueName, parent,
        enableUserMetrics, conf);
  }

  /**
   * Helper method to clear cache.
   */
  @Private
  public synchronized static void clearQueueMetrics() {
    queueMetrics.clear();
  }
  
  /**
   * Simple metrics cache to help prevent re-registrations.
   */
  protected final static Map<String, QueueMetrics> queueMetrics =
      new HashMap<String, QueueMetrics>();
  
  public synchronized static QueueMetrics forQueue(MetricsSystem ms,
      String queueName, Queue parent, boolean enableUserMetrics,
      Configuration conf) {
    QueueMetrics metrics = queueMetrics.get(queueName);
    if (metrics == null) {
      metrics =
          new QueueMetrics(ms, queueName, parent, enableUserMetrics, conf).
              tag(QUEUE_INFO, queueName);
      
      // Register with the MetricsSystems
      if (ms != null) {
        metrics = ms.register(sourceName(queueName).toString(),
            "Metrics for queue: " + queueName, metrics);
      }
      queueMetrics.put(queueName, metrics);
    }

    return metrics;
  }

  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }
    QueueMetrics metrics = users.get(userName);
    if (metrics == null) {
      metrics = new QueueMetrics(metricsSystem, queueName, null, false, conf);
      users.put(userName, metrics);
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '" + userName + "' in queue '" + queueName + "'",
          metrics.tag(QUEUE_INFO, queueName).tag(USER_INFO, userName));
    }
    return metrics;
  }

  private ArrayList<Integer> parseInts(String value) {
    ArrayList<Integer> result = new ArrayList<Integer>();
    for (String s : value.split(",")) {
      result.add(Integer.parseInt(s.trim()));
    }
    return result;
  }

  private MutableGaugeInt[] buildBuckets(Configuration conf) {
    ArrayList<Integer> buckets = parseInts(
        conf.get(YarnConfiguration.RM_METRICS_RUNTIME_BUCKETS,
            YarnConfiguration.DEFAULT_RM_METRICS_RUNTIME_BUCKETS));
    MutableGaugeInt[] result = new MutableGaugeInt[buckets.size() + 1];
    result[0] = registry.newGauge("running_0", "", 0);
    long[] cuts = new long[buckets.size()];
    for (int i = 0; i < buckets.size(); ++i) {
      result[i + 1] = registry.newGauge("running_" + buckets.get(i), "", 0);
      cuts[i] = buckets.get(i) * 1000L * 60; // covert from min to ms
    }
    this.runBuckets = new TimeBucketMetrics<ApplicationId>(cuts);
    return result;
  }

  private void updateRunningTime() {
    int[] counts = runBuckets.getBucketCounts(System.currentTimeMillis());
    for (int i = 0; i < counts.length; ++i) {
      runningTime[i].set(counts[i]);
    }
  }

  public void getMetrics(MetricsCollector collector, boolean all) {
    updateRunningTime();
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  public void submitApp(String user) {
    appsSubmitted.incr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.submitApp(user);
    }
    if (parent != null) {
      parent.submitApp(user);
    }
  }

  public void submitAppAttempt(String user) {
    appsPending.incr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.submitAppAttempt(user);
    }
    if (parent != null) {
      parent.submitAppAttempt(user);
    }
  }

  public void runAppAttempt(ApplicationId appId, String user) {
    runBuckets.add(appId, System.currentTimeMillis());
    appsRunning.incr();
    appsPending.decr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.runAppAttempt(appId, user);
    }
    if (parent != null) {
      parent.runAppAttempt(appId, user);
    }
  }

  public void finishAppAttempt(ApplicationId appId, boolean isPending,
      String user) {
    runBuckets.remove(appId);
    if (isPending) {
      appsPending.decr();
    } else {
      appsRunning.decr();
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.finishAppAttempt(appId, isPending, user);
    }
    if (parent != null) {
      parent.finishAppAttempt(appId, isPending, user);
    }
  }

  public void finishApp(String user, RMAppState rmAppFinalState) {
    switch (rmAppFinalState) {
      case KILLED:
        appsKilled.incr();
        break;
      case FAILED:
        appsFailed.incr();
        break;
      default:
        appsCompleted.incr();
        break;
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.finishApp(user, rmAppFinalState);
    }
    if (parent != null) {
      parent.finishApp(user, rmAppFinalState);
    }
  }
  
  public void moveAppFrom(AppSchedulingInfo app) {
    if (app.isPending()) {
      appsPending.decr();
    } else {
      appsRunning.decr();
    }
    QueueMetrics userMetrics = getUserMetrics(app.getUser());
    if (userMetrics != null) {
      userMetrics.moveAppFrom(app);
    }
    if (parent != null) {
      parent.moveAppFrom(app);
    }
  }
  
  public void moveAppTo(AppSchedulingInfo app) {
    if (app.isPending()) {
      appsPending.incr();
    } else {
      appsRunning.incr();
    }
    QueueMetrics userMetrics = getUserMetrics(app.getUser());
    if (userMetrics != null) {
      userMetrics.moveAppTo(app);
    }
    if (parent != null) {
      parent.moveAppTo(app);
    }
  }

  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   *
   * @param limit
   *     resource limit
   */
  public void setAvailableResourcesToQueue(Resource limit) {
    availableMB.set(limit.getMemory());
    availableVCores.set(limit.getVirtualCores());
  }

  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   *
   * @param user
   * @param limit
   *     resource limit
   */
  public void setAvailableResourcesToUser(String user, Resource limit) {
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.setAvailableResourcesToQueue(limit);
    }
  }

  /**
   * Increment pending resource metrics
   *
   * @param user
   * @param containers
   * @param res
   *     the TOTAL delta of resources note this is different from
   *     the other APIs which use per container resource
   */
  public void incrPendingResources(String user, int containers, Resource res) {
    _incrPendingResources(containers, res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.incrPendingResources(user, containers, res);
    }
    if (parent != null) {
      parent.incrPendingResources(user, containers, res);
    }
  }

  private void _incrPendingResources(int containers, Resource res) {
    pendingContainers.incr(containers);
    pendingMB.incr(res.getMemory() * containers);
    pendingVCores.incr(res.getVirtualCores() * containers);
  }

  public void decrPendingResources(String user, int containers, Resource res) {
    _decrPendingResources(containers, res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.decrPendingResources(user, containers, res);
    }
    if (parent != null) {
      parent.decrPendingResources(user, containers, res);
    }
  }

  private void _decrPendingResources(int containers, Resource res) {
    pendingContainers.decr(containers);
    pendingMB.decr(res.getMemory() * containers);
    pendingVCores.decr(res.getVirtualCores() * containers);
  }

  public void allocateResources(String user, int containers, Resource res,
      boolean decrPending) {
    allocatedContainers.incr(containers);
    aggregateContainersAllocated.incr(containers);
    allocatedMB.incr(res.getMemory() * containers);
    allocatedVCores.incr(res.getVirtualCores() * containers);
    if (decrPending) {
      _decrPendingResources(containers, res);
    }
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.allocateResources(user, containers, res, decrPending);
    }
    if (parent != null) {
      parent.allocateResources(user, containers, res, decrPending);
    }
  }

  public void releaseResources(String user, int containers, Resource res) {
    allocatedContainers.decr(containers);
    aggregateContainersReleased.incr(containers);
    allocatedMB.decr(res.getMemory() * containers);
    allocatedVCores.decr(res.getVirtualCores() * containers);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.releaseResources(user, containers, res);
    }
    if (parent != null) {
      parent.releaseResources(user, containers, res);
    }
  }

  public void reserveResource(String user, Resource res) {
    reservedContainers.incr();
    reservedMB.incr(res.getMemory());
    reservedVCores.incr(res.getVirtualCores());
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.reserveResource(user, res);
    }
    if (parent != null) {
      parent.reserveResource(user, res);
    }
  }

  public void unreserveResource(String user, Resource res) {
    reservedContainers.decr();
    reservedMB.decr(res.getMemory());
    reservedVCores.decr(res.getVirtualCores());
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.unreserveResource(user, res);
    }
    if (parent != null) {
      parent.unreserveResource(user, res);
    }
  }

  public void incrActiveUsers() {
    activeUsers.incr();
  }
  
  public void decrActiveUsers() {
    activeUsers.decr();
  }
  
  public void activateApp(String user) {
    activeApplications.incr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.activateApp(user);
    }
    if (parent != null) {
      parent.activateApp(user);
    }
  }
  
  public void deactivateApp(String user) {
    activeApplications.decr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.deactivateApp(user);
    }
    if (parent != null) {
      parent.deactivateApp(user);
    }
  }
  

  public long getAggregateContainersAllocated() {
    return aggregateContainersAllocated.value();
  }
  
  public long getaggregateContainersReleased() {
    return aggregateContainersReleased.value();
  }
  
  public String getQueueName() {
    return queueName;
  }

  
  public int getAppsSubmitted() {
    return appsSubmitted.value();
  }

  public int getAppsRunning() {
    return appsRunning.value();
  }

  public int getAppsPending() {
    return appsPending.value();
  }

  public int getAppsCompleted() {
    return appsCompleted.value();
  }

  public int getAppsKilled() {
    return appsKilled.value();
  }

  public int getAppsFailed() {
    return appsFailed.value();
  }
  
  public Resource getAllocatedResources() {
    return BuilderUtils
        .newResource(allocatedMB.value(), allocatedVCores.value());
  }

  public int getAllocatedMB() {
    return allocatedMB.value();
  }
  
  public int getAllocatedVirtualCores() {
    return allocatedVCores.value();
  }

  public int getAllocatedContainers() {
    return allocatedContainers.value();
  }

  public int getAvailableMB() {
    return availableMB.value();
  }
  
  public int getAvailableVirtualCores() {
    return availableVCores.value();
  }

  public int getPendingMB() {
    return pendingMB.value();
  }
  
  public int getPendingVirtualCores() {
    return pendingVCores.value();
  }

  public int getPendingContainers() {
    return pendingContainers.value();
  }
  
  public int getReservedMB() {
    return reservedMB.value();
  }
  
  public int getReservedVirtualCores() {
    return reservedVCores.value();
  }

  public int getReservedContainers() {
    return reservedContainers.value();
  }
  
  public int getActiveUsers() {
    return activeUsers.value();
  }
  
  public int getActiveApps() {
    return activeApplications.value();
  }
  
  public MetricsSystem getMetricsSystem() {
    return metricsSystem;
  }
  
}
