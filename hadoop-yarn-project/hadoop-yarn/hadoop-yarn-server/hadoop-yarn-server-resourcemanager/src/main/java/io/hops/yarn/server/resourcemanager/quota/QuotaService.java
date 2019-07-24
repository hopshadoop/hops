/*
 * Copyright 2015 Apache Software Foundation.
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
package io.hops.yarn.server.resourcemanager.quota;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.quota.ProjectQuotaDataAccess;
import io.hops.metadata.yarn.dal.quota.ProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.quota.PriceMultiplicator;
import io.hops.metadata.yarn.entity.quota.ProjectDailyCost;
import io.hops.metadata.yarn.entity.quota.ProjectQuota;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.HopsWorksHelper;
import io.hops.util.RMStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

public class QuotaService extends CompositeService {

  private static final Log LOG = LogFactory.getLog(QuotaService.class);

  private Dispatcher dispatcher;
  boolean quotaServiceEnabled;
  Map<ContainerId, ContainerStartData> runningContainers = new ConcurrentHashMap<>();
  Map<ContainerId, Long> containerQuotaTime = new ConcurrentHashMap<>();
  private int minVcores;
  private int minMemory;
  private int minGpus;
  private float basePriceGeneral;
  private float basePriceGpu;
  private long minRunTime;

  private Thread quotaSchedulingThread;
  private volatile boolean stopped = false;
  private long quotaSchedulingPeriod;
  private RMContext rmContext;
  private Map<PriceMultiplicator.MultiplicatorType,Float> currentMultiplicators = new HashMap<>();

  public QuotaService(RMContext rmContext) {
    super(QuotaService.class.getName());
    this.rmContext = rmContext;
  }

  @Override
  protected synchronized void serviceInit(Configuration conf) throws Exception {
    minVcores = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    assert minVcores > 0 : YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES + " should not be null or negative";
    minGpus = Math.max(1, conf.getInt(YarnConfiguration.QUOTA_MINIMUM_CHARGED_GPUS,
        YarnConfiguration.DEFAULT_QUOTA_MINIMUM_CHARGED_GPUS));
    assert minGpus > 0 : YarnConfiguration.QUOTA_MINIMUM_CHARGED_GPUS + " should not be null or negative";
    minMemory = conf.getInt(YarnConfiguration.QUOTA_MINIMUM_CHARGED_MB,
        YarnConfiguration.DEFAULT_QUOTA_MINIMUM_CHARGED_MB);
    assert minMemory > 0 : YarnConfiguration.QUOTA_MINIMUM_CHARGED_MB + " should not be null or negative";
    basePriceGeneral = conf.getFloat(YarnConfiguration.QUOTA_BASE_PRICE_GENERAL,
        YarnConfiguration.DEFAULT_QUOTA_BASE_PRICE_GPU);
    basePriceGpu = conf.getFloat(YarnConfiguration.QUOTA_BASE_PRICE_GPU, YarnConfiguration.DEFAULT_QUOTA_BASE_PRICE_GPU);
    minRunTime = conf.getInt(YarnConfiguration.QUOTA_MIN_RUN_TIME, YarnConfiguration.DEFAULT_QUOTA_MIN_RUN_TIME);
    quotaServiceEnabled = conf.getBoolean(YarnConfiguration.APPLICATION_QUOTA_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_QUOTA_ENABLED);
    quotaSchedulingPeriod = conf.getLong(YarnConfiguration.QUOTA_SCHEDULING_PERIOD,
        YarnConfiguration.DEFAULT_QUOTA_SCHEDULING_PERIOD);
    if (quotaServiceEnabled) {
      dispatcher = createDispatcher(conf);
      dispatcher.register(QuotaEventType.class, new QuotaService.ForwardingEventHandler());
      addIfService(dispatcher);
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    LOG.info("Starting a new quota schedular service");
    if (quotaServiceEnabled) {
      quotaSchedulingThread = new Thread(new WorkingThread());
      quotaSchedulingThread.setName("Quota scheduling");
      quotaSchedulingThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (quotaSchedulingThread != null) {
      quotaSchedulingThread.interrupt();
    }
    super.serviceStop();
    LOG.info("Stopped the quota scheduling service.");
  }

  protected Dispatcher createDispatcher(Configuration conf) {
    QuotaService.MultiThreadedDispatcher dispatcher = new QuotaService.MultiThreadedDispatcher(
        conf.getInt(YarnConfiguration.RM_QUOTA_MULTI_THREADED_DISPATCHER_POOL_SIZE,
            YarnConfiguration.DEFAULT_RM_QUOTA_MULTI_THREADED_DISPATCHER_POOL_SIZE));
    dispatcher.setDrainEventsOnStop();
    return dispatcher;
  }

  protected void handleQuotaEvent(QuotaEvent event) throws IOException {
    switch (event.getType()) {
      case CONTAINER_START:
        QuotaContainerStartEvent qcsEvent = (QuotaContainerStartEvent) event;
        addToRunningContainers(qcsEvent);
        break;
      case CONTAINER_UPDATE:
        QuotaContainerUpdateEvent qcuEvent = (QuotaContainerUpdateEvent) event;
        processContainerUpdate(qcuEvent);
        break;
      case CONTAINER_FINISH:
        QuotaContainerFinishEvent qcfEvent = (QuotaContainerFinishEvent) event;
        processContainerFinish(qcfEvent);
        break;
      case QUOTA_COMPUTE:
        QuotaComputeEvent qcEvent = (QuotaComputeEvent) event;
        processQuotaCompute(qcEvent);
        break;
      default:
        LOG.error("Unknown QuotaEvent type: "
            + event.getType());
    }
  }

  @SuppressWarnings("unchecked")
  public void containerStarted(RMContainer container) {
    if (quotaServiceEnabled) {
      dispatcher.getEventHandler().handle(
          new QuotaContainerStartEvent(container.getContainerId(),
              ContainerStartData.newInstance(container.getContainerId(),
                  container.getAllocatedResource(), container.getAllocatedNode(),
                  container.getAllocatedPriority(), container.getCreationTime())));
    }
  }

  @SuppressWarnings("unchecked")
  public void containerUpdated(RMContainer container, long updateTime) {
    if (quotaServiceEnabled) {
      dispatcher.getEventHandler().handle(
          new QuotaContainerUpdateEvent(container.getContainerId(),
              ContainerStartData.newInstance(container.getContainerId(),
                  container.getAllocatedResource(), container.getAllocatedNode(),
                  container.getAllocatedPriority(), container.getCreationTime()), updateTime));
    }
  }

  @SuppressWarnings("unchecked")
  public void containerFinished(RMContainer container) {
    if (quotaServiceEnabled) {
      dispatcher.getEventHandler().handle(
          new QuotaContainerFinishEvent(container.getContainerId(),
              ContainerFinishData.newInstance(container.getContainerId(),
                  container.getFinishTime(), container.getDiagnosticsInfo(),
                  container.getContainerExitStatus(),
                  container.getContainerState())));
    }
  }

  @SuppressWarnings("unchecked")
  public void computeQuota(ContainerId containerId, long computeTime) {
    if (quotaServiceEnabled) {
      dispatcher.getEventHandler().handle(
          new QuotaComputeEvent(containerId, computeTime));
    }
  }

  private void addToRunningContainers(QuotaContainerStartEvent event) {
    synchronized (event.getContainerStartData()) {
      runningContainers.put(event.getContainerId(), event.getContainerStartData());
      containerQuotaTime.put(event.getContainerId(), event.getContainerStartData().getStartTime());
    }
  }

  private void processContainerFinish(QuotaContainerFinishEvent event) throws IOException {
    ContainerStartData startData = runningContainers.remove(event.getContainerId());
    if (startData != null) {
      computeAndApplyCharge(startData, event.getContainerFinishData().getFinishTime(), true);
    } else {
      LOG.error("Container finished before starting: " + event.getContainerId());
    }
  }

  private void processContainerUpdate(QuotaContainerUpdateEvent event) throws IOException {
    ContainerStartData startData = runningContainers.put(event.getContainerId(), event.getContainerStartData());
    if (startData != null) {
      computeAndApplyCharge(startData, event.getUpdateTime(), false);
    } else {
      LOG.error("Container updated before starting: " + event.getContainerId());
    }
  }

  private void processQuotaCompute(QuotaComputeEvent event) throws IOException {
    ContainerStartData startData = runningContainers.get(event.getContainerId());
    if (startData != null) {
      computeAndApplyCharge(startData, event.getComputeTime(), false);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("comput event received after container finished");
    }
  }

  private void computeAndApplyCharge(final ContainerStartData startData, long endTime, boolean remove) throws
      IOException {
    Long quotaTime = new Long(0);
    if (remove) {
      quotaTime = containerQuotaTime.remove(startData.getContainerId());
      if (endTime - startData.getStartTime() < minRunTime) {
        endTime = startData.getStartTime() + minRunTime - quotaTime;
      }
    } else {
      quotaTime = containerQuotaTime.get(startData.getContainerId());
    }
    if (quotaTime == null) {
      LOG.error("No container should reach this point without having a quotaTime");
      return;
    }

    final Map<String, Long> resourcesMap = new HashMap<>();

    long usedMillis = endTime - quotaTime;
    Resource resource = startData.getAllocatedResource();
    for (ResourceInformation entry : resource.getResources()) {
      resourcesMap.put(entry.getName(), entry.getValue());
    }

    LightWeightRequestHandler quotaSchedulerHandler = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();
        computeAndApplyChargeInt(startData.getContainerId(), resourcesMap, usedMillis);
        connector.commit();
        return null;
      }

    };
    quotaSchedulerHandler.handle();
    if (!remove) {
      containerQuotaTime.put(startData.getContainerId(), endTime);
    }
  }

  private void computeAndApplyChargeInt(ContainerId containerId, Map<String, Long> resourcesMap, long usedMillis) throws
      StorageException {
    //Get Data  ** ProjectQuota **
    ProjectQuotaDataAccess<ProjectQuota> pqDA = (ProjectQuotaDataAccess) RMStorageFactory.getDataAccess(ProjectQuotaDataAccess.class);
    ProjectsDailyCostDataAccess<ProjectDailyCost> pdcDA = (ProjectsDailyCostDataAccess) RMStorageFactory.getDataAccess(
        ProjectsDailyCostDataAccess.class);
    
    final long curentDay = TimeUnit.DAYS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    // Calculate the quota
    // Get ApplicationId from ContainerId
    ApplicationId appId = containerId.getApplicationAttemptId().getApplicationId();

    //Get ProjectId from ApplicationId in ** ApplicationState Table ** 
    String appOwner;

    RMApp app = rmContext.getRMApps().get(appId);
    if (app == null) {
      LOG.error("Application not found: " + appId.toString() + " for container " + containerId);
      return;
    } else {
      appOwner = app.getUser();

    }



    String projectName = HopsWorksHelper.getProjectName(appOwner);
    String user = HopsWorksHelper.getUserName(appOwner);

    //Get Data  ** ProjectQuota **
    ProjectQuota projectQuota = pqDA.get(projectName);
    ProjectDailyCost projectDailyCost = pdcDA.get(projectName, user, curentDay);
    float charge = computeCharge(resourcesMap, usedMillis);
    chargeProjectQuota(projectQuota, projectName, user, containerId, charge);
    //** ProjectDailyCost charging**
    chargeProjectDailyCost(projectDailyCost, projectName, user, curentDay, charge, appId);
    if (projectQuota != null) {
      pqDA.add(projectQuota);
    }
    if(projectDailyCost!=null){
      pdcDA.add(projectDailyCost);
    }
  }

  private float computeCharge(Map<String, Long> resourcesMap, long usedMillis) {
    //the pricePerTick is set for a minimum sized container, the price to pay is
    //proportional to the container size on the most used resource
    float multiplicator = rmContext.getPriceMultiplicatorService().getMultiplicator(PriceMultiplicator.MultiplicatorType.GENERAL);
    float vcoresUsage = (float) resourcesMap.get(ResourceInformation.VCORES_URI) / minVcores;
    float vcoresCredit = (float) usedMillis / DateUtils.MILLIS_PER_SECOND * vcoresUsage * multiplicator * basePriceGeneral;
    float memoryUsage = (float) resourcesMap.get(ResourceInformation.MEMORY_URI) / minMemory;
    float memoryCredit = (float) usedMillis / DateUtils.MILLIS_PER_SECOND * memoryUsage * multiplicator * basePriceGeneral;
    Long nbGpus = resourcesMap.get(ResourceInformation.GPU_URI);
    float gpuCredit = 0;
    if (nbGpus != null && nbGpus != 0) {
      float gpuUsage = (float) nbGpus / minGpus;
      multiplicator = rmContext.getPriceMultiplicatorService().getMultiplicator(PriceMultiplicator.MultiplicatorType.GPU);
      gpuCredit = (float) usedMillis / DateUtils.MILLIS_PER_SECOND * gpuUsage * multiplicator * basePriceGpu;
    }
    float credit = Math.max(gpuCredit, Math.max(vcoresCredit,memoryCredit));
    return credit;
  }

  private void chargeProjectQuota(ProjectQuota projectQuota, String projectName, String user, ContainerId containerId,
      float charge) {
    LOG.info("Quota: project " + projectName + " user " + user + " has been charged " + charge + " for container: "
        + containerId);
    if (projectQuota != null) {
      projectQuota.decrementQuota(charge);
    } else {
      LOG.error("Project not found: " + projectName);
    }
  }

  private void chargeProjectDailyCost(ProjectDailyCost projectDailyCost, String projectName, String user, long day,
      float charge, ApplicationId appId) {

    LOG.debug("Quota: project " + projectName + " user " + user + " has used " + charge + " credits, on day: " + day);

    if (projectDailyCost == null) {
      projectDailyCost = new ProjectDailyCost(projectName, user, day, 0, appId.toString());
    }

    projectDailyCost.incrementCharge(charge, appId.toString());
  }

  /**
   * EventHandler implementation which forward events to QuotaService Making
   * use of it, QuotaService can avoid to have a public handle method
   */
  private final class ForwardingEventHandler implements EventHandler<QuotaEvent> {

    @Override
    public void handle(QuotaEvent event) {
      try{
        handleQuotaEvent(event);
      }catch(IOException ex){
        LOG.error("error handling quota event", ex);
      }
    }

  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static class MultiThreadedDispatcher extends CompositeService implements Dispatcher {

    private List<AsyncDispatcher> dispatchers = new ArrayList<AsyncDispatcher>();

    public MultiThreadedDispatcher(int num) {
      super(MultiThreadedDispatcher.class.getName());
      for (int i = 0; i < num; ++i) {
        AsyncDispatcher dispatcher = createDispatcher();
        dispatchers.add(dispatcher);
        addIfService(dispatcher);
      }
    }

    @Override
    public EventHandler<Event> getEventHandler() {
      return new CompositEventHandler();
    }

    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
      for (AsyncDispatcher dispatcher : dispatchers) {
        dispatcher.register(eventType, handler);
      }
    }

    public void setDrainEventsOnStop() {
      for (AsyncDispatcher dispatcher : dispatchers) {
        dispatcher.setDrainEventsOnStop();
      }
    }

    private class CompositEventHandler implements EventHandler<Event> {

      @Override
      public void handle(Event event) {
        // Use hashCode (of ContainerId) to dispatch the event to the child
        // dispatcher, such that all the quota events of one container will
        // be handled by one thread, the scheduled order of the these events
        // will be preserved
        int index = (event.hashCode() & Integer.MAX_VALUE) % dispatchers.size();
        dispatchers.get(index).getEventHandler().handle(event);
      }

    }

    protected AsyncDispatcher createDispatcher() {
      return new AsyncDispatcher("RM ApplicationQuota dispatcher");
    }

  }

  private class WorkingThread implements Runnable {

    @Override
    public void run() {
      LOG.info("Quota Scheduler started");
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        long currentLoopTime = System.currentTimeMillis();
        for (Map.Entry<ContainerId, ContainerStartData> entry : runningContainers.entrySet()) {
          computeQuota(entry.getKey(), currentLoopTime);
        }
        long duration = System.currentTimeMillis() - currentLoopTime;
        try {
          Thread.sleep(Math.max(1, quotaSchedulingPeriod - duration));
        } catch (InterruptedException ex) {
          LOG.error(ex, ex);
        }
      }
    }
  }
}
