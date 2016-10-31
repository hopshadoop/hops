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
package org.apache.hadoop.yarn.server.resourcemanager.quota;

import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.quota.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.quota.PriceMultiplicatorDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.quota.ContainerLog;
import io.hops.metadata.yarn.entity.quota.PriceMultiplicator;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.RMStorageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class ContainersLogsService extends CompositeService {

  private static final Log LOG = LogFactory.getLog(ContainersLogsService.class);

  Configuration conf;
  private Thread tickThread;
  private volatile boolean stopped; //Flag for Thread force stop
  private long monitorInterval; //Time in ms till next ContainerStatus read
  private boolean checkpointEnabled;
  private int checkpointInterval; //Time in ticks between checkpoints
  private double alertThreshold;
  private double threshold;
  private final RMContext rMContext;
  private float currentMultiplicator; // This variable will be set/updated by the streaming service.
  private long multiplicatorPeirod;

  ContainerStatusDataAccess containerStatusDA;
  ContainersLogsDataAccess containersLogsDA;
  VariableDataAccess variableDA;

  Map<String, ContainerLog> activeContainers
          = new HashMap<>();
  Map<String, ContainerLog> updateContainers = new HashMap<>();
  LinkedBlockingQueue<ContainerStatus> eventContainers
          = new LinkedBlockingQueue<>();

  LongVariable tickCounter
          = new LongVariable(Variable.Finder.QuotaTicksCounter, 0);

// True when service is up to speed with existing statuses and
// with events triggered while initializing
  boolean recovered = true;

  public ContainersLogsService(RMContext rMContext) {
    super(ContainersLogsService.class.getName());
    this.rMContext = rMContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing containers logs service");
    this.conf = conf;

    // Initialize config parameters
    this.monitorInterval = this.conf.getLong(
            YarnConfiguration.QUOTA_CONTAINERS_LOGS_MONITOR_INTERVAL,
            YarnConfiguration.DEFAULT_QUOTA_CONTAINERS_LOGS_MONITOR_INTERVAL);
    this.checkpointEnabled = this.conf.getBoolean(
            YarnConfiguration.QUOTA_CONTAINERS_LOGS_CHECKPOINTS_ENABLED,
            YarnConfiguration.DEFAULT_QUOTA_CONTAINERS_LOGS_CHECKPOINTS_ENABLED);
    this.checkpointInterval = this.conf.getInt(
            YarnConfiguration.QUOTA_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS,
            YarnConfiguration.DEFAULT_QUOTA_CONTAINERS_LOGS_CHECKPOINTS_MINTICKS)
            * this.conf.getInt(YarnConfiguration.QUOTA_MIN_TICKS_CHARGE,
                    YarnConfiguration.DEFAULT_QUOTA_MIN_TICKS_CHARGE);
    this.alertThreshold = this.conf.getDouble(
            YarnConfiguration.QUOTA_CONTAINERS_LOGS_ALERT_THRESHOLD,
            YarnConfiguration.DEFAULT_QUOTA_CONTAINERS_LOGS_ALERT_THRESHOLD);
    // Calculate execution time warning threshold
    this.threshold = this.monitorInterval * alertThreshold;

    this.multiplicatorPeirod = this.conf.getLong(
            YarnConfiguration.QUOTA_FIXED_MULTIPLICATOR_PERIOD,
            YarnConfiguration.DEFAULT_QUOTA_FIXED_MULTIPLICATOR_PERIOD)
            * checkpointInterval;


    currentMultiplicator = 1;

    // Initialize DataAccesses
    containerStatusDA = (ContainerStatusDataAccess) RMStorageFactory.
            getDataAccess(ContainerStatusDataAccess.class);
    containersLogsDA = (ContainersLogsDataAccess) RMStorageFactory.
            getDataAccess(ContainersLogsDataAccess.class);
    variableDA = (VariableDataAccess) RMStorageFactory.getDataAccess(
            VariableDataAccess.class);

    // Creates separate thread for retrieving container statuses
    tickThread = new Thread(new TickThread());
    tickThread.setName("ContainersLogs Tick Thread");

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting containers logs service");

    recover();
    tickThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping containers logs service");

    stopped = true;
    if (tickThread != null) {
      tickThread.interrupt();
    }

    super.serviceStop();
  }

  /**
   * Appends container statuses obtained from events into event queue
   *
   * @param changedContainerStatuses
   */
  public void insertEvent(List<ContainerStatus> changedContainerStatuses) {
    LOG.debug("CL :: New event, size: " + changedContainerStatuses.size());
    for (ContainerStatus cs : changedContainerStatuses) {
      try {
        eventContainers.put(cs);
      } catch (InterruptedException ex) {
        LOG.warn("Unable to insert container status: " + cs.toString()
                + " inside event queue", ex);
      }
    }
  }

  public synchronized void setCurrentPrice(float currentPrice) {
    LOG.debug("set new price: " + currentPrice);
    this.currentMultiplicator = currentPrice;
  }

  /**
   * Returns list of latest entries in eventContainers list and removes them
   *
   * @return
   */
  private List<ContainerStatus> getLatestEvents() {
    List<ContainerStatus> oldEvents = new ArrayList<>();

    while (!eventContainers.isEmpty()) {
      oldEvents.add(eventContainers.poll());
    }

    return oldEvents;
  }

  /**
   * Retrieve tick counter, unfinished containers logs entries and container
   * statuses entries. Merge them and then retrieve events that have arrived
   * since launching service. Merge events and mark recovery as completed.
   */
  public void recover() {
    LOG.info("Starting containers logs recovery");

    try {
      tickCounter = getTickCounter();
      activeContainers = getContainersLogs();
      //recover current multiplicator
      Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> currentPrices
              = getCurrentMultiplicator();
      if (currentPrices.get(PriceMultiplicator.MultiplicatorType.VARIABLE)
              != null) {
        this.currentMultiplicator = currentPrices.get(
                PriceMultiplicator.MultiplicatorType.VARIABLE).getValue();
      }
      //Finish to log all the containers for which we currently have logs
      //they will restart once they send a new heartbeat
      finishLogging();

      updateContainersLogs(false);

      LOG.info("Finished containers logs recovery");
    } catch (Exception ex) {
      LOG.warn("Unable to finish containers logs recovery", ex);
    }
  }

  private void finishLogging(){
    for(ContainerLog log: activeContainers.values()){
      log.setStop(tickCounter.getValue());
      log.setExitstatus(ContainerExitStatus.ABORTED);
      updateContainers.put(log.getContainerId(), log);
    }
    activeContainers.clear();
  }
  
  private synchronized void checkEventContainerStatuses(
          List<ContainerStatus> latestEvents
  ) {
    for (ContainerStatus cs : latestEvents) {
      ContainerLog cl;
      boolean updatable = false;
      if (cs.getState().equals(ContainerState.NEW)) {
        continue;
      }
      cl = activeContainers.get(cs.getContainerid());
      if (cl == null) {
        Resource containerResources = rMContext.getScheduler().getRMContainer(
                ConverterUtils.toContainerId(cs.getContainerid())).
                getContainer().getResource();
        cl = new ContainerLog(cs.getContainerid(), tickCounter.getValue(),
                ContainerExitStatus.CONTAINER_RUNNING_STATE,
                currentMultiplicator, containerResources.getVirtualCores(),
                containerResources.getMemory());
        
        // Unable to capture start use case
        if (cs.getState().equals(ContainerState.COMPLETE.toString())) {
          //TODO: this is overwriten by the next cl.setExitstatus, need to be verified
          cl.setExitstatus(ContainerExitStatus.UNKNOWN_CONTAINER_EXIT);
        }

        activeContainers.put(cl.getContainerid(), cl);
        updatable = true;
      }

      if (cs.getState().equals(ContainerState.COMPLETE.toString())) {
        cl.setStop(tickCounter.getValue());
        cl.setExitstatus(cs.getExitstatus());
        activeContainers.remove(cl.getContainerid());
        updatable = true;
      }

      if (updatable) {
        updateContainers.put(cl.getContainerid(), cl);
      }
    }
  }

  /**
   * Updates containers logs table with container status information in update
   * list Also update tick counter in YARN variables table
   */
  private void updateContainersLogs(final boolean updatetTick) {

    try {
      LightWeightRequestHandler containersLogsHandler
              = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws StorageException {
          connector.beginTransaction();
          connector.writeLock();

          // Update containers logs table if necessary
          if (updateContainers.size() > 0) {
            LOG.debug("CL :: Update containers logs size: " + updateContainers.
                    size());
            try {
              containersLogsDA.addAll(updateContainers.values());
            } catch (StorageException ex) {
              LOG.warn("Unable to update containers logs table", ex);
            }
          }

          // Update tick counter
          if (updatetTick) {
            variableDA.setVariable(tickCounter);
          }

          connector.commit();
          return null;
        }
      };
      containersLogsHandler.handle();

      QuotaService quotaService = rMContext.getQuotaService();
      if (quotaService != null) {
        quotaService.insertEvents(updateContainers.values());
      }
      updateContainers.clear();

    } catch (IOException ex) {
      LOG.warn("Unable to update containers logs and tick counter", ex);
    }
  }

  /**
   * Retrieves unfinished containers logs entries Used when initializing
   * active list
   *
   * @return
   */
  private Map<String, ContainerLog> getContainersLogs() {
    Map<String, ContainerLog> allContainersLogs
            = new HashMap<>();

    try {
      // Retrieve unfinished containers logs entries
      LightWeightRequestHandler allContainersHandler
              = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws StorageException {
          connector.beginTransaction();
          connector.readCommitted();

          Map<String, ContainerLog> allContainersLogs
                  = containersLogsDA.getAll();

          connector.commit();

          return allContainersLogs;
        }
      };
      allContainersLogs = (Map<String, ContainerLog>) allContainersHandler.
              handle();

    } catch (IOException ex) {
      LOG.warn("Unable to retrieve containers logs table data", ex);
    }

    return allContainersLogs;
  }

  /**
   * Retrieves containers logs tick counter from YARN variables
   *
   * @return
   */
  private LongVariable getTickCounter() {
    LongVariable tc = new LongVariable(Variable.Finder.QuotaTicksCounter, 0);

    try {
      LongVariable found;

      LightWeightRequestHandler tickCounterHandler
              = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws StorageException {
          connector.beginTransaction();
          connector.readCommitted();

          Variable tickCounterVariable = (Variable) variableDA.getVariable(
                  Variable.Finder.QuotaTicksCounter);

          connector.commit();

          return tickCounterVariable;
        }
      };
      found = (LongVariable) tickCounterHandler.handle();

      if (found != null && found.getValue() != null) {
        tc = found;
      }
    } catch (IOException ex) {
      LOG.warn("Unable to retrieve tick counter from YARN variables", ex);
    }

    return tc;
  }

  private Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> getCurrentMultiplicator()
          throws IOException {
    LightWeightRequestHandler currentPriceHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.readCommitted();

        PriceMultiplicatorDataAccess da
                = (PriceMultiplicatorDataAccess) RMStorageFactory.getDataAccess(
                        PriceMultiplicatorDataAccess.class);

        Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> currentPrices
                = da.getAll();

        connector.commit();

        return currentPrices;
      }
    };
    return (Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator>) currentPriceHandler.
            handle();
  }

//TODO optimisation
  /**
   * Loop active list and add all found & not completed container statuses to
   * update list. This ensures that whole running time is not lost.
   */
  private synchronized void createCheckpoint() {
    long tick = tickCounter.getValue();
    for (ContainerLog log : activeContainers.values()) {
      if ((tick - log.getStart()) % checkpointInterval == 0) {
        log.setStop(tickCounter.getValue());
        if ((tick - log.getStart()) % multiplicatorPeirod == 0) {
          log.setPrice(currentMultiplicator);
        }

        updateContainers.put(log.getContainerid(), log);
      }
    }
  }

  /**
   * Retrieve latest events from the queue;
   * Update active and update lists with latest events
   * Perform checkpoint if necessary
   * Update containers logs table
   */
  public void processTick() {
    List<ContainerStatus> latestEvents = getLatestEvents();

    LOG.debug("CL :: Event count: " + latestEvents.size());

    // Go through all events and update active and update lists
    checkEventContainerStatuses(latestEvents);

    // Checkpoint
    if (checkpointEnabled) {
      createCheckpoint();
    }

    LOG.debug("CL :: Update list size: " + updateContainers.size());
    LOG.debug("CL :: Active list size: " + activeContainers.size());

    // Update Containers logs table and tick counter
    updateContainersLogs(true);
  }

  /**
   * Thread that retrieves container statuses, updates active and update
   * lists, and updates containers logs table and tick counter
   */
  private class TickThread implements Runnable {

    @Override
    public void run() {
      try {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          long executionTime = 0;

          long startTime = System.currentTimeMillis();

          if (recovered) {
            LOG.debug("CL :: Current tick: " + tickCounter.getValue());

            // Process everything for single tick
            processTick();

            // Increment tick counter
            tickCounter = new LongVariable(Variable.Finder.QuotaTicksCounter,
                    tickCounter.getValue() + 1);
          } else {
            LOG.debug("CL :: Not yet recovered");
          }

          //Check alert threshold
          executionTime = System.currentTimeMillis() - startTime;
          if (threshold < executionTime) {
            LOG.warn("Monitor interval threshold exceeded!"
                    + " Execution time: " + Long.toString(executionTime) + "ms."
                    + " Threshold: " + Double.toString(threshold) + "ms."
                    + " Consider increasing monitor interval!");
            //To avoid negative values
            executionTime = (executionTime > monitorInterval) ? monitorInterval
                    : executionTime;
          }

          Thread.sleep(Math.max(0,monitorInterval - executionTime));
        }
      } catch (InterruptedException ex) {
        LOG.error(ex, ex);
      }
    }
  }

  public long getCurrentTick() {
    return tickCounter.getValue();
  }
}
