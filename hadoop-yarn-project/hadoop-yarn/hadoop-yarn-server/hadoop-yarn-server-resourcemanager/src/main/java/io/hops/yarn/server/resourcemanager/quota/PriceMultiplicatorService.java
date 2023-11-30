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
package io.hops.yarn.server.resourcemanager.quota;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.quota.PriceMultiplicatorDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.quota.PriceMultiplicator;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.RMStorageFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

public class PriceMultiplicatorService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(PriceMultiplicatorService.class);

  private boolean isVariablePrice;
  private final RMContext rmContext;
  private volatile boolean stopped = false;
  private Thread priceCalculationThread;
  private Map<PriceMultiplicator.MultiplicatorType,Float> tippingPoints = new HashMap<>();
  private Map<PriceMultiplicator.MultiplicatorType,Float> incrementFactors = new HashMap<>();
  private long priceMultiplicationFactorCalculationInterval;
  private Map<PriceMultiplicator.MultiplicatorType,Float> currentMultiplicators = new ConcurrentHashMap<>();

  private PriceMultiplicatorDataAccess priceMultiplicatorDA;

  public PriceMultiplicatorService(RMContext rmctx) {
    super("Price multiplication factor service");
    rmContext = rmctx;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing price estimation service");

    // Initialize config parameters
    this.tippingPoints.put(PriceMultiplicator.MultiplicatorType.GENERAL, conf.getFloat(
            YarnConfiguration.QUOTA_MULTIPLICATOR_THRESHOLD_GENERAL,
            YarnConfiguration.DEFAULT_QUOTA_MULTIPLICATOR_THRESHOLD_GENERAL));
    this.tippingPoints.put(PriceMultiplicator.MultiplicatorType.GPU, conf.getFloat(
            YarnConfiguration.QUOTA_MULTIPLICATOR_THRESHOLD_GPU,
            YarnConfiguration.DEFAULT_QUOTA_MULTIPLICATOR_THRESHOLD_GPU));
    this.incrementFactors.put(PriceMultiplicator.MultiplicatorType.GENERAL, conf.getFloat(
            YarnConfiguration.QUOTA_INCREMENT_FACTOR_GENERAL,
            YarnConfiguration.DEFAULT_QUOTA_INCREMENT_FACTOR_GENERAL));
    this.incrementFactors.put(PriceMultiplicator.MultiplicatorType.GPU, conf.getFloat(
            YarnConfiguration.QUOTA_INCREMENT_FACTOR_GPU,
            YarnConfiguration.DEFAULT_QUOTA_INCREMENT_FACTOR_GPU));
    this.priceMultiplicationFactorCalculationInterval = conf.getLong(
            YarnConfiguration.QUOTA_PRICE_MULTIPLICATOR_INTERVAL,
            YarnConfiguration.DEFAULT_QUOTA_PRICE_MULTIPLICATOR_INTERVAL);
    
    isVariablePrice = conf.getBoolean(YarnConfiguration.QUOTA_VARIABLE_PRICE_ENABLED,
        YarnConfiguration.DEFAULT_QUOTA_VARIABLE_PRICE_ENABLED);

    if (isVariablePrice) {
      for (PriceMultiplicator.MultiplicatorType type : PriceMultiplicator.MultiplicatorType.values()) {
        currentMultiplicators.put(type, new Float(1));
      }
    }
    super.serviceInit(conf);
  }

  private void recover() throws IOException {
    // Initialize DataAccesses
    this.priceMultiplicatorDA = (PriceMultiplicatorDataAccess) RMStorageFactory.
        getDataAccess(PriceMultiplicatorDataAccess.class);
    Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> currentMultiplicators = getCurrentMultiplicator();
    for (PriceMultiplicator.MultiplicatorType type : PriceMultiplicator.MultiplicatorType.values()) {
      if (currentMultiplicators.get(type) != null) {
        this.currentMultiplicators.put(type, currentMultiplicators.get(type).getValue());
      }
    }
  }

  private Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> getCurrentMultiplicator()
          throws IOException {
    LightWeightRequestHandler currentPriceHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.readCommitted();

        Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> currentPrices
                = priceMultiplicatorDA.getAll();

        connector.commit();

        return currentPrices;
      }
    };
    return (Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator>) currentPriceHandler.
            handle();
  }

  @Override
  protected void serviceStart() throws Exception {
    assert !stopped : "starting when already stopped";
    LOG.info("Starting a new price estimation service.");
    if (isVariablePrice) {
      recover();
      priceCalculationThread = new Thread(new WorkingThread());
      priceCalculationThread.setName("Price estimation service");
      priceCalculationThread.setDaemon(true);
      priceCalculationThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (priceCalculationThread != null) {
      priceCalculationThread.interrupt();
    }
    super.serviceStop();
    LOG.info("Stopping the price estimation service.");
  }

  private class WorkingThread implements Runnable {

    @Override
    public void run() {
      LOG.info("Price estimation service started");
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          long start = System.currentTimeMillis();
          // Calculate the price based on resource usage
          computeNewGeneralPrice();
          computeNewGpuPrice();

          persistMultiplicators();
          
          long duration = System.currentTimeMillis() - start;
          Thread.sleep(Math.max(priceMultiplicationFactorCalculationInterval - duration, 1));
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        } catch (IOException ex) {
          LOG.error(ex, ex);
          try {
            Thread.sleep(500);
          } catch (InterruptedException ex1) {
            Thread.currentThread().interrupt();
          }
        }
      }
      LOG.info("Quota scheduler thread is exiting gracefully");
    }
  }

  protected void computeNewGpuPrice() {
    QueueMetrics metrics = rmContext.getScheduler().getRootQueueMetrics();
    float incrementBase = getPercenUsedGpus() - tippingPoints.get(PriceMultiplicator.MultiplicatorType.GPU);
    incrementBase = Math.max(incrementBase, 0);
    float multiplicator = 1 + incrementBase * incrementFactors.get(PriceMultiplicator.MultiplicatorType.GPU);
    multiplicator = Math.max(multiplicator, currentMultiplicators.get(PriceMultiplicator.MultiplicatorType.GENERAL));
    currentMultiplicators.put(PriceMultiplicator.MultiplicatorType.GPU, multiplicator);

    if (LOG.isDebugEnabled()) {
      LOG.debug("New multiplicator: " + currentMultiplicators + " (mem: "
          + getPercenUsedMB(metrics) + ", vcores: " + getPercenUsedCores(metrics) + ", gpus: "
          + getPercenUsedGpus() + ")");
    }

  }

  private float getPercenUsedGpus() {
    List<SchedulerNode> nodes = ((AbstractYarnScheduler) rmContext.getScheduler()).getAllNodes();
    
    long totalGPUs = 0;
    long usedGPUs = 0;
    
    for(SchedulerNode node: nodes){
      totalGPUs += node.getTotalResource().getResourceValue(ResourceInformation.GPU_URI);
      usedGPUs += node.getAllocatedResource().getResourceValue(ResourceInformation.GPU_URI);
    }
    
    return totalGPUs == 0 ? 0 : (float) usedGPUs / totalGPUs;
  }

  private float getPercenUsedCores(QueueMetrics metrics) {
    int totalCores = metrics.getAllocatedVirtualCores() + metrics.getAvailableVirtualCores();
    int usedCores = metrics.getAllocatedVirtualCores() + metrics.getPendingVirtualCores();
    return totalCores == 0 ? 0 : (float) usedCores / totalCores;
  }

  private float getPercenUsedMB(QueueMetrics metrics) {
    long totalMB = metrics.getAllocatedMB() + metrics.getAvailableMB();
    long usedMB = metrics.getAllocatedMB() + metrics.getPendingMB();
    return totalMB == 0 ? 0 : (float) usedMB / totalMB;
  }

  protected void computeNewGeneralPrice() throws IOException {

    QueueMetrics metrics = rmContext.getScheduler().getRootQueueMetrics();
    float incrementBase = Math.max(getPercenUsedCores(metrics), getPercenUsedMB(metrics))
        - tippingPoints.get(PriceMultiplicator.MultiplicatorType.GENERAL);
    incrementBase = Math.max(incrementBase, 0);
    currentMultiplicators.put(PriceMultiplicator.MultiplicatorType.GENERAL, 1 + incrementBase * incrementFactors.get(
        PriceMultiplicator.MultiplicatorType.GENERAL));

    LOG.debug("New multiplicator: " + currentMultiplicators + " (mem: "
        + getPercenUsedMB(metrics) + ", vcores: " + getPercenUsedCores(metrics) + ")");

  }

  private void persistMultiplicators() throws IOException {
    LightWeightRequestHandler prepareHandler;
    prepareHandler = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();
        for (Map.Entry<PriceMultiplicator.MultiplicatorType, Float> entry : currentMultiplicators.entrySet()) {
          priceMultiplicatorDA.add(new PriceMultiplicator(entry.getKey(), entry.getValue()));
        }
        connector.commit();
        LOG.debug("Commited new multiplicator: " + currentMultiplicators + "for VARIABLE");
        return null;
      }
    };
    prepareHandler.handle();
  }
  
  public float getMultiplicator(PriceMultiplicator.MultiplicatorType type){
    return currentMultiplicators.get(type);
  }
}
