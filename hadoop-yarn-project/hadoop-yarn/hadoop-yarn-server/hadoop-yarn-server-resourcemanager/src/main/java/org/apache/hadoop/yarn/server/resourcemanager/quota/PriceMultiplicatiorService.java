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
package org.apache.hadoop.yarn.server.resourcemanager.quota;

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.quota.PriceMultiplicatorDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.quota.PriceMultiplicator;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.RMStorageFactory;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

public class PriceMultiplicatiorService extends AbstractService {

  private static final Log LOG = LogFactory.getLog(PriceMultiplicatiorService.class);

  private Configuration conf;
  private final RMContext rmcontext;
  private volatile boolean stopped = false;
  private Thread priceCalculationThread;
  private float tippingPoint;
  private float incrementFactor;
  private long priceMultiplicationFactorCalculationInterval;
  private float currentMultiplicator;

  private PriceMultiplicatorDataAccess priceMultiplicatorDA;

  public PriceMultiplicatiorService(RMContext rmctx) {
    super("Price multiplication factor service");
    rmcontext = rmctx;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing price estimation service");
    this.conf = conf;

    // Initialize config parameters
    this.tippingPoint = this.conf.getFloat(
            YarnConfiguration.QUOTA_MULTIPLICATOR_THRESHOLD,
            YarnConfiguration.DEFAULT_QUOTA_MULTIPLICATOR_THRESHOLD);
    this.incrementFactor = this.conf.getFloat(
            YarnConfiguration.QUOTA_INCREMENT_FACTOR,
            YarnConfiguration.DEFAULT_QUOTA_INCREMENT_FACTOR);
    this.priceMultiplicationFactorCalculationInterval = this.conf.getLong(
            YarnConfiguration.QUOTA_PRICE_MULTIPLICATOR_INTERVAL,
            YarnConfiguration.DEFAULT_QUOTA_PRICE_MULTIPLICATOR_INTERVAL);

    // Initialize DataAccesses
    this.priceMultiplicatorDA = (PriceMultiplicatorDataAccess) RMStorageFactory.
            getDataAccess(PriceMultiplicatorDataAccess.class);
    currentMultiplicator = 1;
    recover();
  }

  private void recover() throws IOException {
    Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> currentMultiplicators
            = getCurrentMultiplicator();
    if (currentMultiplicators.get(PriceMultiplicator.MultiplicatorType.VARIABLE)
            != null) {
      currentMultiplicator = currentMultiplicators.get(
              PriceMultiplicator.MultiplicatorType.VARIABLE).getValue();
    }
  }

  private Map<PriceMultiplicator.MultiplicatorType, PriceMultiplicator> getCurrentMultiplicator()
          throws IOException {
    LightWeightRequestHandler currentPriceHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
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

    priceCalculationThread = new Thread(new WorkingThread());
    priceCalculationThread.setName("Price estimation service");
    priceCalculationThread.start();
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

          // Calculate the price based on resource usage
          CalculateNewPrice();

          persistMultiplicator();
          // Pass the latest price to the Containers Log Service          
          ContainersLogsService cl = rmcontext.getContainersLogsService();
          if (cl != null) {
            cl.setCurrentPrice(currentMultiplicator);
          }

          Thread.sleep(priceMultiplicationFactorCalculationInterval);
        } catch (Exception ex) {
          LOG.error(ex, ex);
        }
      }
      LOG.info("Quota scheduler thread is exiting gracefully");
    }
  }

  protected void CalculateNewPrice() throws IOException {

    QueueMetrics metrics = rmcontext.getScheduler().
            getRootQueueMetrics();

    int totalMB = metrics.getAllocatedMB() + metrics.getAvailableMB();
    int totalCores = metrics.getAllocatedVirtualCores() + metrics.
            getAvailableVirtualCores();

    int usedMB = metrics.getAllocatedMB() + metrics.getPendingMB();
    float persentUsedMB = (float) usedMB / totalMB;

    int usedCores = metrics.getAllocatedVirtualCores() + metrics.
            getPendingVirtualCores();
    float persentUsedCores = (float) usedCores / totalCores;

    float incrementBase = Math.max(persentUsedCores, persentUsedMB)
            - tippingPoint;
    incrementBase = Math.max(incrementBase, 0);
    currentMultiplicator = 1 + incrementBase * incrementFactor;

    LOG.debug("New multiplicator: " + currentMultiplicator + " (mem: "
            + persentUsedMB + ", vcores: " + persentUsedCores + ")");

  }

  private void persistMultiplicator() throws IOException {
    LightWeightRequestHandler prepareHandler;
    prepareHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        priceMultiplicatorDA.add(new PriceMultiplicator(
                PriceMultiplicator.MultiplicatorType.VARIABLE,
                currentMultiplicator));

        connector.commit();
        return null;
      }
    };
    prepareHandler.handle();
  }
}
