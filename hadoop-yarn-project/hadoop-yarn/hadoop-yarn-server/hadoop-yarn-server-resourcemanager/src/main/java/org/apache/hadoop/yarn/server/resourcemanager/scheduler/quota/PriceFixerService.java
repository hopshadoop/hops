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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.quota;

import io.hops.exception.StorageException;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.yarn.dal.YarnHistoryPriceDataAccess;
import io.hops.metadata.yarn.dal.YarnRunningPriceDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.YarnHistoryPrice;
import io.hops.metadata.yarn.entity.YarnRunningPrice;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ContainersLogsService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

public class PriceFixerService extends AbstractService {

  private Configuration conf;
  private RMContext rmcontext;
  private volatile boolean stopped = false;
  private Thread priceCalculationThread;
  private float tippingPointMB;
  private float tippingPointVC;
  private float incrementFactorForMemory;
  private float incrementFactorForVirtualCore;
  private float basePricePerTickMB;
  private float basePricePerTickVC;
  private int priceCalculationInterval;
  private float currentPrice;
  private long currentPriceTick = 0;

  private YarnRunningPriceDataAccess runningPriceDA;
  private YarnHistoryPriceDataAccess historyPriceDA;
  private static final Log LOG = LogFactory.getLog(PriceFixerService.class);

  public PriceFixerService(RMContext rmctx) {
    super("Price estimation service");
    rmcontext = rmctx;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing price estimation service");
    this.conf = conf;

    // Initialize config parameters
    this.tippingPointMB = this.conf.getFloat(
            YarnConfiguration.OVERPRICING_THRESHOLD_MB
,            YarnConfiguration.DEFAULT_OVERPRICING_THRESHOLD_MB);
    this.tippingPointVC = this.conf.getFloat(
            YarnConfiguration.OVERPRICING_THRESHOLD_VC,
            YarnConfiguration.DEFAULT_OVERPRICING_THRESHOLD_VC);
    this.incrementFactorForMemory = this.conf.getFloat(
            YarnConfiguration.MEMORY_INCREMENT_FACTOR,
            YarnConfiguration.DEFAULT_MEMORY_INCREMENT_FACTOR);
    this.incrementFactorForVirtualCore = this.conf.getFloat(
            YarnConfiguration.VCORE_INCREMENT_FACTOR,
            YarnConfiguration.DEFAULT_VCOREINCREMENT_FACTOR);
    this.basePricePerTickMB = this.conf.getFloat(
            YarnConfiguration.BASE_PRICE_PER_TICK_FOR_MEMORY,
            YarnConfiguration.DEFAULT_BASE_PRICE_PER_TICK_FOR_MEMORY);
    this.basePricePerTickVC = this.conf.getFloat(
            YarnConfiguration.BASE_PRICE_PER_TICK_FOR_VIRTUAL_CORE,
            YarnConfiguration.DEFAULT_BASE_PRICE_PER_TICK_FOR_VIRTUAL_CORE);
    this.priceCalculationInterval = this.conf.getInt(
            YarnConfiguration.QUOTAS_PRICE_FIXER_INTERVAL,
            YarnConfiguration.DEFAULT_QUOTAS_PRICE_FIXER_INTERVAL);;

    // Initialize DataAccesses
    this.runningPriceDA = (YarnRunningPriceDataAccess) RMStorageFactory.
            getDataAccess(YarnRunningPriceDataAccess.class);
    this.historyPriceDA = (YarnHistoryPriceDataAccess) RMStorageFactory.
            getDataAccess(YarnHistoryPriceDataAccess.class);
    currentPrice = basePricePerTickMB + basePricePerTickVC;
    recover();
  }

  private void recover() {
    Map<YarnRunningPrice.PriceType, YarnRunningPrice> currentPrices
            = getCurrentPrice();
    if (currentPrices.get(YarnRunningPrice.PriceType.VARIABLE) != null) {
      currentPrice = currentPrices.get(YarnRunningPrice.PriceType.VARIABLE).
              getPrice();
      currentPriceTick = currentPrices.get(YarnRunningPrice.PriceType.VARIABLE).
              getTime();
    }
  }

  private Map<YarnRunningPrice.PriceType, YarnRunningPrice> getCurrentPrice() {
    try {
      LightWeightRequestHandler currentPriceHandler
              = new LightWeightRequestHandler(YARNOperationType.TEST) {
        @Override
        public Object performTask() throws StorageException {
          connector.beginTransaction();
          connector.readCommitted();

          Map<YarnRunningPrice.PriceType, YarnRunningPrice> currentPrices
                  = runningPriceDA.
                  getAll();

          connector.commit();

          return currentPrices;
        }
      };
      return (Map<YarnRunningPrice.PriceType, YarnRunningPrice>) currentPriceHandler.
              handle();
    } catch (IOException ex) {
      LOG.warn("Unable to retrieve container statuses", ex);
    }
    return null;
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

          // Pass the latest price to the Containers Log Service          
          if (rmcontext.isLeadingRT()) {
            ContainersLogsService cl = rmcontext.getContainersLogsService();
            if (cl != null) {
              cl.setCurrentPrice(currentPrice);
            }
          }
          Thread.sleep(priceCalculationInterval);
        } catch (IOException ex) {
          LOG.error(ex, ex);
        } catch (InterruptedException ex) {
          Logger.getLogger(PriceFixerService.class.getName()).
                  log(Level.SEVERE, null, ex);
        }

      }
      LOG.info("Quota scheduler thread is exiting gracefully");
    }
  }

  protected void CalculateNewPrice() throws IOException {

    QueueMetrics metrics = rmcontext.getScheduler().
            getRootQueueMetrics();

    int totalMB = metrics.getAllocatedMB() + metrics.getAvailableMB();
    int totalCore = metrics.getAllocatedVirtualCores() + metrics.
            getAvailableVirtualCores();

    int usedMB = metrics.getAllocatedMB() + metrics.getPendingMB();
    float incrementBaseMB = ((float) usedMB / (float) totalMB)
            - tippingPointMB;
    incrementBaseMB = incrementBaseMB > 0 ? incrementBaseMB : 0;
    float newPriceMB = basePricePerTickMB + (incrementBaseMB
            * incrementFactorForMemory);
    LOG.debug("MB use: " + usedMB + " of " + totalMB + " ("
            + (int) ((float) usedMB * 100 / (float) totalMB) + "%) "
            + incrementBaseMB + "% over limit");
    LOG.debug("MB price: " + newPriceMB);

    int usedCore = metrics.getAllocatedVirtualCores() + metrics.
            getPendingVirtualCores();
    float incrementBaseVC = ((float) usedCore / (float) totalCore)
            - tippingPointVC;
    incrementBaseVC = incrementBaseVC > 0 ? incrementBaseVC : 0;
    float newPriceVC = basePricePerTickVC + (incrementBaseVC
            * incrementFactorForVirtualCore);
    LOG.debug("VC use: " + usedCore + " of " + totalCore + " ("
            + (int) ((float) usedCore * 100 / (float) totalCore)
            + "%) " + incrementBaseVC + "% over limit");
    LOG.debug("VC price: " + newPriceVC);

    currentPrice = newPriceMB + newPriceVC;
    currentPriceTick++;

    LOG.debug("New price: " + currentPrice + " (" + currentPriceTick + ")");

    try {
      persistPrice();
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  private void persistPrice() throws IOException {
    LightWeightRequestHandler prepareHandler;
    prepareHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        runningPriceDA.add(new YarnRunningPrice(
                YarnRunningPrice.PriceType.VARIABLE, currentPriceTick,
                currentPrice));
//        historyPriceDA.add(
//                new YarnHistoryPrice(currentPriceTick, currentPrice));

        connector.commit();
        return null;
      }
    };
    prepareHandler.handle();
  }
}
