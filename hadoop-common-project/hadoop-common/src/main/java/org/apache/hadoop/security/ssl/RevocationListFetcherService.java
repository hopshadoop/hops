/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.ssl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.service.AbstractService;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Service running on RM, NM, NN, DN and periodically fetches a CRL
 */
public class RevocationListFetcherService extends AbstractService implements RevocationListFetcher {
  private final static Logger LOG = LogManager.getLogger(RevocationListFetcherService.class);
  
  private CRLFetcher crlFetcher;
  private Thread fetcherThread;
  private long fetcherInterval;
  private TimeUnit intervalTimeUnit;
  
  public RevocationListFetcherService() {
    super("CRL fetcher service");
    intervalTimeUnit = TimeUnit.MINUTES;
  }
  
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LOG.info("Initializing CRL fetching service");
    crlFetcher = CRLFetcherFactory.getInstance().getCRLFetcher(conf);
    fetcherInterval = conf.getTimeDuration(CommonConfigurationKeys.HOPS_CRL_FETCHER_INTERVAL_KEY,
        CommonConfigurationKeys.HOPS_CRL_FETCHER_INTERVAL_DEFAULT, intervalTimeUnit);
    // Minimum fetcher interval for normal execution is 1 minute
    if (intervalTimeUnit.equals(TimeUnit.MINUTES) && fetcherInterval < 1) {
      LOG.info("Configured fetcher interval is too low: " + fetcherInterval + " minutes, falling back to 1 minute");
      fetcherInterval = 1;
    }
    super.serviceInit(conf);
  }
  
  @Override
  public void serviceStart() throws Exception {
    LOG.info("Starting CRL fetching service");
    // Start fetcher thread
    if (fetcherThread == null) {
      CountDownLatch readySignal = new CountDownLatch(1);
      fetcherThread = new FetcherThread(readySignal);
      fetcherThread.setDaemon(true);
      fetcherThread.setName("CRL fetcher");
      fetcherThread.start();
      
      // Wait until we fetch the CRL or throw an exception
      boolean ready = readySignal.await(30, TimeUnit.SECONDS);
      if (!ready) {
        String errorMsg = "Waited for more than 30 seconds to fetch the CRL, killing the service";
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }
    }
    super.serviceStart();
  }
  
  @Override
  public void serviceStop() throws Exception {
    LOG.info("Stopping CRL fetching service");
    if (fetcherThread != null) {
      fetcherThread.interrupt();
    }
    super.serviceStop();
  }
  
  // This method should be called before initialize the service and it is *ONLY* for testing
  // The normal interval time unit should be MINUTES
  @VisibleForTesting
  public void setIntervalTimeUnit(TimeUnit intervalTimeUnit) {
    this.intervalTimeUnit = intervalTimeUnit;
  }
  
  @VisibleForTesting
  public long getFetcherInterval() {
    return fetcherInterval;
  }
  
  @VisibleForTesting
  public Thread getFetcherThread() {
    return fetcherThread;
  }
  
  private class FetcherThread extends Thread {
  
    private final CountDownLatch readySignal;
    
    private int numberOfFailures = 0;
    private boolean init = true;
    
    private FetcherThread(CountDownLatch readySignal) {
      this.readySignal = readySignal;
    }
    
    @Override
    public void run() {
      LOG.debug("Starting CRL fetcher thread");
      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (!init) {
            intervalTimeUnit.sleep(fetcherInterval);
          } else {
            init = false;
          }
          crlFetcher.fetch();
          numberOfFailures = 0;
          readySignal.countDown();
        } catch (IOException ex) {
          numberOfFailures++;
          if (numberOfFailures > 5) {
            LOG.error("Failed to fetch CRL more than 5 times. Stopping fetcher thread", ex);
            Thread.currentThread().interrupt();
          }
        } catch (InterruptedException ex) {
          LOG.info("CRL fetcher thread is terminating...", ex);
          Thread.currentThread().interrupt();
        }
      }
      LOG.info("CRL fetcher thread terminated");
    }
  }
}
