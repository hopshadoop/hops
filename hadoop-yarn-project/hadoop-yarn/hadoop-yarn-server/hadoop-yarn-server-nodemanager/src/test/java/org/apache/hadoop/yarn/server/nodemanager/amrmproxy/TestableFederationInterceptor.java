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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.AMHeartbeatRequestHandler;
import org.apache.hadoop.yarn.server.MockResourceManagerFacade;
import org.apache.hadoop.yarn.server.uam.UnmanagedAMPoolManager;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends the FederationInterceptor and overrides methods to provide a testable
 * implementation of FederationInterceptor.
 */
public class TestableFederationInterceptor extends FederationInterceptor {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestableFederationInterceptor.class);

  private ConcurrentHashMap<String, MockResourceManagerFacade>
      secondaryResourceManagers = new ConcurrentHashMap<>();
  private AtomicInteger runningIndex = new AtomicInteger(0);
  private MockResourceManagerFacade mockRm;

  public TestableFederationInterceptor() {
  }

  public TestableFederationInterceptor(MockResourceManagerFacade homeRM,
      ConcurrentHashMap<String, MockResourceManagerFacade> secondaries) {
    mockRm = homeRM;
    secondaryResourceManagers = secondaries;
  }

  @Override
  protected UnmanagedAMPoolManager createUnmanagedAMPoolManager(
      ExecutorService threadPool) {
    return new TestableUnmanagedAMPoolManager(threadPool);
  }

  @Override
  protected AMHeartbeatRequestHandler createHomeHeartbeartHandler(
      Configuration conf, ApplicationId appId) {
    return new TestableAMRequestHandlerThread(conf, appId);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <T> T createHomeRMProxy(AMRMProxyApplicationContext appContext,
      Class<T> protocol, UserGroupInformation user) {
    synchronized (this) {
      if (mockRm == null) {
        mockRm = new MockResourceManagerFacade(
            new YarnConfiguration(super.getConf()), 0);
      }
    }
    return (T) mockRm;
  }

  @SuppressWarnings("unchecked")
  protected <T> T createSecondaryRMProxy(Class<T> proxyClass,
      Configuration conf, String subClusterId) throws IOException {
    // We create one instance of the mock resource manager per sub cluster. Keep
    // track of the instances of the RMs in the map keyed by the sub cluster id
    synchronized (this.secondaryResourceManagers) {
      if (this.secondaryResourceManagers.containsKey(subClusterId)) {
        return (T) this.secondaryResourceManagers.get(subClusterId);
      } else {
        // The running index here is used to simulate different RM_EPOCH to
        // generate unique container identifiers in a federation environment
        MockResourceManagerFacade rm = new MockResourceManagerFacade(
            new Configuration(conf), runningIndex.addAndGet(10000));
        this.secondaryResourceManagers.put(subClusterId, rm);
        return (T) rm;
      }
    }
  }

  protected void setShouldReRegisterNext() {
    if (mockRm != null) {
      mockRm.setShouldReRegisterNext();
    }
    for (MockResourceManagerFacade subCluster : secondaryResourceManagers
        .values()) {
      subCluster.setShouldReRegisterNext();
    }
  }

  protected MockResourceManagerFacade getHomeRM() {
    return mockRm;
  }

  protected ConcurrentHashMap<String, MockResourceManagerFacade>
      getSecondaryRMs() {
    return secondaryResourceManagers;
  }

  protected MockResourceManagerFacade getSecondaryRM(String scId) {
    return secondaryResourceManagers.get(scId);
  }

  /**
   * Drain all aysnc heartbeat threads, comes in two favors:
   *
   * 1. waitForAsyncHBThreadFinish == false. Only wait for the async threads to
   * pick up all pending heartbeat requests. Not necessarily wait for all
   * threads to finish processing the last request. This is used to make sure
   * all new UAM are launched by the async threads, but at the same time will
   * finish draining while (slow) RM is still processing the last heartbeat
   * request.
   *
   * 2. waitForAsyncHBThreadFinish == true. Wait for all async thread to finish
   * processing all heartbeat requests.
   */
  protected void drainAllAsyncQueue(boolean waitForAsyncHBThreadFinish)
      throws YarnException {

    LOG.info("waiting to drain home heartbeat handler");
    if (waitForAsyncHBThreadFinish) {
      getHomeHeartbeartHandler().drainHeartbeatThread();
    } else {
      while (getHomeHeartbeartHandler().getRequestQueueSize() > 0) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
      }
    }

    LOG.info("waiting to drain UAM heartbeat handlers");
    UnmanagedAMPoolManager uamPool = getUnmanagedAMPool();
    if (waitForAsyncHBThreadFinish) {
      getUnmanagedAMPool().drainUAMHeartbeats();
    } else {
      while (true) {
        boolean done = true;
        for (String scId : uamPool.getAllUAMIds()) {
          if (uamPool.getRequestQueueSize(scId) > 0) {
            done = false;
            break;
          }
        }
        if (done) {
          break;
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
      }
    }
  }

  protected UserGroupInformation getUGIWithToken(
      ApplicationAttemptId appAttemptId) {
    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(appAttemptId.toString());
    AMRMTokenIdentifier token = new AMRMTokenIdentifier(appAttemptId, 1);
    ugi.addTokenIdentifier(token);
    return ugi;
  }

  /**
   * Extends the UnmanagedAMPoolManager and overrides methods to provide a
   * testable implementation of UnmanagedAMPoolManager.
   */
  protected class TestableUnmanagedAMPoolManager
      extends UnmanagedAMPoolManager {
    public TestableUnmanagedAMPoolManager(ExecutorService threadpool) {
      super(threadpool);
    }

    @Override
    public UnmanagedApplicationManager createUAM(Configuration conf,
        ApplicationId appId, String queueName, String submitter,
        String appNameSuffix, boolean keepContainersAcrossApplicationAttempts,
        String rmId) {
      return new TestableUnmanagedApplicationManager(conf, appId, queueName,
          submitter, appNameSuffix, keepContainersAcrossApplicationAttempts);
    }
  }

  /**
   * Extends the UnmanagedApplicationManager and overrides methods to provide a
   * testable implementation.
   */
  protected class TestableUnmanagedApplicationManager
      extends UnmanagedApplicationManager {

    public TestableUnmanagedApplicationManager(Configuration conf,
        ApplicationId appId, String queueName, String submitter,
        String appNameSuffix, boolean keepContainersAcrossApplicationAttempts) {
      super(conf, appId, queueName, submitter, appNameSuffix,
          keepContainersAcrossApplicationAttempts, "TEST");
      setHandlerThread(new TestableAMRequestHandlerThread(conf, appId));
    }

    /**
     * We override this method here to return a mock RM instances. The base
     * class returns the proxy to the real RM which will not work in case of
     * stand alone test cases.
     */
    @Override
    protected <T> T createRMProxy(Class<T> protocol, Configuration config,
        UserGroupInformation user, Token<AMRMTokenIdentifier> token)
        throws IOException {
      return createSecondaryRMProxy(protocol, config,
          YarnConfiguration.getClusterId(config));
    }
  }

  /**
   * Wrap the handler thread so it calls from the same user.
   */
  protected class TestableAMRequestHandlerThread
      extends AMHeartbeatRequestHandler {
    public TestableAMRequestHandlerThread(Configuration conf,
        ApplicationId applicationId) {
      super(conf, applicationId);
    }

    @Override
    public void run() {
      try {
        getUGIWithToken(getAttemptId())
            .doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() {
                TestableAMRequestHandlerThread.super.run();
                return null;
              }
            });
      } catch (Exception e) {
      }
    }
  }
}
