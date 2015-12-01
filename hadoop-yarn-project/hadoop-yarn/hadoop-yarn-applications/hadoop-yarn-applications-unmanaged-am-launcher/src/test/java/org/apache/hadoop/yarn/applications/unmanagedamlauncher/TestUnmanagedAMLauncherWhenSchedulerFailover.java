/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.applications.unmanagedamlauncher;

import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.RMUtilities;
import io.hops.metadata.util.YarnAPIStorageFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
//TODO find a good place to put this test

public class TestUnmanagedAMLauncherWhenSchedulerFailover {

  private static final Log LOG = LogFactory.getLog(
          TestUnmanagedAMLauncherWhenSchedulerFailover.class);

  protected MiniYARNCluster yarnCluster = null;
  protected static YarnConfiguration conf = new YarnConfiguration();
  private boolean flag = false;
  private AtomicInteger success = new AtomicInteger(0);

  @Before
  public void setup() throws Exception {
    LOG.info("Starting up YARN cluster");
    URL url = Thread.currentThread().getContextClassLoader()
            .getResource("yarn-site.xml");
    if (url == null) {
      throw new RuntimeException(
              "Could not find 'yarn-site.xml' dummy file in classpath");
    }
    File f = new File(url.getPath());
    f.delete();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
            ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
    conf.setBoolean(YarnConfiguration.RECOVERY_ENABLED, true);
    conf
            .set(YarnConfiguration.RM_STORE, NDBRMStateStore.class.getName());
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC,
            true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 1000);
    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_MAX_MS, 1000);
    YarnAPIStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    RMUtilities.InitializeDB();
    conf.set("yarn.log.dir", "target");
    if (yarnCluster == null) {
      setRMConfiguration("rm0", conf, 0);
      setRMConfiguration("rm1", conf, 1);

      yarnCluster = new MiniYARNCluster(
              TestUnmanagedAMLauncherWhenSchedulerFailover.class.
              getSimpleName(), 2, 1,
              1, 1);
      yarnCluster.init(conf);
      yarnCluster.start();
      NodeManager nm = yarnCluster.getNodeManager(0);
      waitForNMToRegister(nm);

      Configuration yarnClusterConfig = yarnCluster.getConfig();
      yarnClusterConfig.set("yarn.application.classpath",
              new File(url.getPath()).getParent());
      //write the document to a buffer (not directly to the file, as that
      //can cause the file being written to get read -which will then fail.
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      yarnClusterConfig.writeXml(bytesOut);
      bytesOut.close();
      //write the bytes to the file in the classpath
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      os.write(bytesOut.toByteArray());
      os.close();
    }
    FileContext fsContext = FileContext.getLocalFSFileContext();
    fsContext.delete(
            new Path(conf.get(
                            "yarn.timeline-service.leveldb-timeline-store.path")),
            true);
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @After
  public void tearDown() {
    yarnCluster.stop();
  }

  @Test(timeout = 100000)
  public void testSchedulerFailover() throws Exception {

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          startScripts();
        } catch (Exception ex) {
          LOG.error(ex, ex);
          Assert.fail(ex.toString());
        }
      }
    });
    t.start();

    Thread.sleep(20000);

    LOG.info("stopping leading RM");
    yarnCluster.getResourceManager().stop();

    t.join();
    Assert.assertFalse("some script took too long to run", flag);
    Assert.assertEquals("the number of script that finished is incorect "
            + success.get(), 30, success.get());
  }

  public void startScripts() throws InterruptedException {
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < 5; i++) {
      final int id = i;
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            runScript(id);
          } catch (Exception ex) {
            LOG.error(ex, ex);
            Assert.fail(ex.toString());
          }
        }
      });
      threads.add(t);
      t.start();
      Thread.sleep(100);
    }
    for (Thread t : threads) {
      t.join();
    }
  }

  public void runScript(int id) throws Exception {
    String classpath = getTestRuntimeClasspath();
    String javaHome = System.getenv("JAVA_HOME");
    if (javaHome == null) {
      LOG.fatal("JAVA_HOME not defined. Test not running.");
      return;
    }
    String[] args = {"--classpath", classpath, "--queue", "default", "--cmd",
      javaHome + "/bin/java -Xmx512m "
      + TestUnmanagedAMLauncherWhenSchedulerFailover.class.
      getCanonicalName() + " success " + id};

    LOG.info("Initializing Launcher");
    UnmanagedAMLauncher launcher = new UnmanagedAMLauncher(new Configuration(
            yarnCluster.getConfig())) {
              public void launchAM(ApplicationAttemptId attemptId)
              throws IOException, YarnException {
                YarnApplicationAttemptState attemptState = rmClient.
                getApplicationAttemptReport(attemptId)
                .getYarnApplicationAttemptState();
                Assert.assertTrue(
                        attemptState.
                        equals(YarnApplicationAttemptState.LAUNCHED));
                super.launchAM(attemptId);
              }
            };
    boolean initSuccess = launcher.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running Launcher " + id);
    long starttime = System.currentTimeMillis();
    boolean result = launcher.run();
    long totalTime = System.currentTimeMillis() - starttime;
    LOG.info("Launcher run completed " + id + " Result=" + result + " "
            + totalTime);
    Assert.assertTrue(result);
    success.incrementAndGet();
    if (id < 25) {
      runScript(id + 5);
    }
  }

  private static String getTestRuntimeClasspath() {
    LOG.info(
            "Trying to generate classpath for app master from current thread's classpath");
    String envClassPath = "";
    String cp = System.getProperty("java.class.path");
    if (cp != null) {
      envClassPath += cp.trim() + File.pathSeparator;
    }
    // yarn-site.xml at this location contains proper config for mini cluster
    ClassLoader thisClassLoader = Thread.currentThread().getContextClassLoader();
    URL url = thisClassLoader.getResource("yarn-site.xml");
    envClassPath += new File(url.getFile()).getParent();
    return envClassPath;
  }

  protected static void waitForNMToRegister(NodeManager nm) throws Exception {
    int attempt = 60;
    ContainerManagerImpl cm = ((ContainerManagerImpl) nm.getNMContext().
            getContainerManager());
    while (cm.getBlockNewContainerRequestsStatus() && attempt-- > 0) {
      Thread.sleep(2000);
    }
  }

  private void verifyContainerLog() {
    File logFolder = new File(yarnCluster.getNodeManager(0).getConfig()
            .get(YarnConfiguration.NM_LOG_DIRS,
                    YarnConfiguration.DEFAULT_NM_LOG_DIRS));

    File[] listOfFiles = logFolder.listFiles();
    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].listFiles().length > 2) {
        Assert.fail("too many containers for application " + listOfFiles[i].
                getAbsolutePath());
        break;
      }
    }

  }

  private void setRMConfiguration(final String rmId, Configuration conf,
          int index) {
    String hostname = MiniYARNCluster.getHostname();
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      if (confKey.equals(YarnConfiguration.RM_GROUP_MEMBERSHIP_ADDRESS)) {
        int port = YarnConfiguration.DEFAULT_RM_GROUP_MEMBERSHIP_PORT + index
                * 1000;
        conf.set(HAUtil.addSuffix(confKey, rmId), hostname + ":" + port);
      } else if (confKey.equals(YarnConfiguration.RM_ADDRESS)) {
        int port = YarnConfiguration.DEFAULT_RM_PORT + index * 1000;
        conf.set(HAUtil.addSuffix(confKey, rmId), hostname + ":" + port);
      } else if (confKey.equals(YarnConfiguration.RM_ADMIN_ADDRESS)) {
        int port = YarnConfiguration.DEFAULT_RM_ADMIN_PORT + index * 1000;
        conf.set(HAUtil.addSuffix(confKey, rmId), hostname + ":" + port);
      } else if (confKey.equals(YarnConfiguration.RM_SCHEDULER_ADDRESS)) {
        int port = YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT + index * 1000;
        conf.set(HAUtil.addSuffix(confKey, rmId), hostname + ":" + port);
      } else if (confKey.equals(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS)) {
        int port = YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT + index
                * 1000;
        conf.set(HAUtil.addSuffix(confKey, rmId), hostname + ":" + port);
      } else {
        conf.set(HAUtil.addSuffix(confKey, rmId), hostname + ":0");
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int id = Integer.parseInt(args[1]);
    if (args[0].equals("success")) {
      ApplicationMasterProtocol client = ClientRMProxy.createRMProxy(conf,
              ApplicationMasterProtocol.class);
      boolean registered=false;
      while (!registered) {
        try {
          RegisterApplicationMasterResponse registerResponse = client.
                  registerApplicationMaster(RegisterApplicationMasterRequest
                          .newInstance(NetUtils.getHostname(), -1, ""));
          registered = true;
        } catch (IOException ex) {
          LOG.error(ex, ex);
        }
        Thread.sleep(1000);
      }
      ArrayList<ResourceRequest> resourceRequest
              = new ArrayList<ResourceRequest>();
      resourceRequest.add(ResourceRequest.newInstance(Priority.newInstance(0),
              ResourceRequest.ANY, Resource.newInstance(128, 1), 1
      ));
      AllocateRequest request = AllocateRequest
              .newInstance(0, 50f, resourceRequest,
                      new ArrayList<ContainerId>(), ResourceBlacklistRequest
                      .newInstance(new ArrayList<String>(),
                              new ArrayList<String>()));
      LOG.info(id + " sending container request");
      AllocateResponse response = client.allocate(request);

      List<Container> containers = response.getAllocatedContainers();
      while (containers.size() < 1) {
        Thread.sleep(1000);

        request = AllocateRequest
                .newInstance(response.getResponseId(), 50f,
                        new ArrayList<ResourceRequest>(),
                        new ArrayList<ContainerId>(), ResourceBlacklistRequest
                        .newInstance(new ArrayList<String>(),
                                new ArrayList<String>()));
        LOG.info(id + " sending container request");
        response = client.allocate(request);
        containers = response.getAllocatedContainers();
        LOG.info(id + " AM got " + containers.size() + " containers");
      }

      Thread.sleep(1000);

      ArrayList<ContainerId> containersToFree = new ArrayList<ContainerId>();
      for (Container c : containers) {
        containersToFree.add(c.getId());
      }
      request = AllocateRequest
              .newInstance(response.getResponseId(), 100f,
                      new ArrayList<ResourceRequest>(),
                      containersToFree, ResourceBlacklistRequest
                      .newInstance(new ArrayList<String>(),
                              new ArrayList<String>()));
      response = client.allocate(request);

      Thread.sleep(1000);
      int nbTry = 0;
      FinishApplicationMasterResponse resp;
      while (true) {
        try {
          LOG.info(id + " finish applicationMaster");
          resp = client.finishApplicationMaster(
                  FinishApplicationMasterRequest
                  .newInstance(FinalApplicationStatus.SUCCEEDED, "success", null));
          break;
        } catch (IOException ex) {
          LOG.warn(id + " exception while finishing application", ex);
          nbTry++;
          if (nbTry > 10) {
            LOG.error(id + " retried 10 time and still error");
            throw ex;
          }
        }
      }
      assertTrue(resp.getIsUnregistered());
      LOG.info(id + " exiting 0");
      System.exit(0);
    } else {
      LOG.info(id + " exiting 1");
      System.exit(1);
    }
  }
}
