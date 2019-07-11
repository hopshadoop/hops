/**
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
package org.apache.hadoop.yarn.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.HopsSSLTestUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.SSLException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class TestYarnSSLServer extends HopsSSLTestUtils {
    private final Log LOG = LogFactory.getLog(TestYarnSSLServer.class);

    private MiniYARNCluster cluster;
    private ApplicationClientProtocol acClient, acClient1;
    private static String classpathDir;

    public TestYarnSSLServer(CERT_ERR error_mode) {
        super.error_mode = error_mode;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        classpathDir = KeyStoreTestUtil.getClasspathDir(TestYarnSSLServer.class);
    }
    
    @Before
    public void setUp() throws Exception {
        LOG.debug("Error mode: " + error_mode.name());

        conf = new YarnConfiguration();
        conf.set(YarnConfiguration.HOPS_RM_SECURITY_ACTOR_KEY,
            "org.apache.hadoop.yarn.server.resourcemanager.security.TestingRMAppSecurityActions");
        filesToPurge = prepareCryptoMaterial(conf, classpathDir);
        setCryptoConfig(conf, classpathDir);

        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
        conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);

        cluster = new MiniYARNCluster(TestYarnSSLServer.class.getName(), 1,
            3, 1, 1, false, true);
        cluster.init(conf);
        cluster.start();

        LOG.info("Started cluster");
        acClient = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    }

    @After
    public void tearDown() throws Exception {
        if (invoker != null) {
            invoker.join();
            invoker = null;
        }
        if (cluster != null) {
            LOG.info("Stopping MiniYARN cluster");
            cluster.stop();
        }

        if (acClient != null) {
            RPC.stopProxy(acClient);
        }

        if (acClient1 != null) {
            RPC.stopProxy(acClient1);
        }
    }

    @Test
    public void testSubmitApplication() throws Exception {
        GetNewApplicationRequest newAppReq = GetNewApplicationRequest
            .newInstance();
        GetNewApplicationResponse newAppRes = acClient.getNewApplication
            (newAppReq);
        ApplicationSubmissionContext appCtx = Records.newRecord
            (ApplicationSubmissionContext.class);
        //appCtx.setApplicationId(ApplicationId.newInstance(0L, 1));
        appCtx.setApplicationId(newAppRes.getApplicationId());
        appCtx.setApplicationName("RandomApplication");
        appCtx.setApplicationType("SomeType");
        
        Map<String, LocalResource> localResources = new HashMap<>();
        LocalResource lr = LocalResource.newInstance(URL.newInstance
                ("hdfs://", "localhost", 8020, "aFile"),
            LocalResourceType.FILE, LocalResourceVisibility.PUBLIC, 100L, 100L);
        localResources.put("aFile", lr);
        
        Map<String, String> env = new HashMap<>();
        env.put("env0", "someValue");
        
        List<String> amCommnads = new ArrayList<>();
        amCommnads.add("someRandom --command");
    
        ContainerLaunchContext amCtx = ContainerLaunchContext.newInstance
            (localResources, env, amCommnads, null, null, null);
        
        appCtx.setAMContainerSpec(amCtx);
        appCtx.setResource(Resource.newInstance(2048, 2));
        appCtx.setQueue("default");
    
        ApplicationClientProtocol client = ClientRMProxy.createRMProxy(conf,
            ApplicationClientProtocol.class);
        
        Thread invoker = new Thread(new Invoker(client));
        invoker.setName("AnotherClient");
        invoker.start();
        SubmitApplicationRequest appReq = SubmitApplicationRequest
            .newInstance(appCtx);
        LOG.debug("Submitting the application");
        acClient.submitApplication(appReq);
        LOG.debug("Submitted the application");
        
        LOG.debug("Getting new application");
        newAppRes = acClient.getNewApplication(newAppReq);
        assertNotNull(newAppRes);
        LOG.debug("I have gotten the new application");
        
        List<ApplicationReport> appsReport = acClient.getApplications(
            GetApplicationsRequest.newInstance()).getApplicationList();
        boolean found = false;
        
        for (ApplicationReport appRep : appsReport) {
            if (appRep.getApplicationId().equals(appCtx.getApplicationId())) {
                found = true;
                break;
            }
        }
        
        assertTrue(found);
        TimeUnit.SECONDS.sleep(10);
    }
    
    @Test(timeout = 3000)
    public void testRpcCall() throws Exception {
        EnumSet<NodeState> filter = EnumSet.of(NodeState.RUNNING);
        GetClusterNodesRequest req = GetClusterNodesRequest.newInstance();
        req.setNodeStates(filter);
        LOG.debug("Sending request");
        GetClusterNodesResponse res = acClient.getClusterNodes(req);
        LOG.debug("Got response from server");
        assertNotNull("Response should not be null", res);
        List<NodeReport> reports = res.getNodeReports();
        LOG.debug("Printing cluster nodes report");
        for (NodeReport report : reports) {
            LOG.debug("NodeId: " + report.getNodeId().toString());
        }
    }

    @Test
    public void testRpcCallWithNonValidCert() throws Exception {
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), err_clientKeyStore.toString());
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), err_clientTrustStore.toString());
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), passwd);

        // Exception will be thrown later. JUnit does not execute the code
        // after the exception, so make the call in a separate thread
        invoker = new Thread(new Invoker(acClient));
        invoker.start();

        LOG.debug("Creating the second client");
        acClient1 = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);

        GetNewApplicationRequest req1 = GetNewApplicationRequest.newInstance();
        if (error_mode.equals(CERT_ERR.NO_CA)) {
            rule.expect(SSLException.class);
        } else if (error_mode.equals(CERT_ERR.ERR_CN)) {
            rule.expect(RpcServerException.class);
        }
        GetNewApplicationResponse res1 = acClient1.getNewApplication(req1);
    }

    private class Invoker implements Runnable {
        private final ApplicationClientProtocol client;

        public Invoker(ApplicationClientProtocol client) {
            this.client = client;
        }

        @Override
        public void run() {
            EnumSet<NodeState> filter = EnumSet.of(NodeState.RUNNING);
            GetClusterNodesRequest req = GetClusterNodesRequest.newInstance();
            req.setNodeStates(filter);
            LOG.debug("Sending cluster nodes request from first client");
            try {
                TimeUnit.SECONDS.sleep(1);
                GetClusterNodesResponse res = client.getClusterNodes(req);
                assertNotNull("Response from the first client should not be null", res);
                LOG.debug("NodeReports: " + res.getNodeReports().size());
                for (NodeReport nodeReport : res.getNodeReports()) {
                    LOG.debug("Node: " + nodeReport.getNodeId() + " Capability: " + nodeReport.getCapability());
                }
            } catch (Exception ex) {
                LOG.error(ex, ex);
            }
        }
    }
}
