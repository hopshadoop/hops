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

package org.apache.hadoop.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

/*
 * Test that the container resource increase rpc times out properly.
 * This is used by AM to increase container resource.
 */
public class TestContainerResourceIncreaseRPC {

  static final Log LOG = LogFactory.getLog(
      TestContainerResourceIncreaseRPC.class);

  @Test
  public void testHadoopProtoRPCTimeout() throws Exception {
    testRPCTimeout(HadoopYarnProtoRPC.class.getName());
  }

  private void testRPCTimeout(String rpcClass) throws Exception {
    Configuration conf = new Configuration();
    // set timeout low for the test
    conf.setInt("yarn.rpc.nm-command-timeout", 3000);
    conf.set(YarnConfiguration.IPC_RPC_IMPL, rpcClass);
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    Server server = rpc.getServer(ContainerManagementProtocol.class,
        new DummyContainerManager(), addr, conf, null, 1);
    server.start();
    try {
      ContainerManagementProtocol proxy =
          (ContainerManagementProtocol) rpc.getProxy(
              ContainerManagementProtocol.class,
                  server.getListenerAddress(), conf);
      ApplicationId applicationId = ApplicationId.newInstance(0, 0);
      ApplicationAttemptId applicationAttemptId =
          ApplicationAttemptId.newInstance(applicationId, 0);
      ContainerId containerId =
          ContainerId.newContainerId(applicationAttemptId, 100);
      NodeId nodeId = NodeId.newInstance("localhost", 1234);
      Resource resource = Resource.newInstance(1234, 2);
      ContainerTokenIdentifier containerTokenIdentifier =
          new ContainerTokenIdentifier(containerId, "localhost", "user",
              resource, System.currentTimeMillis() + 10000, 42, 42,
                  Priority.newInstance(0), 0);
      Token containerToken =
          newContainerToken(nodeId, "password".getBytes(),
              containerTokenIdentifier);
      // Construct container resource increase request,
      List<Token> increaseTokens = new ArrayList<>();
      increaseTokens.add(containerToken);
      ContainerUpdateRequest request = ContainerUpdateRequest
          .newInstance(increaseTokens);

      try {
        proxy.updateContainer(request);
      } catch (Exception e) {
        LOG.info(StringUtils.stringifyException(e));
        Assert.assertEquals("Error, exception is not: "
            + SocketTimeoutException.class.getName(),
            SocketTimeoutException.class.getName(), e.getClass().getName());
        return;
      }
    } finally {
      server.stop();
    }
    Assert.fail("timeout exception should have occurred!");
  }

  public static Token newContainerToken(NodeId nodeId, byte[] password,
      ContainerTokenIdentifier tokenIdentifier) {
    // RPC layer client expects ip:port as service for tokens
    InetSocketAddress addr =
        NetUtils.createSocketAddrForHost(nodeId.getHost(), nodeId.getPort());
    // NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
    Token containerToken =
        Token.newInstance(tokenIdentifier.getBytes(),
          ContainerTokenIdentifier.KIND.toString(), password, SecurityUtil
            .buildTokenService(addr).toString());
    return containerToken;
  }

  public class DummyContainerManager implements ContainerManagementProtocol {

    @Override
    public StartContainersResponse startContainers(
        StartContainersRequest requests) throws YarnException, IOException {
      Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw new YarnException(e);
    }

    @Override
    public StopContainersResponse
    stopContainers(StopContainersRequest requests) throws YarnException,
        IOException {
      Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw new YarnException(e);
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
        GetContainerStatusesRequest request) throws YarnException, IOException {
      Exception e = new Exception("Dummy function", new Exception(
          "Dummy function cause"));
      throw new YarnException(e);
    }

    @Override
    @Deprecated
    public IncreaseContainersResourceResponse increaseContainersResource(
        IncreaseContainersResourceRequest request)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public ContainerUpdateResponse updateContainer(ContainerUpdateRequest
        request) throws YarnException, IOException {
      try {
        // make the thread sleep to look like its not going to respond
        Thread.sleep(10000);
      } catch (Exception e) {
        LOG.error(e);
        throw new YarnException(e);
      }
      throw new YarnException("Shouldn't happen!!");
    }

    @Override
    public SignalContainerResponse signalToContainer(
        SignalContainerRequest request) throws YarnException, IOException {
      return null;
    }

    @Override public ResourceLocalizationResponse localize(
        ResourceLocalizationRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ReInitializeContainerResponse reInitializeContainer(
        ReInitializeContainerRequest request) throws YarnException,
        IOException {
      return null;
    }

    @Override
    public RestartContainerResponse restartContainer(ContainerId containerId)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public RollbackResponse rollbackLastReInitialization(
        ContainerId containerId) throws YarnException, IOException {
      return null;
    }

    @Override
    public CommitResponse commitLastReInitialization(ContainerId containerId)
        throws YarnException, IOException {
      return null;
    }
  }
}
