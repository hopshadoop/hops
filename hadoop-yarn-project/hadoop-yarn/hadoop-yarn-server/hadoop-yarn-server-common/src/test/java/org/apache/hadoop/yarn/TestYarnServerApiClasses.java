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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.SerializedExceptionPBImpl;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdatedCryptoForApp;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.api.records.impl.pb.NodeStatusPBImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Simple test classes from org.apache.hadoop.yarn.server.api
 */
public class TestYarnServerApiClasses {

  private final static org.apache.hadoop.yarn.factories.RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  /**
   * Test RegisterNodeManagerResponsePBImpl. Test getters and setters. The
   * RegisterNodeManagerResponsePBImpl should generate a prototype and data
   * restore from prototype
   */
  @Test
  public void testRegisterNodeManagerResponsePBImpl() {
    RegisterNodeManagerResponsePBImpl original =
        new RegisterNodeManagerResponsePBImpl();
    original.setContainerTokenMasterKey(getMasterKey());
    original.setNMTokenMasterKey(getMasterKey());
    original.setNodeAction(NodeAction.NORMAL);
    original.setDiagnosticsMessage("testDiagnosticMessage");

    RegisterNodeManagerResponsePBImpl copy =
        new RegisterNodeManagerResponsePBImpl(
            original.getProto());
    assertEquals(1, copy.getContainerTokenMasterKey().getKeyId());
    assertEquals(1, copy.getNMTokenMasterKey().getKeyId());
    assertEquals(NodeAction.NORMAL, copy.getNodeAction());
    assertEquals("testDiagnosticMessage", copy.getDiagnosticsMessage());
    assertFalse(copy.getAreNodeLabelsAcceptedByRM());
  }

  @Test
  public void testRegisterNodeManagerResponsePBImplWithRMAcceptLbls() {
    RegisterNodeManagerResponsePBImpl original =
        new RegisterNodeManagerResponsePBImpl();
    original.setAreNodeLabelsAcceptedByRM(true);
    RegisterNodeManagerResponsePBImpl copy =
        new RegisterNodeManagerResponsePBImpl(original.getProto());
    assertTrue(copy.getAreNodeLabelsAcceptedByRM());
  }

  /**
   * Test NodeHeartbeatRequestPBImpl.
   */
  @Test
  public void testNodeHeartbeatRequestPBImpl() {
    NodeHeartbeatRequestPBImpl original = new NodeHeartbeatRequestPBImpl();
    original.setLastKnownContainerTokenMasterKey(getMasterKey());
    original.setLastKnownNMTokenMasterKey(getMasterKey());
    original.setNodeStatus(getNodeStatus());
    original.setNodeLabels(getValidNodeLabels());
    NodeHeartbeatRequestPBImpl copy = new NodeHeartbeatRequestPBImpl(
        original.getProto());
    assertEquals(1, copy.getLastKnownContainerTokenMasterKey().getKeyId());
    assertEquals(1, copy.getLastKnownNMTokenMasterKey().getKeyId());
    assertEquals("localhost", copy.getNodeStatus().getNodeId().getHost());
    // check labels are coming with valid values
    Assert.assertTrue(original.getNodeLabels()
        .containsAll(copy.getNodeLabels()));
    // check for empty labels
    original.setNodeLabels(new HashSet<NodeLabel> ());
    copy = new NodeHeartbeatRequestPBImpl(
        original.getProto());
    Assert.assertNotNull(copy.getNodeLabels());
    Assert.assertEquals(0, copy.getNodeLabels().size());
  }

  /**
   * Test NodeHeartbeatRequestPBImpl.
   */
  @Test
  public void testNodeHeartbeatRequestPBImplWithNullLabels() {
    NodeHeartbeatRequestPBImpl original = new NodeHeartbeatRequestPBImpl();
    NodeHeartbeatRequestPBImpl copy =
        new NodeHeartbeatRequestPBImpl(original.getProto());
    Assert.assertNull(copy.getNodeLabels());
  }
  
  @Test
  public void testNodeHeartbeatRequestPBImplWithUpdatedCryptoForApps() {
    NodeHeartbeatRequestPBImpl original = new NodeHeartbeatRequestPBImpl();
    Set<ApplicationId> updatedApps = new HashSet<>();
    long now = System.currentTimeMillis();
    ApplicationId appId1 = ApplicationId.newInstance(now, 1);
    ApplicationId appId2 = ApplicationId.newInstance(now, 2);
    updatedApps.add(appId1);
    updatedApps.add(appId2);
    original.setUpdatedApplicationsWithNewCryptoMaterial(updatedApps);
    
    NodeHeartbeatRequestPBImpl copy = new NodeHeartbeatRequestPBImpl(original.getProto());
    Set<ApplicationId> copyUpdatedApps = copy.getUpdatedApplicationsWithNewCryptoMaterial();
    assertNotNull(copyUpdatedApps);
    assertEquals(2, copyUpdatedApps.size());
    assertTrue(copyUpdatedApps.contains(appId1));
    assertTrue(copyUpdatedApps.contains(appId2));
  }
  
  /**
   * Test NodeHeartbeatResponsePBImpl.
   */

  @Test
  public void testNodeHeartbeatResponsePBImpl() {
    NodeHeartbeatResponsePBImpl original = new NodeHeartbeatResponsePBImpl();

    original.setDiagnosticsMessage("testDiagnosticMessage");
    original.setContainerTokenMasterKey(getMasterKey());
    original.setNMTokenMasterKey(getMasterKey());
    original.setNextHeartBeatInterval(1000);
    original.setNodeAction(NodeAction.NORMAL);
    original.setResponseId(100);

    NodeHeartbeatResponsePBImpl copy = new NodeHeartbeatResponsePBImpl(
        original.getProto());
    assertEquals(100, copy.getResponseId());
    assertEquals(NodeAction.NORMAL, copy.getNodeAction());
    assertEquals(1000, copy.getNextHeartBeatInterval());
    assertEquals(1, copy.getContainerTokenMasterKey().getKeyId());
    assertEquals(1, copy.getNMTokenMasterKey().getKeyId());
    assertEquals("testDiagnosticMessage", copy.getDiagnosticsMessage());
    assertEquals(false, copy.getAreNodeLabelsAcceptedByRM());
   }

  @Test
  public void testNodeHeartbeatResponsePBImplWithRMAcceptLbls() {
    NodeHeartbeatResponsePBImpl original = new NodeHeartbeatResponsePBImpl();
    original.setAreNodeLabelsAcceptedByRM(true);
    NodeHeartbeatResponsePBImpl copy =
        new NodeHeartbeatResponsePBImpl(original.getProto());
    assertTrue(copy.getAreNodeLabelsAcceptedByRM());
  }

  @Test
  public void testNodeHeartbeatResponsePBImplWithDecreasedContainers() {
    NodeHeartbeatResponsePBImpl original = new NodeHeartbeatResponsePBImpl();
    original.addAllContainersToDecrease(
        Arrays.asList(getDecreasedContainer(1, 2, 2048, 2),
            getDecreasedContainer(2, 3, 1024, 1)));
    NodeHeartbeatResponsePBImpl copy =
        new NodeHeartbeatResponsePBImpl(original.getProto());
    assertEquals(1, copy.getContainersToDecrease().get(0)
        .getId().getContainerId());
    assertEquals(1024, copy.getContainersToDecrease().get(1)
        .getResource().getMemorySize());
  }
  
  @Test
  public void testNodeHeartbeatResponseWithUpdateCryptoForApps() {
    NodeHeartbeatResponsePBImpl original = new NodeHeartbeatResponsePBImpl();
    Map<ApplicationId, UpdatedCryptoForApp> updatedCryptoForApps = new HashMap<>();
    
    long now = System.currentTimeMillis();
    ApplicationId appId1 = ApplicationId.newInstance(now, 1);
    UpdatedCryptoForApp crypto1 = recordFactory.newRecordInstance(UpdatedCryptoForApp.class);
    ByteBuffer keyStore1 = ByteBuffer.wrap("keyStore1".getBytes());
    char[] keyStorePassword1 = "keyStorePassword1".toCharArray();
    ByteBuffer trustStore1 = ByteBuffer.wrap("trustStore1".getBytes());
    char[] trustStorePassword1 = "trustStorePassword1".toCharArray();
    crypto1.setKeyStore(keyStore1);
    crypto1.setKeyStorePassword(keyStorePassword1);
    crypto1.setTrustStore(trustStore1);
    crypto1.setTrustStorePassword(trustStorePassword1);
    crypto1.setVersion(1);
    updatedCryptoForApps.put(appId1, crypto1);
    
    ApplicationId appId2 = ApplicationId.newInstance(now, 2);
    UpdatedCryptoForApp crypto2 = recordFactory.newRecordInstance(UpdatedCryptoForApp.class);
    ByteBuffer keyStore2 = ByteBuffer.wrap("keyStore2".getBytes());
    char[] keyStorePassword2 = "keyStorePassword2".toCharArray();
    ByteBuffer trustStore2 = ByteBuffer.wrap("trustStore2".getBytes());
    char[] trustStorePassword2 = "trustStorePassword2".toCharArray();
    crypto2.setKeyStore(keyStore2);
    crypto2.setKeyStorePassword(keyStorePassword2);
    crypto2.setTrustStore(trustStore2);
    crypto2.setTrustStorePassword(trustStorePassword2);
    crypto2.setVersion(1);
    updatedCryptoForApps.put(appId2, crypto2);
    
    original.setUpdatedCryptoForApps(updatedCryptoForApps);
    
    NodeHeartbeatResponsePBImpl copy = new NodeHeartbeatResponsePBImpl(original.getProto());
    Map<ApplicationId, UpdatedCryptoForApp> copyCrypto = copy.getUpdatedCryptoForApps();
    assertNotNull(copyCrypto);
    assertEquals(2, copyCrypto.size());
    
    UpdatedCryptoForApp copyAppCrypto1 = copyCrypto.get(appId1);
    assertNotNull(copyAppCrypto1);
    assertTrue(keyStore1.equals(copyAppCrypto1.getKeyStore()));
    assertArrayEquals(keyStorePassword1, copyAppCrypto1.getKeyStorePassword());
    assertTrue(trustStore1.equals(copyAppCrypto1.getTrustStore()));
    assertArrayEquals(trustStorePassword1, copyAppCrypto1.getTrustStorePassword());
    assertEquals(1, copyAppCrypto1.getVersion());
    
    UpdatedCryptoForApp copyAppCrypto2 = copyCrypto.get(appId2);
    assertNotNull(copyAppCrypto2);
    assertTrue(keyStore2.equals(copyAppCrypto2.getKeyStore()));
    assertArrayEquals(keyStorePassword2, copyAppCrypto2.getKeyStorePassword());
    assertTrue(trustStore2.equals(copyAppCrypto2.getTrustStore()));
    assertArrayEquals(trustStorePassword2, copyAppCrypto2.getTrustStorePassword());
    assertEquals(1, copyAppCrypto2.getVersion());
    
    assertFalse(copyAppCrypto1.getKeyStore().equals(copyAppCrypto2.getKeyStore()));
    assertFalse(copyAppCrypto1.getTrustStore().equals(copyAppCrypto2.getTrustStore()));
  }
  
  /**
   * Test RegisterNodeManagerRequestPBImpl.
   */

  @Test
  public void testRegisterNodeManagerRequestPBImpl() {
    RegisterNodeManagerRequestPBImpl original = new RegisterNodeManagerRequestPBImpl();
    original.setHttpPort(8080);
    original.setNodeId(getNodeId());
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    resource.setMemorySize(10000);
    resource.setVirtualCores(2);
    resource.setGPUs(3);
    original.setResource(resource);
    RegisterNodeManagerRequestPBImpl copy = new RegisterNodeManagerRequestPBImpl(
        original.getProto());

    assertEquals(8080, copy.getHttpPort());
    assertEquals(9090, copy.getNodeId().getPort());
    assertEquals(10000, copy.getResource().getMemorySize());
    assertEquals(2, copy.getResource().getVirtualCores());
    assertEquals(3, copy.getResource().getGPUs());

  }

  /**
   * Test MasterKeyPBImpl.
   */

  @Test
  public void testMasterKeyPBImpl() {
    MasterKeyPBImpl original = new MasterKeyPBImpl();
    original.setBytes(ByteBuffer.allocate(0));
    original.setKeyId(1);

    MasterKeyPBImpl copy = new MasterKeyPBImpl(original.getProto());
    assertEquals(1, copy.getKeyId());
    assertTrue(original.equals(copy));
    assertEquals(original.hashCode(), copy.hashCode());

  }

  /**
   * Test SerializedExceptionPBImpl.
   */
  @Test
  public void testSerializedExceptionPBImpl() {
    SerializedExceptionPBImpl original = new SerializedExceptionPBImpl();
    original.init("testMessage");
    SerializedExceptionPBImpl copy = new SerializedExceptionPBImpl(
        original.getProto());
    assertEquals("testMessage", copy.getMessage());

    original = new SerializedExceptionPBImpl();
    original.init("testMessage", new Throwable(new Throwable("parent")));
    copy = new SerializedExceptionPBImpl(original.getProto());
    assertEquals("testMessage", copy.getMessage());
    assertEquals("parent", copy.getCause().getMessage());
    assertTrue( copy.getRemoteTrace().startsWith(
        "java.lang.Throwable: java.lang.Throwable: parent"));

  }

  /**
   * Test NodeStatusPBImpl.
   */

  @Test
  public void testNodeStatusPBImpl() {
    NodeStatusPBImpl original = new NodeStatusPBImpl();

    original.setContainersStatuses(Arrays.asList(getContainerStatus(1, 2, 1),
        getContainerStatus(2, 3, 1)));
    original.setKeepAliveApplications(Arrays.asList(getApplicationId(3),
        getApplicationId(4)));
    original.setNodeHealthStatus(getNodeHealthStatus());
    original.setNodeId(getNodeId());
    original.setResponseId(1);
    original.setIncreasedContainers(
        Arrays.asList(getIncreasedContainer(1, 2, 2048, 2),
            getIncreasedContainer(2, 3, 4096, 3)));

    NodeStatusPBImpl copy = new NodeStatusPBImpl(original.getProto());
    assertEquals(3L, copy.getContainersStatuses().get(1).getContainerId()
        .getContainerId());
    assertEquals(3, copy.getKeepAliveApplications().get(0).getId());
    assertEquals(1000, copy.getNodeHealthStatus().getLastHealthReportTime());
    assertEquals(9090, copy.getNodeId().getPort());
    assertEquals(1, copy.getResponseId());
    assertEquals(1, copy.getIncreasedContainers().get(0)
        .getId().getContainerId());
    assertEquals(4096, copy.getIncreasedContainers().get(1)
        .getResource().getMemorySize());
  }

  @Test
  public void testRegisterNodeManagerRequestWithNullLabels() {
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(
            NodeId.newInstance("host", 1234), 1234, Resource.newInstance(0, 0),
            "version", null, null);

    // serialze to proto, and get request from proto
    RegisterNodeManagerRequest request1 =
        new RegisterNodeManagerRequestPBImpl(
            ((RegisterNodeManagerRequestPBImpl) request).getProto());

    // check labels are coming with no values
    Assert.assertNull(request1.getNodeLabels());
  }

  @Test
  public void testRegisterNodeManagerRequestWithValidLabels() {
    HashSet<NodeLabel> nodeLabels = getValidNodeLabels();
    RegisterNodeManagerRequest request =
        RegisterNodeManagerRequest.newInstance(
            NodeId.newInstance("host", 1234), 1234, Resource.newInstance(0, 0),
            "version", null, null, nodeLabels);

    // serialze to proto, and get request from proto
    RegisterNodeManagerRequest copy =
        new RegisterNodeManagerRequestPBImpl(
            ((RegisterNodeManagerRequestPBImpl) request).getProto());

    // check labels are coming with valid values
    Assert.assertEquals(true, nodeLabels.containsAll(copy.getNodeLabels()));

    // check for empty labels
    request.setNodeLabels(new HashSet<NodeLabel> ());
    copy = new RegisterNodeManagerRequestPBImpl(
        ((RegisterNodeManagerRequestPBImpl) request).getProto());
    Assert.assertNotNull(copy.getNodeLabels());
    Assert.assertEquals(0, copy.getNodeLabels().size());
  }

  @Test
  public void testUnRegisterNodeManagerRequestPBImpl() throws Exception {
    UnRegisterNodeManagerRequestPBImpl request = new UnRegisterNodeManagerRequestPBImpl();
    NodeId nodeId = NodeId.newInstance("host", 1234);
    request.setNodeId(nodeId);

    UnRegisterNodeManagerRequestPBImpl copy = new UnRegisterNodeManagerRequestPBImpl(
        request.getProto());
    Assert.assertEquals(nodeId, copy.getNodeId());
  }

  private HashSet<NodeLabel> getValidNodeLabels() {
    HashSet<NodeLabel> nodeLabels = new HashSet<NodeLabel>();
    nodeLabels.add(NodeLabel.newInstance("java"));
    nodeLabels.add(NodeLabel.newInstance("windows"));
    nodeLabels.add(NodeLabel.newInstance("gpu"));
    nodeLabels.add(NodeLabel.newInstance("x86"));
    return nodeLabels;
  }

  private ContainerStatus getContainerStatus(int applicationId,
      int containerID, int appAttemptId) {
    ContainerStatus status = recordFactory
        .newRecordInstance(ContainerStatus.class);
    status.setContainerId(getContainerId(containerID, appAttemptId));
    return status;
  }

  private ApplicationAttemptId getApplicationAttemptId(int appAttemptId) {
    ApplicationAttemptId result = ApplicationAttemptIdPBImpl.newInstance(
        getApplicationId(appAttemptId), appAttemptId);
    return result;
  }

  private ContainerId getContainerId(int containerID, int appAttemptId) {
    ContainerId containerId = ContainerIdPBImpl.newContainerId(
        getApplicationAttemptId(appAttemptId), containerID);
    return containerId;
  }

  private ApplicationId getApplicationId(int applicationId) {
    ApplicationIdPBImpl appId = new ApplicationIdPBImpl() {
      public ApplicationIdPBImpl setParameters(int id, long timestamp) {
        setClusterTimestamp(timestamp);
        setId(id);
        build();
        return this;
      }
    }.setParameters(applicationId, 1000);
    return new ApplicationIdPBImpl(appId.getProto());
  }

  private Container getDecreasedContainer(int containerID,
      int appAttemptId, int memory, int vCores) {
    ContainerId containerId = getContainerId(containerID, appAttemptId);
    Resource capability = Resource.newInstance(memory, vCores);
    return Container.newInstance(
        containerId, null, null, capability, null, null);
  }

  private Container getIncreasedContainer(int containerID,
      int appAttemptId, int memory, int vCores) {
    ContainerId containerId = getContainerId(containerID, appAttemptId);
    Resource capability = Resource.newInstance(memory, vCores);
    return Container.newInstance(
        containerId, null, null, capability, null, null);
  }

  private NodeStatus getNodeStatus() {
    NodeStatus status = recordFactory.newRecordInstance(NodeStatus.class);
    status.setContainersStatuses(new ArrayList<ContainerStatus>());
    status.setKeepAliveApplications(new ArrayList<ApplicationId>());

    status.setNodeHealthStatus(getNodeHealthStatus());
    status.setNodeId(getNodeId());
    status.setResponseId(1);
    return status;
  }

  private NodeId getNodeId() {
    return NodeId.newInstance("localhost", 9090);
  }

  private NodeHealthStatus getNodeHealthStatus() {
    NodeHealthStatus healStatus = recordFactory
        .newRecordInstance(NodeHealthStatus.class);
    healStatus.setHealthReport("healthReport");
    healStatus.setIsNodeHealthy(true);
    healStatus.setLastHealthReportTime(1000);
    return healStatus;

  }

  private MasterKey getMasterKey() {
    MasterKey key = recordFactory.newRecordInstance(MasterKey.class);
    key.setBytes(ByteBuffer.allocate(0));
    key.setKeyId(1);
    return key;

  }
}
