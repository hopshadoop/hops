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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.common.base.Joiner;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import io.hops.ha.common.TransactionState;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.YarnAPIStorageFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRMWebServicesNodes extends JerseyTest {

  private static MockRM rm;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      try {
        bind(JAXBContextResolver.class);
        bind(RMWebServices.class);
        bind(GenericExceptionHandler.class);
        YarnConfiguration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, "4000");
        conf.setInt(YarnConfiguration.HOPS_BATCH_MAX_DURATION, 60000);
        conf.setInt(YarnConfiguration.HOPS_BATCH_MAX_SIZE, 10000);
        YarnAPIStorageFactory.setConfiguration(conf);
        RMStorageFactory.setConfiguration(conf);
        RMStorageFactory.getConnector().formatStorage();
        rm = new MockRM(conf);
        rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
        rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
        bind(ResourceManager.class).toInstance(rm);
        bind(RMContext.class).toInstance(rm.getRMContext());
        bind(ApplicationACLsManager.class)
            .toInstance(rm.getApplicationACLsManager());
        bind(QueueACLsManager.class).toInstance(rm.getQueueACLsManager());
        serve("/*").with(GuiceContainer.class);
      } catch (Exception ex) {
        Logger.getLogger(TestRMWebServicesNodes.class.getName())
            .log(Level.SEVERE, null, ex);
      }
    }
  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public TestRMWebServicesNodes() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test (timeout = 60000) 
  public void testNodes() throws JSONException, Exception {
    testNodesHelper("nodes", MediaType.APPLICATION_JSON);
  }

  @Test (timeout = 60000) 
  public void testNodesSlash() throws JSONException, Exception {
    testNodesHelper("nodes/", MediaType.APPLICATION_JSON);
  }

  @Test (timeout = 60000)
  public void testNodesDefault() throws JSONException, Exception {
    testNodesHelper("nodes/", "");
  }

  @Test (timeout = 60000)
  public void testNodesDefaultWithUnHealthyNode()
      throws JSONException, Exception {

    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), NodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), NodeState.NEW);

    MockNM nm3 = rm.registerNode("h3:1236", 5122);
    rm.NMwaitForState(nm3.getNodeId(), NodeState.NEW);
    rm.sendNodeStarted(nm3);
    rm.NMwaitForState(nm3.getNodeId(), NodeState.RUNNING);
    RMNodeImpl node =
        (RMNodeImpl) rm.getRMContext().getActiveRMNodes().get(nm3.getNodeId());
    NodeHealthStatus nodeHealth = NodeHealthStatus
        .newInstance(false, "test health report", System.currentTimeMillis());
    node.handle(new RMNodeStatusEvent(nm3.getNodeId(), nodeHealth,
        new ArrayList<ContainerStatus>(), null, null,
        new TransactionStateImpl( TransactionState.TransactionType.RM)));
    rm.NMwaitForState(nm3.getNodeId(), NodeState.UNHEALTHY);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    // 3 nodes, including the unhealthy node and the new node.
    assertEquals("incorrect number of elements", 3, nodeArray.length());
  }

  @Test (timeout = 60000)
  public void testNodesQueryNew() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), NodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), NodeState.NEW);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .queryParam("states", NodeState.NEW.toString())
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 1, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);

    verifyNodeInfo(info, nm2);
  }

  @Test (timeout = 60000)
  public void testNodesQueryStateNone() throws JSONException, Exception {
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .queryParam("states", NodeState.DECOMMISSIONED.toString())
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not null", JSONObject.NULL, json.get("nodes"));
  }

  @Test (timeout = 60000)
  public void testNodesQueryStateInvalid() throws JSONException, Exception {
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .queryParam("states", "BOGUSSTATE").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception querying invalid state");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());

      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringContains("exception message",
          "org.apache.hadoop.yarn.api.records.NodeState.BOGUSSTATE", message);
      WebServicesTestUtils
          .checkStringMatch("exception type", "IllegalArgumentException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "java.lang.IllegalArgumentException", classname);

    } finally {
      rm.stop();
    }
  }
  
  @Test (timeout = 60000)
  public void testNodesQueryStateLost() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1234", 5120);
    rm.sendNodeStarted(nm1);
    rm.sendNodeStarted(nm2);
    rm.NMwaitForState(nm1.getNodeId(), NodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), NodeState.RUNNING);
    rm.sendNodeLost(nm1);
    rm.sendNodeLost(nm2);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .queryParam("states", NodeState.LOST.toString())
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 2, nodeArray.length());
    for (int i = 0; i < nodeArray.length(); ++i) {
      JSONObject info = nodeArray.getJSONObject(i);
      String host = info.get("id").toString().split(":")[0];
      RMNode rmNode = rm.getRMContext().getInactiveRMNodes().get(host);
      WebServicesTestUtils.checkStringMatch("nodeHTTPAddress", "",
          info.getString("nodeHTTPAddress"));
      WebServicesTestUtils
          .checkStringMatch("state", rmNode.getState().toString(),
              info.getString("state"));
    }
  }
  
  @Test (timeout = 60000)
  public void testSingleNodeQueryStateLost() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1234", 5120);
    rm.sendNodeStarted(nm1);
    rm.sendNodeStarted(nm2);
    rm.NMwaitForState(nm1.getNodeId(), NodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), NodeState.RUNNING);
    rm.sendNodeLost(nm1);
    rm.sendNodeLost(nm2);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("h2:1234")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject info = json.getJSONObject("node");
    String id = info.get("id").toString();

    assertEquals("Incorrect Node Information.", "h2:1234", id);

    RMNode rmNode = rm.getRMContext().getInactiveRMNodes().get("h2");
    WebServicesTestUtils.checkStringMatch("nodeHTTPAddress", "",
        info.getString("nodeHTTPAddress"));
    WebServicesTestUtils.checkStringMatch("state", rmNode.getState().toString(),
        info.getString("state"));
  }

  @Test (timeout = 60000)
  public void testNodesQueryRunning() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), NodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), NodeState.NEW);
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .queryParam("states", "running").accept(MediaType.APPLICATION_JSON)
            .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 1, nodeArray.length());
  }

  @Test (timeout = 60000)
  public void testNodesQueryHealthyFalse() throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.NMwaitForState(nm1.getNodeId(), NodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), NodeState.NEW);
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .queryParam("states", "UNHEALTHY")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    assertEquals("nodes is not null", JSONObject.NULL, json.get("nodes"));
  }

  public void testNodesHelper(String path, String media)
      throws JSONException, Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    rm.sendNodeStarted(nm1);
    rm.sendNodeStarted(nm2);
    rm.NMwaitForState(nm1.getNodeId(), NodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), NodeState.RUNNING);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path(path).accept(media)
            .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 2, nodeArray.length());
    JSONObject info = nodeArray.getJSONObject(0);
    String id = info.get("id").toString();

    if (id.matches("h1:1234")) {
      verifyNodeInfo(info, nm1);
      verifyNodeInfo(nodeArray.getJSONObject(1), nm2);
    } else {
      verifyNodeInfo(info, nm2);
      verifyNodeInfo(nodeArray.getJSONObject(1), nm1);
    }
  }

  @Test (timeout = 60000)
  public void testSingleNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    testSingleNodeHelper("h2:1235", nm2, MediaType.APPLICATION_JSON);
  }

  @Test (timeout = 60000)
  public void testSingleNodeSlash() throws JSONException, Exception {
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    testSingleNodeHelper("h1:1234/", nm1, MediaType.APPLICATION_JSON);
  }

  @Test (timeout = 60000)
  public void testSingleNodeDefault() throws JSONException, Exception {
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    testSingleNodeHelper("h1:1234/", nm1, "");
  }

  public void testSingleNodeHelper(String nodeid, MockNM nm, String media)
      throws JSONException, Exception {
    WebResource r = resource();
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes").path(nodeid)
            .accept(media).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    JSONObject info = json.getJSONObject("node");
    verifyNodeInfo(info, nm);
  }

  @Test (timeout = 60000)
  public void testNonexistNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyNonexistNodeException(message, type, classname);

    } finally {
      rm.stop();
    }
  }

  // test that the exception output defaults to JSON
  @Test (timeout = 60000)
  public void testNonexistNodeDefault() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      verifyNonexistNodeException(message, type, classname);
    } finally {
      rm.stop();
    }
  }

  // test that the exception output works in XML
  @Test (timeout = 60000)
  public void testNonexistNodeXML() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid:99").accept(MediaType.APPLICATION_XML)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();
      assertEquals(Status.NOT_FOUND, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
      String msg = response.getEntity(String.class);
      System.out.println(msg);
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(msg));
      Document dom = db.parse(is);
      NodeList nodes = dom.getElementsByTagName("RemoteException");
      Element element = (Element) nodes.item(0);
      String message = WebServicesTestUtils.getXmlString(element, "message");
      String type = WebServicesTestUtils.getXmlString(element, "exception");
      String classname =
          WebServicesTestUtils.getXmlString(element, "javaClassName");
      verifyNonexistNodeException(message, type, classname);
    } finally {
      rm.stop();
    }
  }

  private void verifyNonexistNodeException(String message, String type,
      String classname) {
    assertTrue("exception message incorrect",
        "java.lang.Exception: nodeId, node_invalid:99, is not found"
            .matches(message));
    assertTrue("exception type incorrect", "NotFoundException".matches(type));
    assertTrue("exception className incorrect",
        "org.apache.hadoop.yarn.webapp.NotFoundException".matches(classname));
  }

  @Test (timeout = 60000)
  public void testInvalidNode() throws JSONException, Exception {
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);

    WebResource r = resource();
    try {
      r.path("ws").path("v1").path("cluster").path("nodes")
          .path("node_invalid_foo").accept(MediaType.APPLICATION_JSON)
          .get(JSONObject.class);

      fail("should have thrown exception on non-existent nodeid");
    } catch (UniformInterfaceException ue) {
      ClientResponse response = ue.getResponse();

      assertEquals(Status.BAD_REQUEST, response.getClientResponseStatus());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
      JSONObject msg = response.getEntity(JSONObject.class);
      JSONObject exception = msg.getJSONObject("RemoteException");
      assertEquals("incorrect number of elements", 3, exception.length());
      String message = exception.getString("message");
      String type = exception.getString("exception");
      String classname = exception.getString("javaClassName");
      WebServicesTestUtils.checkStringMatch("exception message",
          "Invalid NodeId \\[node_invalid_foo\\]. Expected host:port", message);
      WebServicesTestUtils
          .checkStringMatch("exception type", "IllegalArgumentException", type);
      WebServicesTestUtils.checkStringMatch("exception classname",
          "java.lang.IllegalArgumentException", classname);
    } finally {
      rm.stop();
    }
  }

  @Test (timeout = 60000)
  public void testNodesXML() throws JSONException, Exception {
    rm.start();
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    // MockNM nm2 = rm.registerNode("h2:1235", 5121);
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("nodes");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("node");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyNodesXML(nodes, nm1);
    rm.stop();
  }

  @Test (timeout = 60000)
  public void testSingleNodesXML() throws JSONException, Exception {
    rm.start();
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    // MockNM nm2 = rm.registerNode("h2:1235", 5121);
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("h1:1234")
            .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("node");
    assertEquals("incorrect number of elements", 1, nodes.getLength());
    verifyNodesXML(nodes, nm1);
    rm.stop();
  }

  @Test (timeout = 60000)
  public void testNodes2XML() throws JSONException, Exception {
    rm.start();
    WebResource r = resource();
    rm.registerNode("h1:1234", 5120);
    rm.registerNode("h2:1235", 5121);
    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_XML_TYPE, response.getType());
    String xml = response.getEntity(String.class);

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodesApps = dom.getElementsByTagName("nodes");
    assertEquals("incorrect number of elements", 1, nodesApps.getLength());
    NodeList nodes = dom.getElementsByTagName("node");
    assertEquals("incorrect number of elements", 2, nodes.getLength());
    rm.stop();
  }
  
  @Test (timeout = 60000)
  public void testQueryAll() throws Exception {
    WebResource r = resource();
    MockNM nm1 = rm.registerNode("h1:1234", 5120);
    MockNM nm2 = rm.registerNode("h2:1235", 5121);
    MockNM nm3 = rm.registerNode("h3:1236", 5122);
    rm.sendNodeStarted(nm1);
    rm.sendNodeStarted(nm3);
    rm.NMwaitForState(nm1.getNodeId(), NodeState.RUNNING);
    rm.NMwaitForState(nm2.getNodeId(), NodeState.NEW);
    rm.sendNodeLost(nm3);

    ClientResponse response =
        r.path("ws").path("v1").path("cluster").path("nodes")
            .queryParam("states",
                Joiner.on(',').join(EnumSet.allOf(NodeState.class)))
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    JSONObject json = response.getEntity(JSONObject.class);
    JSONObject nodes = json.getJSONObject("nodes");
    assertEquals("incorrect number of elements", 1, nodes.length());
    JSONArray nodeArray = nodes.getJSONArray("node");
    assertEquals("incorrect number of elements", 3, nodeArray.length());
  }

  public void verifyNodesXML(NodeList nodes, MockNM nm)
      throws JSONException, Exception {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyNodeInfoGeneric(nm,
          WebServicesTestUtils.getXmlString(element, "state"),
          WebServicesTestUtils.getXmlString(element, "rack"),
          WebServicesTestUtils.getXmlString(element, "id"),
          WebServicesTestUtils.getXmlString(element, "nodeHostName"),
          WebServicesTestUtils.getXmlString(element, "nodeHTTPAddress"),
          WebServicesTestUtils.getXmlLong(element, "lastHealthUpdate"),
          WebServicesTestUtils.getXmlString(element, "healthReport"),
          WebServicesTestUtils.getXmlInt(element, "numContainers"),
          WebServicesTestUtils.getXmlLong(element, "usedMemoryMB"),
          WebServicesTestUtils.getXmlLong(element, "availMemoryMB"),
          WebServicesTestUtils.getXmlString(element, "version"));
    }
  }

  public void verifyNodeInfo(JSONObject nodeInfo, MockNM nm)
      throws JSONException, Exception {
    assertEquals("incorrect number of elements", 11, nodeInfo.length());

    verifyNodeInfoGeneric(nm, nodeInfo.getString("state"),
        nodeInfo.getString("rack"), nodeInfo.getString("id"),
        nodeInfo.getString("nodeHostName"),
        nodeInfo.getString("nodeHTTPAddress"),
        nodeInfo.getLong("lastHealthUpdate"),
        nodeInfo.getString("healthReport"), nodeInfo.getInt("numContainers"),
        nodeInfo.getLong("usedMemoryMB"), nodeInfo.getLong("availMemoryMB"),
        nodeInfo.getString("version"));

  }

  public void verifyNodeInfoGeneric(MockNM nm, String state, String rack,
      String id, String nodeHostName, String nodeHTTPAddress,
      long lastHealthUpdate, String healthReport, int numContainers,
      long usedMemoryMB, long availMemoryMB, String version)
      throws JSONException, Exception {

    RMNode node = rm.getRMContext().getActiveRMNodes().get(nm.getNodeId());
    ResourceScheduler sched = rm.getResourceScheduler();
    SchedulerNodeReport report = sched.getNodeReport(nm.getNodeId());

    WebServicesTestUtils
        .checkStringMatch("state", node.getState().toString(), state);
    WebServicesTestUtils.checkStringMatch("rack", node.getRackName(), rack);
    WebServicesTestUtils.checkStringMatch("id", nm.getNodeId().toString(), id);
    WebServicesTestUtils
        .checkStringMatch("nodeHostName", nm.getNodeId().getHost(),
            nodeHostName);
    WebServicesTestUtils.checkStringMatch("healthReport",
        String.valueOf(node.getHealthReport()), healthReport);
    String expectedHttpAddress =
        nm.getNodeId().getHost() + ":" + nm.getHttpPort();
    WebServicesTestUtils
        .checkStringMatch("nodeHTTPAddress", expectedHttpAddress,
            nodeHTTPAddress);
    WebServicesTestUtils
        .checkStringMatch("version", node.getNodeManagerVersion(), version);

    long expectedHealthUpdate = node.getLastHealthReportTime();
    assertEquals("lastHealthUpdate doesn't match, got: " + lastHealthUpdate +
            " expected: " + expectedHealthUpdate, expectedHealthUpdate,
        lastHealthUpdate);

    if (report != null) {
      assertEquals("numContainers doesn't match: " + numContainers,
          report.getNumContainers(), numContainers);
      assertEquals("usedMemoryMB doesn't match: " + usedMemoryMB,
          report.getUsedResource().getMemory(), usedMemoryMB);
      assertEquals("availMemoryMB doesn't match: " + availMemoryMB,
          report.getAvailableResource().getMemory(), availMemoryMB);
    }
  }

}
