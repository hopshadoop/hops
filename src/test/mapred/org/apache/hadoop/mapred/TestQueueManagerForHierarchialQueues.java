/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.*;
import static org.apache.hadoop.mapred.QueueConfigurationParser.*;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


public class TestQueueManagerForHierarchialQueues extends TestCase {

  private static final Log LOG = LogFactory.getLog(
    TestQueueManagerForHierarchialQueues.class);


  protected void tearDown() throws Exception {
    super.tearDown();
    new File(CONFIG).delete();
  }

  public void testDefault() throws Exception {
    QueueManager qm = new QueueManager();
    Queue root = qm.getRoot();
    assertEquals(root.getChildren().size(), 1);
    assertEquals(root.getChildren().iterator().next().getName(), "default");
    assertFalse(qm.isAclsEnabled());
    assertNull(root.getChildren().iterator().next().getChildren());
  }

  public void testXMLParsing() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);
    Set<Queue> rootQueues = qm.getRoot().getChildren();
    List<String> names = new ArrayList<String>();
    for (Queue q : rootQueues) {
      names.add(q.getName());
    }

    //Size of root.
    assertEquals(rootQueues.size(), 2);

    //check root level queues
    assertTrue(names.contains("q1"));
    assertTrue(names.contains("p1"));


    //check for leaf names
    Set<String> leafNames = qm.getLeafQueueNames();
    Queue p = qm.getQueue("p1");
    Set<Queue> children = p.getChildren();
    assertTrue(children.size() == 2);

    //check leaf level queues
    assertTrue(
      leafNames.contains(
        "p1" + NAME_SEPARATOR + "p11"));
    assertTrue(
      leafNames.contains(
        "p1" + NAME_SEPARATOR + "p12"));


    Queue q = qm.getQueue(
      "p1" + NAME_SEPARATOR + "p12");

    assertTrue(
      q.getAcls().get(
        QueueManager.toFullPropertyName(
          q.getName(), ACL_SUBMIT_JOB_TAG)).getUsers().contains(
        "u1"));

    assertTrue(
      q.getAcls().get(
        QueueManager.toFullPropertyName(
          q.getName(),
          ACL_ADMINISTER_JOB_TAG))
        .getUsers().contains("u2"));
    assertTrue(q.getState().equals(Queue.QueueState.STOPPED));
  }

  public void testhasAccess() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    UserGroupInformation ugi =
      new UnixUserGroupInformation("u1", new String[]{" "});
    assertTrue(
      qm.hasAccess(
        "p1" + NAME_SEPARATOR + "p12",
        Queue.QueueOperation.SUBMIT_JOB, ugi));
  }

  public void testhasAccessForParent() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    UserGroupInformation ugi =
      new UnixUserGroupInformation("u1", new String[]{" "});
    assertFalse(
      qm.hasAccess(
        "p1",
        Queue.QueueOperation.SUBMIT_JOB, ugi));
  }

  public void testValidation() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    Element queues = createQueuesNode(doc, "false");
    Element q1 = createQueue(doc, "q1");

    q1.appendChild(createAcls(doc, "acl-submit-job", "u1"));
    q1.appendChild(createAcls(doc, "acl-administer-jobs", "u2"));
    q1.appendChild(createQueue(doc, "p15"));
    q1.appendChild(createQueue(doc, "p16"));

    queues.appendChild(q1);
    writeToFile(doc, CONFIG);
    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception as configuration is wrong ");
    } catch (RuntimeException re) {
      LOG.info(re.getMessage());
    }
  }

  public void testInvalidName() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    Element queues = createQueuesNode(doc, "false");
    Element q1 = createQueue(doc, "");
    queues.appendChild(q1);
    writeToFile(doc, CONFIG);
    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception as configuration is wrong ");
    } catch (Exception re) {
      re.printStackTrace();
      LOG.info(re.getMessage());
    }
    checkForConfigFile();
    doc = createDocument();
    queues = createQueuesNode(doc, "false");
    q1 = doc.createElement("queue");
    queues.appendChild(q1);
    writeToFile(doc, CONFIG);
    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception as configuration is wrong ");
    } catch (RuntimeException re) {
      re.printStackTrace();
      LOG.info(re.getMessage());
    }
  }


  public void testEmptyProperties() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    Element queues = createQueuesNode(doc, "false");
    Element q1 = createQueue(doc, "q1");
    Element p = createProperties(doc, null);
    q1.appendChild(p);
    queues.appendChild(q1);
  }

  public void testEmptyFile() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    writeToFile(doc, CONFIG);
    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception as configuration is wrong ");
    } catch (Exception re) {
      re.printStackTrace();
      LOG.info(re.getMessage());
    }
  }

  public void testJobQueueInfoGeneration() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    List<JobQueueInfo> rootQueues =
      qm.getRoot().getJobQueueInfo().getChildren();
    assertEquals(rootQueues.size(), 2);
    List<String> names = new ArrayList<String>();
    for (JobQueueInfo q : rootQueues) {
      names.add(q.getQueueName());
      if (q.getQueueName().equals("q1")) {
        Properties p = q.getProperties();
        assertEquals(p.getProperty("capacity"), "10");
        assertEquals(p.getProperty("maxCapacity"), "35");

        assertTrue(q.getChildren().isEmpty());
      } else if (q.getQueueName().equals("p1")) {
        List<JobQueueInfo> children = q.getChildren();
        assertEquals(children.size(), 2);
        for (JobQueueInfo child : children) {
          if (child.getQueueName().equals(
            "p1" + NAME_SEPARATOR + "p12")) {
            assertEquals(
              child.getQueueState(), Queue.QueueState.STOPPED.getStateName());
          } else if (child.getQueueName().equals(
            "p1" + NAME_SEPARATOR + "p11")) {
            assertEquals(
              child.getQueueState(), Queue.QueueState.RUNNING.getStateName());
          } else {
            fail("Only 2 children");
          }
        }
      } else {
        fail("Only 2 queues with q1 and p1 ");
      }
    }
  }

  public void testRefresh() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);
    Queue beforeRefreshRoot = qm.getRoot();
    //remove the file and create new one.
    Set<Queue> rootQueues = beforeRefreshRoot.getChildren();
    for (Queue qs : rootQueues) {
      if (qs.getName().equals("q1")) {

        assertEquals(qs.getProperties().getProperty("capacity"), "10");
        assertEquals(qs.getProperties().getProperty("maxCapacity"), "35");

      } else if (qs.getName().equals("p1")) {

        Set<Queue> children = qs.getChildren();
        for (Queue child : children) {
          if (child.getName().equals(
            "p1" + NAME_SEPARATOR + "p12")) {
            assertTrue(
              child.getAcls().get(
                QueueManager.toFullPropertyName(
                  child.getName(), ACL_SUBMIT_JOB_TAG))
                .getUsers().contains("u1"));

            assertTrue(
              child.getAcls().get(
                QueueManager.toFullPropertyName(
                  child.getName(),
                  ACL_ADMINISTER_JOB_TAG))
                .getUsers().contains("u2"));
            assertTrue(child.getState().equals(Queue.QueueState.STOPPED));
          } else {
            assertTrue(child.getState().equals(Queue.QueueState.RUNNING));
          }
        }
      }
    }
    checkForConfigFile();
    doc = createDocument();
    refreshSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueConfigurationParser cp = new QueueConfigurationParser(CONFIG);
    qm.getRoot().isHierarchySameAs(cp.getRoot());
    qm.setQueues(
      cp.getRoot().getChildren().toArray(
        new Queue[cp.getRoot().getChildren().size()]));
    Queue afterRefreshRoot = qm.getRoot();
    //remove the file and create new one.
    rootQueues = afterRefreshRoot.getChildren();
    for (Queue qs : rootQueues) {
      if (qs.getName().equals("q1")) {

        assertEquals(qs.getProperties().getProperty("capacity"), "70");
        assertEquals(qs.getProperties().getProperty("maxCapacity"), "35");

      } else if (qs.getName().equals("p1")) {

        Set<Queue> children = qs.getChildren();
        for (Queue child : children) {
          if (child.getName().equals(
            "p1" + NAME_SEPARATOR + "p12")) {
            assertTrue(
              child.getAcls().get(
                QueueManager.toFullPropertyName(
                  child.getName(),
                  ACL_SUBMIT_JOB_TAG))
                .getUsers().contains("u3"));

            assertTrue(
              child.getAcls().get(
                QueueManager.toFullPropertyName(
                  child.getName(),
                  ACL_ADMINISTER_JOB_TAG))
                .getUsers().contains("u4"));
            assertTrue(child.getState().equals(Queue.QueueState.RUNNING));
          } else {
            assertTrue(child.getState().equals(Queue.QueueState.STOPPED));
          }
        }
      }
    }
  }

  public void testRefreshFailureForHierarchyChange() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    checkForConfigFile();
    doc = createDocument();
    addMoreChildToSimpleDocumentStructure(doc);
    writeToFile(doc, CONFIG);
    QueueConfigurationParser cp = new QueueConfigurationParser(CONFIG);
    assertFalse(qm.getRoot().isHierarchySameAs(cp.getRoot()));
  }

  public void testRefreshWithInvalidFile() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    checkForConfigFile();
    doc = createDocument();
    Element queues = createQueuesNode(doc, "false");
    Element q1 = createQueue(doc, "");
    queues.appendChild(q1);
    writeToFile(doc, CONFIG);
    try {
      QueueConfigurationParser cp = new QueueConfigurationParser(CONFIG);

      fail("Should throw an exception as configuration is wrong ");
    } catch (Throwable re) {
      re.printStackTrace();
      LOG.info(re.getMessage());
    }
  }
}
