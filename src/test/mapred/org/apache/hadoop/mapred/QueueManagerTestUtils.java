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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import static org.apache.hadoop.mapred.Queue.*;
import static org.apache.hadoop.mapred.QueueConfigurationParser.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.dom.DOMSource;
import java.util.Properties;
import java.util.Set;
import java.io.File;

public class QueueManagerTestUtils {
  final static String CONFIG = new File("./test-mapred-queues.xml")
    .getAbsolutePath();

  //methods to create hierarchy.
  public static Document createDocument() throws Exception {
    Document doc = DocumentBuilderFactory
      .newInstance().newDocumentBuilder().newDocument();
    return doc;
  }

  public static void createSimpleDocument(
    Document doc) throws Exception {
    Element queues = createQueuesNode(doc, "true");

    //Create parent level queue q1.
    Element q1 = createQueue(doc, "q1");
    Properties props = new Properties();
    props.setProperty("capacity", "10");
    props.setProperty("maxCapacity", "35");
    q1.appendChild(createProperties(doc, props));
    queues.appendChild(q1);

    //Create another parent level p1
    Element p1 = createQueue(doc, "p1");

    //append child p11 to p1
    p1.appendChild(createQueue(doc, "p11"));

    Element p12 = createQueue(doc, "p12");

    p12.appendChild(createState(doc, QueueState.STOPPED.getStateName()));
    p12.appendChild(createAcls(doc, QueueConfigurationParser.ACL_SUBMIT_JOB_TAG, "u1"));
    p12.appendChild(createAcls(doc, QueueConfigurationParser.ACL_ADMINISTER_JOB_TAG, "u2"));

    //append p12 to p1.
    p1.appendChild(p12);


    queues.appendChild(p1);
  }

  public static void refreshSimpleDocument(
    Document doc) throws Exception {
    Element queues = createQueuesNode(doc, "true");

    //Create parent level queue q1.
    Element q1 = createQueue(doc, "q1");
    Properties props = new Properties();
    props.setProperty("capacity", "70");
    props.setProperty("maxCapacity", "35");
    q1.appendChild(createProperties(doc, props));
    queues.appendChild(q1);

    //Create another parent level p1
    Element p1 = createQueue(doc, "p1");

    //append child p11 to p1
    Element p11 = createQueue(doc, "p11");
    p11.appendChild(createState(doc, QueueState.STOPPED.getStateName()));
    p1.appendChild(p11);

    Element p12 = createQueue(doc, "p12");

    p12.appendChild(createState(doc, QueueState.RUNNING.getStateName()));
    p12.appendChild(createAcls(doc, "acl-submit-job", "u3"));
    p12.appendChild(createAcls(doc, "acl-administer-jobs", "u4"));

    //append p12 to p1.
    p1.appendChild(p12);


    queues.appendChild(p1);
  }

  /**
   * Adding a new child to q1
   *
   * @param doc
   * @throws Exception
   */
  public static void addMoreChildToSimpleDocumentStructure(Document doc)
    throws Exception {
    Element queues = createQueuesNode(doc, "true");

    //Create parent level queue q1.
    Element q1 = createQueue(doc, "q1");
    Properties props = new Properties();
    props.setProperty("capacity", "70");
    props.setProperty("maxCapacity", "35");
    q1.appendChild(createProperties(doc, props));
    queues.appendChild(q1);

    //Adding q11 to existing simple document
    q1.appendChild(createQueue(doc, "q11"));

    //Create another parent level p1
    Element p1 = createQueue(doc, "p1");

    //append child p11 to p1
    Element p11 = createQueue(doc, "p11");
    p11.appendChild(createState(doc, QueueState.STOPPED.getStateName()));
    p1.appendChild(p11);

    Element p12 = createQueue(doc, "p12");

    p12.appendChild(createState(doc, QueueState.RUNNING.getStateName()));
    p12.appendChild(createAcls(doc, ACL_SUBMIT_JOB_TAG, "u3"));
    p12.appendChild(createAcls(doc, ACL_ADMINISTER_JOB_TAG, "u4"));

    //append p12 to p1.
    p1.appendChild(p12);


    queues.appendChild(p1);
  }

  public static Element createQueuesNode(Document doc, String enable) {
    Element queues = doc.createElement("queues");
    doc.appendChild(queues);
    queues.setAttribute("aclsEnabled", enable);
    return queues;
  }

  public static void writeToFile(Document doc, String filePath)
    throws TransformerException {
    Transformer trans = TransformerFactory.newInstance().newTransformer();
    trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    trans.setOutputProperty(OutputKeys.INDENT, "yes");
    DOMSource source = new DOMSource(doc);
    trans.transform(source, new StreamResult(new File(filePath)));
  }

  public static Element createQueue(Document doc, String name) {
    Element queue = doc.createElement("queue");
    Element nameNode = doc.createElement("name");
    nameNode.setTextContent(name);
    queue.appendChild(nameNode);
    return queue;
  }

  public static Element createAcls(
    Document doc, String aclName, String listNames) {
    Element acls = doc.createElement(aclName);
    acls.setTextContent(listNames);
    return acls;
  }

  public static Element createState(Document doc, String state) {
    Element stateElement = doc.createElement("state");
    stateElement.setTextContent(state);
    return stateElement;
  }

  public static Element createProperties(Document doc, Properties props) {
    Element propsElement = doc.createElement("properties");
    if (props != null) {
      Set<String> propList = props.stringPropertyNames();
      for (String prop : propList) {
        Element property = doc.createElement("property");
        property.setAttribute("key", prop);
        property.setAttribute("value", (String) props.get(prop));
        propsElement.appendChild(property);
      }
    }
    return propsElement;
  }

  public static void checkForConfigFile() {
    if (new File(CONFIG).exists()) {
      new File(CONFIG).delete();
    }
  }
}
