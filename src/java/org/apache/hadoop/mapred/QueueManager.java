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

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Queue.QueueState;
import org.apache.hadoop.security.SecurityUtil.AccessControlList;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.net.URL;


/**
 * Class that exposes information about queues maintained by the Hadoop
 * Map/Reduce framework.
 * <p/>
 * The Map/Reduce framework can be configured with one or more queues,
 * depending on the scheduler it is configured with. While some
 * schedulers work only with one queue, some schedulers support multiple
 * queues. Some schedulers also support the notion of queues within
 * queues - a feature called hierarchical queues.
 * <p/>
 * Queue names are unique, and used as a key to lookup queues. Hierarchical
 * queues are named by a 'fully qualified name' such as q1:q2:q3, where
 * q2 is a child queue of q1 and q3 is a child queue of q2.
 * <p/>
 * Leaf level queues are queues that contain no queues within them. Jobs
 * can be submitted only to leaf level queues.
 * <p/>
 * Queues can be configured with various properties. Some of these
 * properties are common to all schedulers, and those are handled by this
 * class. Schedulers might also associate several custom properties with
 * queues. These properties are parsed and maintained per queue by the
 * framework. If schedulers need more complicated structure to maintain
 * configuration per queue, they are free to not use the facilities
 * provided by the framework, but define their own mechanisms. In such cases,
 * it is likely that the name of the queue will be used to relate the
 * common properties of a queue with scheduler specific properties.
 * <p/>
 * Information related to a queue, such as its name, properties, scheduling
 * information and children are exposed by this class via a serializable
 * class called {@link JobQueueInfo}.
 * <p/>
 * Queues are configured in the configuration file mapred-queues.xml.
 * To support backwards compatibility, queues can also be configured
 * in mapred-site.xml. However, when configured in the latter, there is
 * no support for hierarchical queues.
 */

class QueueManager {

  private static final Log LOG = LogFactory.getLog(QueueManager.class);

  // Map of a queue name and Queue object
  private Map<String, Queue> leafQueues = new HashMap<String,Queue>();
  private Map<String, Queue> allQueues = new HashMap<String, Queue>();
  static final String QUEUE_CONF_FILE_NAME = "mapred-queues.xml";

  // Prefix in configuration for queue related keys
  static final String QUEUE_CONF_PROPERTY_NAME_PREFIX
    = "mapred.queue.";
  //Resource in which queue acls are configured.

  private Queue root = null;
  private boolean isAclEnabled = false;

  /**
   * Factory method to create an appropriate instance of a queue
   * configuration parser.
   * <p/>
   * Returns a parser that can parse either the deprecated property
   * style queue configuration in mapred-site.xml, or one that can
   * parse hierarchical queues in mapred-queues.xml. First preference
   * is given to configuration in mapred-site.xml. If no queue
   * configuration is found there, then a parser that can parse
   * configuration in mapred-queues.xml is created.
   *
   * @param conf Configuration instance that determines which parser
   *             to use.
   * @return Queue configuration parser
   */
  static QueueConfigurationParser getQueueConfigurationParser(
    Configuration conf, boolean reloadConf) {
    if (conf != null && conf.get(
      DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY) != null) {
      if (reloadConf) {
        conf.reloadConfiguration();
      }
      return new DeprecatedQueueConfigurationParser(conf);
    } else {
      URL filePath =
        ClassLoader.getSystemClassLoader().getResource(QUEUE_CONF_FILE_NAME);
      return new QueueConfigurationParser(filePath.getPath());
    }
  }

  public QueueManager() {
    initialize(getQueueConfigurationParser(null, false));
  }

  /**
   * Construct a new QueueManager using configuration specified in the passed
   * in {@link org.apache.hadoop.conf.Configuration} object.
   * <p/>
   * This instance supports queue configuration specified in mapred-site.xml,
   * but without support for hierarchical queues. If no queue configuration
   * is found in mapred-site.xml, it will then look for site configuration
   * in mapred-queues.xml supporting hierarchical queues.
   *
   * @param conf Configuration object where queue configuration is specified.
   */
  public QueueManager(Configuration conf) {
    initialize(getQueueConfigurationParser(conf, false));
  }

  /**
   * Create an instance that supports hierarchical queues, defined in
   * the passed in configuration file.
   * <p/>
   * This is mainly used for testing purposes and should not called from
   * production code.
   *
   * @param confFile File where the queue configuration is found.
   */
  QueueManager(String confFile) {
    QueueConfigurationParser cp = new QueueConfigurationParser(confFile);
    initialize(cp);
  }

  private void initialize(QueueConfigurationParser cp) {
    this.root = cp.getRoot();
    leafQueues.clear();
    allQueues.clear();
    //At this point we have root populated
    //update data structures leafNodes.
    leafQueues = getRoot().getLeafQueues();
    allQueues.putAll(getRoot().getInnerQueues());
    allQueues.putAll(leafQueues);

    LOG.info(
      "Leaf queues and allQueues " + allQueues + " " +
        "leafQueues " + leafQueues);
    this.isAclEnabled = cp.isAclsEnabled();
  }


  /**
   * Return the set of leaf level queues configured in the system to
   * which jobs are submitted.
   * <p/>
   * The number of queues configured should be dependent on the Scheduler
   * configured. Note that some schedulers work with only one queue, whereas
   * others can support multiple queues.
   *
   * @return Set of queue names.
   */
  public synchronized Set<String> getLeafQueueNames() {
    return leafQueues.keySet();
  }

  /**
   * Return true if the given {@link Queue.QueueOperation} can be
   * performed by the specified user on the given queue.
   * <p/>
   * An operation is allowed if all users are provided access for this
   * operation, or if either the user or any of the groups specified is
   * provided access.
   *
   * @param queueName Queue on which the operation needs to be performed.
   * @param oper      The operation to perform
   * @param ugi       The user and groups who wish to perform the operation.
   * @return true if the operation is allowed, false otherwise.
   */
  public synchronized boolean hasAccess(
    String queueName,
    Queue.QueueOperation oper,
    UserGroupInformation ugi) {
    return hasAccess(queueName, null, oper, ugi);
  }

  /**
   * Return true if the given {@link Queue.QueueOperation} can be
   * performed by the specified user on the specified job in the given queue.
   * <p/>
   * An operation is allowed either if the owner of the job is the user
   * performing the task, all users are provided access for this
   * operation, or if either the user or any of the groups specified is
   * provided access.
   * <p/>
   * If the {@link Queue.QueueOperation} is not job specific then the
   * job parameter is ignored.
   *
   * @param queueName Queue on which the operation needs to be performed.
   * @param job       The {@link JobInProgress} on which the operation is being
   *                  performed.
   * @param oper      The operation to perform
   * @param ugi       The user and groups who wish to perform the operation.
   * @return true if the operation is allowed, false otherwise.
   */
  public synchronized boolean hasAccess(
    String queueName, JobInProgress job,
    Queue.QueueOperation oper,
    UserGroupInformation ugi) {
    
    Queue q = leafQueues.get(queueName);

    if (q == null) {
      LOG.info("Queue " + queueName + " is not present");
      return false;
    }

    if(q.getChildren() != null && !q.getChildren().isEmpty()) {
      LOG.info("Cannot submit job to parent queue " + q.getName());
      return false;
    }

    if (!isAclsEnabled()) {
      return true;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "checking access for : "
          + QueueManager.toFullPropertyName(queueName, oper.getAclName()));
    }

    if (oper.isJobOwnerAllowed()) {
      if (job != null
        && job.getJobConf().getUser().equals(ugi.getUserName())) {
        return true;
      }
    }

    AccessControlList acl = q.getAcls().get(
      toFullPropertyName(
        queueName,
        oper.getAclName()));
    if (acl == null) {
      return false;
    }

    // Check the ACL list
    boolean allowed = acl.allAllowed();
    if (!allowed) {
      // Check the allowed users list
      if (acl.getUsers().contains(ugi.getUserName())) {
        allowed = true;
      } else {
        // Check the allowed groups list
        Set<String> allowedGroups = acl.getGroups();
        for (String group : ugi.getGroupNames()) {
          if (allowedGroups.contains(group)) {
            allowed = true;
            break;
          }
        }
      }
    }

    return allowed;
  }

  /**
   * Checks whether the given queue is running or not.
   *
   * @param queueName name of the queue
   * @return true, if the queue is running.
   */
  synchronized boolean isRunning(String queueName) {
    Queue q = leafQueues.get(queueName);
    if (q != null) {
      return q.getState().equals(QueueState.RUNNING);
    }
    return false;
  }

  /**
   * Set a generic Object that represents scheduling information relevant
   * to a queue.
   * <p/>
   * A string representation of this Object will be used by the framework
   * to display in user facing applications like the JobTracker web UI and
   * the hadoop CLI.
   *
   * @param queueName queue for which the scheduling information is to be set.
   * @param queueInfo scheduling information for this queue.
   */
  public synchronized void setSchedulerInfo(
    String queueName,
    Object queueInfo) {
    if (allQueues.get(queueName) != null) {
      allQueues.get(queueName).setSchedulingInfo(queueInfo);
    }
  }

  /**
   * Return the scheduler information configured for this queue.
   *
   * @param queueName queue for which the scheduling information is required.
   * @return The scheduling information for this queue.
   */
  public synchronized Object getSchedulerInfo(String queueName) {
    if (allQueues.get(queueName) != null) {
      return allQueues.get(queueName).getSchedulingInfo();
    }
    return null;
  }

  /**
   * Refresh acls, state and scheduler properties for the configured queues.
   * <p/>
   * This method reloads configuration related to queues, but does not
   * support changes to the list of queues or hierarchy. The expected usage
   * is that an administrator can modify the queue configuration file and
   * fire an admin command to reload queue configuration. If there is a
   * problem in reloading configuration, then this method guarantees that
   * existing queue configuration is untouched and in a consistent state.
   *
   * @throws IOException when queue configuration file is invalid.
   */
  synchronized void refreshQueues(Configuration conf) throws IOException {
    QueueConfigurationParser cp
      = QueueManager.getQueueConfigurationParser(conf, true);
    if (!root.isHierarchySameAs(cp.getRoot())) {
      throw new IOException(
        "unable to refresh queues , queue hierarchy changed " +
          "retaining existing configuration ");
    }
    initialize(cp);
  }

  static final String toFullPropertyName(
    String queue,
    String property) {
    return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }

  /**
   * Return an array of {@link JobQueueInfo} objects for all the
   * queues configurated in the system.
   *
   * @return array of JobQueueInfo objects.
   */
  synchronized JobQueueInfo[] getJobQueueInfos() {
    ArrayList<JobQueueInfo> queueInfoList = new ArrayList<JobQueueInfo>();
    for (String queue : allQueues.keySet()) {
      JobQueueInfo queueInfo = getJobQueueInfo(queue);
      if (queueInfo != null) {
        queueInfoList.add(queueInfo);
      }
    }
    return queueInfoList.toArray(
      new JobQueueInfo[queueInfoList.size()]);
  }


  /**
   * Return {@link JobQueueInfo} for a given queue.
   *
   * @param queue name of the queue
   * @return JobQueueInfo for the queue, null if the queue is not found.
   */
  synchronized JobQueueInfo getJobQueueInfo(String queue) {
    if (allQueues.containsKey(queue)) {
      return allQueues.get(queue).getJobQueueInfo();
    }

    return null;
  }

  /**
   * JobQueueInfo for all the queues.
   * <p/>
   * Contribs can use this data structure to either create a hierarchy or for
   * traversing.
   * They can also use this to refresh properties in case of refreshQueues
   *
   * @return a map for easy navigation.
   */
  synchronized Map<String, JobQueueInfo> getJobQueueInfoMapping() {
    Map<String, JobQueueInfo> m = new HashMap<String, JobQueueInfo>();

    for (String key : allQueues.keySet()) {
      m.put(key, allQueues.get(key).getJobQueueInfo());
    }

    return m;
  }

  /**
   * Generates the array of QueueAclsInfo object.
   * <p/>
   * The array consists of only those queues for which user has acls.
   *
   * @return QueueAclsInfo[]
   * @throws java.io.IOException
   */
  synchronized QueueAclsInfo[] getQueueAcls(UserGroupInformation ugi)
    throws IOException {
    //List of all QueueAclsInfo objects , this list is returned
    ArrayList<QueueAclsInfo> queueAclsInfolist =
      new ArrayList<QueueAclsInfo>();
    Queue.QueueOperation[] operations = Queue.QueueOperation.values();
    for (String queueName : leafQueues.keySet()) {
      QueueAclsInfo queueAclsInfo = null;
      ArrayList<String> operationsAllowed = null;
      for (Queue.QueueOperation operation : operations) {
        if (hasAccess(queueName, operation, ugi)) {
          if (operationsAllowed == null) {
            operationsAllowed = new ArrayList<String>();
          }
          operationsAllowed.add(operation.getAclName());
        }
      }
      if (operationsAllowed != null) {
        //There is atleast 1 operation supported for queue <queueName>
        //, hence initialize queueAclsInfo
        queueAclsInfo = new QueueAclsInfo(
          queueName, operationsAllowed.toArray
            (new String[operationsAllowed.size()]));
        queueAclsInfolist.add(queueAclsInfo);
      }
    }
    return queueAclsInfolist.toArray(
      new QueueAclsInfo[
        queueAclsInfolist.size()]);
  }

  /**
   * ONLY FOR TESTING - Do not use in production code.
   * This method is used for setting up of leafQueues only.
   * We are not setting the hierarchy here.
   *
   * @param queues
   */
  synchronized void setQueues(Queue[] queues) {
    root.getChildren().clear();
    leafQueues.clear();
    allQueues.clear();

    for (Queue queue : queues) {
      root.addChild(queue);
    }
    //At this point we have root populated
    //update data structures leafNodes.
    leafQueues = getRoot().getLeafQueues();
    allQueues.putAll(getRoot().getInnerQueues());
    allQueues.putAll(leafQueues);
  }

  /**
   * Return an array of {@link JobQueueInfo} objects for the root
   * queues configured in the system.
   * <p/>
   * Root queues are queues that are at the top-most level in the
   * hierarchy of queues in mapred-queues.xml, or they are the queues
   * configured in the mapred.queue.names key in mapred-site.xml.
   *
   * @return array of JobQueueInfo objects for root level queues.
   */

  JobQueueInfo[] getRootQueues() {
    List<JobQueueInfo> list = getRoot().getJobQueueInfo().getChildren();
    return list.toArray(new JobQueueInfo[list.size()]);
  }

  /**
   * Get the complete hierarchy of children for queue
   * queueName
   *
   * @param queueName
   * @return
   */
  JobQueueInfo[] getChildQueues(String queueName) {
    List<JobQueueInfo> list =
      allQueues.get(queueName).getJobQueueInfo().getChildren();
    if (list != null) {
      return list.toArray(new JobQueueInfo[list.size()]);
    } else {
      return new JobQueueInfo[0];
    }
  }

  /**
   * Used only for testing purposes .
   * This method is unstable as refreshQueues would leave this
   * data structure in unstable state.
   *
   * @param queueName
   * @return
   */
  Queue getQueue(String queueName) {
    return this.allQueues.get(queueName);
  }


  /**
   * Return if ACLs are enabled for the Map/Reduce system
   *
   * @return true if ACLs are enabled.
   */
  boolean isAclsEnabled() {
    return isAclEnabled;
  }

  /**
   * Used only for test.
   *
   * @return
   */
  Queue getRoot() {
    return root;
  }
}
