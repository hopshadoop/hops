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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Set;

/**
 * Hierarchy builder for the CapacityScheduler.
 * 
 */
public class QueueHierarchyBuilder {

  static final Log LOG = LogFactory.getLog(CapacityTaskScheduler.class);
  private final String NAME_SEPERATOR = ".";
  private CapacitySchedulerConf schedConf;
  
  QueueHierarchyBuilder(CapacitySchedulerConf schedConf) {
    this.schedConf = schedConf;
  }
  
  /**
   * The first call would expect that parent has children.
   * @param parent       parent Queue
   * @param children     children
   */
  void createHierarchy(
    AbstractQueue parent, Set<String> children) {
    //check if children have further childrens.
    if (children != null && !children.isEmpty()) {
      float totalCapacity = 0.0f;
      for (String qName : children) {
        if(qName.contains(NAME_SEPERATOR)) {
          throw new IllegalArgumentException( NAME_SEPERATOR  +  "" +
            " not allowed in queue name \'" + qName + "\'.");
        }
        //generate fully qualified name.
        if (!parent.getName().equals("")) {
          qName = parent.getName() + NAME_SEPERATOR + qName;
        }
        //Check if this child has any more children.
        Set<String> childQueues = schedConf.getSubQueues(qName);

        if (childQueues != null && childQueues.size() > 0) {
          //generate a new ContainerQueue and recursively
          //create hierarchy.
          AbstractQueue cq = new ContainerQueue(
            parent,
            loadContext(
              qName));
          //update totalCapacity
          totalCapacity += cq.qsc.getCapacityPercent();
          LOG.info("Created a ContainerQueue " + qName);
          //create child hiearchy
          createHierarchy(cq, childQueues);
        } else {
          //if not this is a JobQueue.

          //create a JobQueue.
          AbstractQueue jq = new JobQueue(
            parent,
            loadContext(
              qName));
          totalCapacity += jq.qsc.getCapacityPercent();
          LOG.info("Created a jobQueue " + qName);
        }
      }

      //check for totalCapacity at each level , the total for children
      //shouldn't cross 100.

      if (totalCapacity > 100.0) {
        throw new IllegalArgumentException(
          "For queue " + parent.getName() +
            " Sum of child queue capacities over 100% at "
            + totalCapacity);
      }
    }
  }


  private QueueSchedulingContext loadContext(
    String queueName) {
    float capacity = schedConf.getCapacity(queueName);
    float stretchCapacity = schedConf.getMaxCapacity(queueName);
    if (capacity == -1.0) {
      LOG.info("No capacity specified for queue " + queueName);
    }
    int ulMin = schedConf.getMinimumUserLimitPercent(queueName);
    // create our QSC and add to our hashmap
    QueueSchedulingContext qsi = new QueueSchedulingContext(
      queueName, capacity, stretchCapacity, ulMin,
      schedConf.getMaxMapCap(
        queueName), schedConf.getMaxReduceCap(queueName));
    qsi.setSupportsPriorities(
      schedConf.isPrioritySupported(
        queueName));
    return qsi;
  }
}
