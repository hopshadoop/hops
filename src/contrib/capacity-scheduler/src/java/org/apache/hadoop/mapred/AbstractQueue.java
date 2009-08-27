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
import org.apache.hadoop.conf.Configuration;

import java.util.Comparator;
import java.util.List;


/**
 * Parent class for hierarchy of queues.
 * All queues extend this class.
 * <p/>
 * Even though all the Queue classes extend this class , there are 2 categories
 * of queues define.
 * <p/>
 * 1.ContainerQueue: which are composite of queues.
 * 2.JobQueue: leaf level queues.
 * <p/>
 * Typically ContainerQueue consists of JobQueue. All the SchedulingContext data
 * in ContainerQueue is cummulative of its children.
 * <p/>
 * JobQueue consists of actual job list , i.e, runningJob, WaitingJob etc.
 * <p/>
 * This is done so to make sure that all the job related data is at one place
 * and queues at higher level are typically cummulative data of organization at
 * there children level.
 */

public abstract class AbstractQueue {

  static final Log LOG = LogFactory.getLog(AbstractQueue.class);

  protected QueueSchedulingContext qsc;
  protected AbstractQueue parent;


  protected AbstractQueue(AbstractQueue parent, QueueSchedulingContext qsc) {
    this.parent = parent;
    this.qsc = qsc;
    //Incase of root this value would be null
    if (parent != null) {
      parent.addChild(this);
    }
  }

  /**
   * This involves updating each qC structure.
   * <p/>
   * First update QueueSchedulingContext at this level is updated.
   * then update QueueSchedulingContext of all the children.
   * <p/>
   * Children consider parent's capacity as the totalclustercapacity
   * and do there calculations accordingly.
   *
   * @param mapClusterCapacity
   * @param reduceClusterCapacity
   */

  public void update(int mapClusterCapacity, int reduceClusterCapacity) {
    qsc.updateContext(mapClusterCapacity,reduceClusterCapacity);
  }

  /**
   * @return qsc
   */
  public QueueSchedulingContext getQueueSchedulingContext() {
    return qsc;
  }

  String getName() {
    return qsc.getQueueName();
  }

  protected AbstractQueue getParent() {
    return parent;
  }

  protected void setParent(AbstractQueue queue) {
    this.parent = queue;
  }

  /**
   * Return sorted list of leaf level queues.
   *
   * @return
   */
  public abstract List<AbstractQueue> getDescendentJobQueues();

  /**
   * Sorts all levels below current level.
   *
   * @param queueComparator
   */
  public abstract void sort(Comparator queueComparator);

  /**
   * returns list of immediate children.
   * null in case of leaf.
   *
   * @return
   */
  abstract List<AbstractQueue> getChildren();

  /**
   * adds children to the current level.
   * There is no support for adding children at leaf level node.
   *
   * @param queue
   */
  public abstract void addChild(AbstractQueue queue);

  /**
   * Distribute the unconfigured capacity % among the queues.
   *
   */
  abstract void distributeUnConfiguredCapacity();

  @Override
  public String toString() {
    return this.getName().toString() 
            + "\n" + getQueueSchedulingContext().toString();
  }

  @Override
  /**
   * Returns true, if the other object is an AbstractQueue
   * with the same name.
   */
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof AbstractQueue)) {
      return false;
    }
    
    AbstractQueue otherQueue = (AbstractQueue)other;
    return otherQueue.getName().equals(getName());
  }
  
  @Override
  public int hashCode() {
    return this.getName().hashCode();
  }

}
