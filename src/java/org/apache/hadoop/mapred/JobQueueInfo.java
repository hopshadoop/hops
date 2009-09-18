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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.QueueState;

/**
 * Class that contains the information regarding the Job Queues which are 
 * maintained by the Hadoop Map/Reduce framework.
 * @deprecated Use {@link QueueInfo} instead
 */
@Deprecated
public class JobQueueInfo extends QueueInfo {

  /**
   * Default constructor for Job Queue Info.
   * 
   */
  public JobQueueInfo() {
    super();  
  }

  /**
   * Construct a new JobQueueInfo object using the queue name and the
   * scheduling information passed.
   * 
   * @param queueName Name of the job queue
   * @param schedulingInfo Scheduling Information associated with the job
   * queue
   */
  public JobQueueInfo(String queueName, String schedulingInfo) {
    super(queueName, schedulingInfo);
  }
  
  JobQueueInfo(QueueInfo queue) {
    this(queue.getQueueName(), queue.getSchedulingInfo());
    setQueueState(queue.getState().name());
    setQueueChildren(queue.getQueueChildren());
    setProperties(queue.getProperties());
    setJobStatuses(queue.getJobStatuses());
  }
  
  /**
   * Set the queue name of the JobQueueInfo
   * 
   * @param queueName Name of the job queue.
   */
  protected void setQueueName(String queueName) {
    super.setQueueName(queueName);
  }

  /**
   * Set the scheduling information associated to particular job queue
   * 
   * @param schedulingInfo
   */
  protected void setSchedulingInfo(String schedulingInfo) {
    super.setSchedulingInfo(schedulingInfo);
  }

  /**
   * Set the state of the queue
   * @param state state of the queue.
   */
  protected void setQueueState(String state) {
    super.setState(QueueState.valueOf(state));
  }
  
  String getQueueState() {
    return super.getState().toString();
  }
  
  protected void setChildren(List<JobQueueInfo> children) {
    List<QueueInfo> list = new ArrayList<QueueInfo>();
    for (JobQueueInfo q : children) {
      list.add(q);
    }
    super.setQueueChildren(list);
  }

  public List<JobQueueInfo> getChildren() {
    List<JobQueueInfo> list = new ArrayList<JobQueueInfo>();
    for (QueueInfo q : super.getQueueChildren()) {
      list.add(new JobQueueInfo(q));
    }
    return list;
  }

  protected void setProperties(Properties props) {
    super.setProperties(props);
  }
  
  protected void setJobStatuses(org.apache.hadoop.mapreduce.JobStatus[] stats) {
    super.setJobStatuses(stats);
  }

}
