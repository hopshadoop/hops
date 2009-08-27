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

import org.apache.hadoop.mapreduce.TaskType;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

/**
 * ********************************************************************
 * Keeping track of scheduling information for queues
 * <p/>
 * Maintain information specific to
 * each kind of task, Map or Reduce (num of running tasks, pending
 * tasks etc).
 * <p/>
 * This scheduling information is used to decide how to allocate
 * tasks, redistribute capacity, etc.
 * <p/>
 * A TaskSchedulingContext (TSI) object represents scheduling
 * information for a particular kind of task (Map or Reduce).
 * <p/>
 * ********************************************************************
 */
public class TaskSchedulingContext {

  private TaskType type;
  private static final String LIMIT_NORMALIZED_CAPACITY_STRING
    = "(Capacity is restricted to max limit of %d slots.\n" +
    "Remaining %d slots will be used by other queues.)\n";
  /**
   * the actual capacity, which depends on how many slots are available
   * in the cluster at any given time.
   */
  private int capacity = 0;
  // number of running tasks
  private int numRunningTasks = 0;
  // number of slots occupied by running tasks
  private int numSlotsOccupied = 0;

  //the actual capacity stretch which depends on how many slots are available
  //in cluster at any given time.
  private int maxCapacity = -1;

  /**
   * max task limit
   * This value is the maximum slots that can be used in a
   * queue at any point of time. So for example assuming above config value
   * is 100 , not more than 100 tasks would be in the queue at any point of
   * time, assuming each task takes one slot.
   */
  private int maxTaskLimit = -1;

  /**
   * for each user, we need to keep track of number of slots occupied by
   * running tasks
   */
  private Map<String, Integer> numSlotsOccupiedByUser =
    new HashMap<String, Integer>();
  final static String JOB_SCHEDULING_INFO_FORMAT_STRING =
    "%s running map tasks using %d map slots. %d additional slots reserved." +
      " %s running reduce tasks using %d reduce slots." +
      " %d additional slots reserved.";

  public TaskSchedulingContext(TaskType type) {
    this.type = type;
  }

  /**
   * reset the variables associated with tasks
   */
  void resetTaskVars() {
    setNumRunningTasks(0);
    setNumSlotsOccupied(0);
    for (String s : getNumSlotsOccupiedByUser().keySet()) {
      getNumSlotsOccupiedByUser().put(s, Integer.valueOf(0));
    }
  }


  int getMaxTaskLimit() {
    return maxTaskLimit;
  }

  void setMaxTaskLimit(int maxTaskCap) {
    this.maxTaskLimit = maxTaskCap;
  }

  /**
   * This method checks for maxfinalLimit and
   * sends minimum of maxTaskLimit and capacity.
   *
   * @return
   */
  int getCapacity() {
    if ((maxTaskLimit >= 0) && (maxTaskLimit < capacity)) {
      return maxTaskLimit;
    }
    return capacity;
  }

  /**
   * checks if current capacity reached its maximum capacity %
   *
   * @return
   */
  boolean checkIfReachedMax() {
    if (maxCapacity < 0) {
      return false;
    }
    return (maxCapacity <= numSlotsOccupied);
  }

  /**
   * Mutator method for capacity
   *
   * @param capacity
   */
  void setCapacity(int capacity) {
    this.capacity = capacity;
  }


  /**
   * return information about the tasks
   */
  @Override
  public String toString() {
    float occupiedSlotsAsPercent =
      getCapacity() != 0 ?
        ((float) getNumSlotsOccupied() * 100 / getCapacity()) : 0;
    StringBuffer sb = new StringBuffer();

    sb.append("Capacity: " + getCapacity() + " slots\n");
    //If maxTaskLimit is less than the capacity
    if (getMaxTaskLimit() >= 0 && getMaxTaskLimit() < getCapacity()) {
      sb.append(
        String.format(
          LIMIT_NORMALIZED_CAPACITY_STRING,
          getMaxTaskLimit(), (getCapacity() - getMaxTaskLimit())));
    }
    if (getMaxTaskLimit() >= 0) {
      sb.append(String.format("Maximum Slots Limit: %d\n", getMaxTaskLimit()));
    }
    sb.append(
      String.format(
        "Used capacity: %d (%.1f%% of Capacity)\n",
        Integer.valueOf(getNumSlotsOccupied()), Float
          .valueOf(occupiedSlotsAsPercent)));
    sb.append(
      String.format(
        "Running tasks: %d\n", Integer
          .valueOf(getNumRunningTasks())));
    // include info on active users
    if (getNumSlotsOccupied() != 0) {
      sb.append("Active users:\n");
      for (Map.Entry<String, Integer> entry : getNumSlotsOccupiedByUser()
        .entrySet()) {
        if ((entry.getValue() == null) ||
          (entry.getValue().intValue() <= 0)) {
          // user has no tasks running
          continue;
        }
        sb.append("User '" + entry.getKey() + "': ");
        int numSlotsOccupiedByThisUser = entry.getValue().intValue();
        float p =
          (float) numSlotsOccupiedByThisUser * 100 / getNumSlotsOccupied();
        sb.append(
          String.format(
            "%d (%.1f%% of used capacity)\n", Long
              .valueOf(numSlotsOccupiedByThisUser), Float.valueOf(p)));
      }
    }
    return sb.toString();
  }

  int getNumRunningTasks() {
    return numRunningTasks;
  }

  void setNumRunningTasks(int numRunningTasks) {
    this.numRunningTasks = numRunningTasks;
  }

  int getNumSlotsOccupied() {
    return numSlotsOccupied;
  }

  void setNumSlotsOccupied(int numSlotsOccupied) {
    this.numSlotsOccupied = numSlotsOccupied;
  }

  Map<String, Integer> getNumSlotsOccupiedByUser() {
    return numSlotsOccupiedByUser;
  }

  void setNumSlotsOccupiedByUser(
    Map<String, Integer> numSlotsOccupiedByUser) {
    this.numSlotsOccupiedByUser = numSlotsOccupiedByUser;
  }

  int getMaxCapacity() {
    return maxCapacity;
  }

  void setMaxCapacity(int maxCapacity) {
    this.maxCapacity = maxCapacity;
  }

  void update(TaskSchedulingContext tc) {
    this.numSlotsOccupied += tc.numSlotsOccupied;
    this.numRunningTasks += tc.numRunningTasks;
    //this.maxTaskLimit += tc.maxTaskLimit;
    updateNoOfSlotsOccupiedByUser(tc.numSlotsOccupiedByUser);
  }

  private void updateNoOfSlotsOccupiedByUser(Map<String, Integer> nou) {
    Set<String> keys = nou.keySet();
    for (String key : keys) {
      if (this.numSlotsOccupiedByUser.containsKey(key)) {
        int currentVal = this.numSlotsOccupiedByUser.get(key);
        this.numSlotsOccupiedByUser.put(key, currentVal + nou.get(key));
      }
    }
  }
}