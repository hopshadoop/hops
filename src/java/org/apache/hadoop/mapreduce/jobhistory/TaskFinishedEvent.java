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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Event to record the successful completion of a task
 *
 */
public class TaskFinishedEvent implements HistoryEvent {

  private EventCategory category;
  private TaskID taskid;
  private TaskType taskType;
  private long finishTime;
  private String status;
  private Counters counters;
  
  enum EventFields { EVENT_CATEGORY,
                     TASK_ID,
                     TASK_TYPE,
                     FINISH_TIME,
                     STATUS,
                     COUNTERS }
    
  TaskFinishedEvent() {
  }

  /**
   * Create an event to record the successful completion of a task
   * @param id Task ID
   * @param finishTime Finish time of the task
   * @param taskType Type of the task
   * @param status Status string
   * @param counters Counters for the task
   */
  public TaskFinishedEvent(TaskID id, long finishTime,
                           TaskType taskType,
                           String status, Counters counters) {
    this.taskid = id;
    this.finishTime = finishTime;
    this.counters = counters;
    this.taskType = taskType;
    this.status = status;
    this.category = EventCategory.TASK;
  }
  
  /** Get task id */
  public TaskID getTaskId() { return taskid; }
  /** Get the task finish time */
  public long getFinishTime() { return finishTime; }
  /** Get task counters */
  public Counters getCounters() { return counters; }
  /** Get task type */
  public TaskType getTaskType() { return taskType; }
  /** Get task status */
  public String getTaskStatus() { return status; }
  /** Get event type */
  public EventType getEventType() {
    return EventType.TASK_FINISHED;
  }
  /** Get Event Category */
  public EventCategory getEventCategory() { return category; }
  
  
  public void readFields(JsonParser jp) throws IOException {
    if (jp.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Unexpected token while reading");
    }
    
    while (jp.nextToken() != JsonToken.END_OBJECT) {
      String fieldname = jp.getCurrentName();
      jp.nextToken(); // move to value
      switch (Enum.valueOf(EventFields.class, fieldname)) {
      case EVENT_CATEGORY:
        category = Enum.valueOf(EventCategory.class, jp.getText());
        break;
      case TASK_ID:
        taskid = TaskID.forName(jp.getText());
        break;
      case TASK_TYPE:
        taskType = TaskType.valueOf(jp.getText());
        break;
      case FINISH_TIME:
        finishTime = jp.getLongValue();
        break;
      case STATUS:
        status = jp.getText();
        break;
      case COUNTERS:
        counters = EventReader.readCounters(jp);
        break;
      default: 
        throw new IOException("Unrecognized field '"+fieldname+"'!");
      }
    }
  }

  public void writeFields(JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(EventFields.EVENT_CATEGORY.toString(),
        category.toString());
    gen.writeStringField(EventFields.TASK_ID.toString(), taskid.toString());
    gen.writeStringField(EventFields.TASK_TYPE.toString(),
        taskType.toString());
    gen.writeNumberField(EventFields.FINISH_TIME.toString(), finishTime);
    gen.writeStringField(EventFields.STATUS.toString(), status);
    EventWriter.writeCounters(counters, gen);
    gen.writeEndObject();
  }
}
