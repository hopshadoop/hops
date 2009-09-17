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

import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Event to record the start of a task
 *
 */
public class TaskStartedEvent implements HistoryEvent {

  private EventCategory category;
  private TaskID taskid;
  private TaskType taskType;
  private  long startTime;
  private  String splitLocations;

  enum EventFields { EVENT_CATEGORY,
                     TASK_ID,
                     TASK_TYPE,
                     START_TIME,
                     SPLIT_LOCATIONS }

  /**
   * Create an event to record start of a task
   * @param id Task Id
   * @param startTime Start time of the task
   * @param taskType Type of the task
   * @param splitLocations Split locations, applicable for map tasks
   */
  public TaskStartedEvent(TaskID id, long startTime, 
      TaskType taskType, String splitLocations) {
    this.taskid = id;
    this.splitLocations = splitLocations;
    this.startTime = startTime;
    this.taskType = taskType;
    this.category = EventCategory.TASK;
  }

  TaskStartedEvent() {
  }

  /** Get the task id */
  public TaskID getTaskId() { return taskid; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the split locations, applicable for map tasks */
  public String getSplitLocations() { return splitLocations; }
  /** Get the start time of the task */
  public long getStartTime() { return startTime; }
  /** Get the task type */
  public TaskType getTaskType() { return taskType; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.TASK_STARTED;
  }

  public void readFields(JsonParser jp) throws IOException {
    if (jp.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Unexpected Token while reading");
    }
    
    while (jp.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = jp.getCurrentName();
      jp.nextToken(); // Move to the value
      switch (Enum.valueOf(EventFields.class, fieldName)) {
      case EVENT_CATEGORY:
        category = Enum.valueOf(EventCategory.class, jp.getText());
        break;
      case TASK_ID:
        taskid = TaskID.forName(jp.getText());
        break;
      case TASK_TYPE:
        taskType = TaskType.valueOf(jp.getText());
        break;
      case START_TIME:
        startTime = jp.getLongValue();
        break;
      case SPLIT_LOCATIONS:
        splitLocations = jp.getText();
        break;
      default: 
        throw new IOException("Unrecognized field '"+fieldName+"'!");
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
    gen.writeNumberField(EventFields.START_TIME.toString(), startTime);
    gen.writeStringField(EventFields.SPLIT_LOCATIONS.toString(),
        splitLocations);
    gen.writeEndObject();
  }
}
