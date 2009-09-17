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
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Event to record updates to a task
 *
 */
public class TaskUpdatedEvent implements HistoryEvent {

  private EventCategory category;
  private TaskID taskid;
  private  long finishTime;

  enum EventFields { EVENT_CATEGORY,
                     TASK_ID,
                     FINISH_TIME }

  /**
   * Create an event to record task updates
   * @param id Id of the task
   * @param finishTime Finish time of the task
   */
  public TaskUpdatedEvent(TaskID id, long finishTime) {
    this.taskid = id;
    this.finishTime = finishTime;
    this.category = EventCategory.TASK;
  }

  TaskUpdatedEvent() {
  }
  /** Get the task ID */
  public TaskID getTaskId() { return taskid; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the task finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.TASK_UPDATED;
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
      case FINISH_TIME:
        finishTime = jp.getLongValue();
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
    gen.writeNumberField(EventFields.FINISH_TIME.toString(), finishTime);
    gen.writeEndObject();
  }
}
