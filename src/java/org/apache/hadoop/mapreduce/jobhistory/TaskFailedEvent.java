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

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Event to record the failure of a task
 *
 */
public class TaskFailedEvent implements HistoryEvent {

  private EventCategory category;
  private TaskID taskid;
  private TaskType taskType;
  private  long finishTime;
  private  String error;
  private TaskAttemptID failedDueToAttempt;
  private String status;

  enum EventFields { EVENT_CATEGORY,
                     TASK_ID,
                     TASK_TYPE,
                     FINISH_TIME,
                     ERROR,
                     STATUS,
                     FAILED_ATTEMPT_ID }

  /**
   * Create an event to record task failure
   * @param id Task ID
   * @param finishTime Finish time of the task
   * @param taskType Type of the task
   * @param error Error String
   * @param status Status
   * @param failedDueToAttempt The attempt id due to which the task failed
   */
  public TaskFailedEvent(TaskID id, long finishTime, 
      TaskType taskType, String error, String status,
      TaskAttemptID failedDueToAttempt) {
    this.taskid = id;
    this.error = error;
    this.finishTime = finishTime;
    this.taskType = taskType;
    this.failedDueToAttempt = failedDueToAttempt;
    this.category = EventCategory.TASK;
    this.status = status;
  }

  TaskFailedEvent() {
  }

  /** Get the task id */
  public TaskID getTaskId() { return taskid; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the error string */
  public String getError() { return error; }
  /** Get the finish time of the attempt */
  public long getFinishTime() { return finishTime; }
  /** Get the task type */
  public TaskType getTaskType() { return taskType; }
  /** Get the attempt id due to which the task failed */
  public TaskAttemptID getFailedAttemptID() { return failedDueToAttempt; }
  /** Get the task status */
  public String getTaskStatus() { return status; }
  /** Get the event type */
  public EventType getEventType() { return EventType.TASK_FAILED; }

  
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
      case FINISH_TIME:
        finishTime = jp.getLongValue();
        break;
      case ERROR:
        error = jp.getText();
        break;
      case STATUS:
        status = jp.getText();
        break;
      case FAILED_ATTEMPT_ID:
        failedDueToAttempt = TaskAttemptID.forName(jp.getText());
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
    gen.writeNumberField(EventFields.FINISH_TIME.toString(), finishTime);
    gen.writeStringField(EventFields.ERROR.toString(), error);
    gen.writeStringField(EventFields.STATUS.toString(), status);
    if (failedDueToAttempt != null) {
      gen.writeStringField(EventFields.FAILED_ATTEMPT_ID.toString(),
          failedDueToAttempt.toString());
    }
    gen.writeEndObject();
  }
}
