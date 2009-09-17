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
 * Event to record unsuccessful (Killed/Failed) completion of task attempts
 *
 */
public class TaskAttemptUnsuccessfulCompletionEvent implements HistoryEvent {

  private EventCategory category;
  private TaskID taskid;
  private TaskType taskType;
  private TaskAttemptID attemptId;
  private  long finishTime;
  private String hostname;
  private String status;
  private  String error;

  enum EventFields { EVENT_CATEGORY,
    TASK_ID,
    TASK_TYPE,
    TASK_ATTEMPT_ID,
    FINISH_TIME,
    HOSTNAME,
    STATUS,
    ERROR }

  /** 
   * Create an event to record the unsuccessful completion of attempts
   * @param id Attempt ID
   * @param taskType Type of the task
   * @param status Status of the attempt
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param error Error string
   */
  public TaskAttemptUnsuccessfulCompletionEvent(TaskAttemptID id, 
      TaskType taskType,
      String status, long finishTime, 
      String hostname, String error) {
    this.taskid = id.getTaskID();
    this.taskType = taskType;
    this.attemptId = id;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.error = error;
    this.status = status;
    this.category = EventCategory.TASK_ATTEMPT;
  }

  TaskAttemptUnsuccessfulCompletionEvent() {
  }

  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the task id */
  public TaskID getTaskId() { return taskid; }
  /** Get the task type */
  public TaskType getTaskType() { return taskType; }
  /** Get the attempt id */
  public TaskAttemptID getTaskAttemptId() { return attemptId; }
  /** Get the finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the name of the host where the attempt executed */
  public String getHostname() { return hostname; }
  /** Get the error string */
  public String getError() { return error; }
  /** Get the task status */
  public String getTaskStatus() { return status; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.MAP_ATTEMPT_KILLED;
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
      case TASK_ATTEMPT_ID: 
        attemptId = TaskAttemptID.forName(jp.getText());
        break;
      case FINISH_TIME:
        finishTime = jp.getLongValue();
        break;
      case HOSTNAME:
        hostname = jp.getText();
        break;
      case ERROR:
        error = jp.getText();
        break;
      case STATUS:
        status = jp.getText();
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
    gen.writeStringField(EventFields.TASK_ATTEMPT_ID.toString(),
        attemptId.toString());
    gen.writeNumberField(EventFields.FINISH_TIME.toString(), finishTime);
    gen.writeStringField(EventFields.HOSTNAME.toString(), hostname);
    gen.writeStringField(EventFields.ERROR.toString(), error);
    gen.writeStringField(EventFields.STATUS.toString(), status);
    gen.writeEndObject();
  }
}
