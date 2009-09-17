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
 * Event to record start of a task attempt
 *
 */
public class TaskAttemptStartedEvent implements HistoryEvent {

  private EventCategory category;
  private TaskID taskid;
  private TaskType taskType;
  private TaskAttemptID attemptId;
  private  long startTime;
  private  String trackerName;
  private int httpPort;

  enum EventFields { EVENT_CATEGORY,
                     TASK_ID,
                     TASK_TYPE,
                     TASK_ATTEMPT_ID,
                     START_TIME,
                     TRACKER_NAME,
                     HTTP_PORT }

  /**
   * Create an event to record the start of an attempt
   * @param attemptId Id of the attempt
   * @param taskType Type of task
   * @param startTime Start time of the attempt
   * @param trackerName Name of the Task Tracker where attempt is running
   * @param httpPort The port number of the tracker
   */
  public TaskAttemptStartedEvent( TaskAttemptID attemptId,  
      TaskType taskType, long startTime, String trackerName,
      int httpPort) {
    this.attemptId = attemptId;
    this.taskid = attemptId.getTaskID();
    this.startTime = startTime;
    this.taskType = taskType;
    this.trackerName = trackerName;
    this.httpPort = httpPort;
    this.category = EventCategory.TASK_ATTEMPT;
  }

  TaskAttemptStartedEvent() {
  }

  /** Get the task id */
  public TaskID getTaskId() { return taskid; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the tracker name */
  public String getTrackerName() { return trackerName; }
  /** Get the start time */
  public long getStartTime() { return startTime; }
  /** Get the task type */
  public TaskType getTaskType() { return taskType; }
  /** Get the HTTP port */
  public int getHttpPort() { return httpPort; }
  /** Get the attempt id */
  public TaskAttemptID getTaskAttemptId() { return attemptId; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.MAP_ATTEMPT_STARTED;
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
      case TASK_ATTEMPT_ID: 
        attemptId = TaskAttemptID.forName(jp.getText());
        break;
      case TASK_TYPE:
        taskType = TaskType.valueOf(jp.getText());
        break;
      case START_TIME:
        startTime = jp.getLongValue();
        break;
      case TRACKER_NAME:
        trackerName = jp.getText();
        break;
      case HTTP_PORT:
        httpPort = jp.getIntValue();
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
    gen.writeStringField(EventFields.TASK_ATTEMPT_ID.toString(),
        attemptId.toString());
    gen.writeStringField(EventFields.TASK_TYPE.toString(),
        taskType.toString());
    gen.writeNumberField(EventFields.START_TIME.toString(), startTime);
    gen.writeStringField(EventFields.TRACKER_NAME.toString(), trackerName);
    gen.writeNumberField(EventFields.HTTP_PORT.toString(), httpPort);
    gen.writeEndObject();
  }
}
