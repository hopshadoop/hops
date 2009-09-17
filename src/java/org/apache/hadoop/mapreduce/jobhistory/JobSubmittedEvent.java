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

import org.apache.hadoop.mapreduce.JobID;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Event to record the submission of a job
 *
 */
public class JobSubmittedEvent implements HistoryEvent {

  private EventCategory category;
  private JobID jobid;
  private  String jobName;
  private  String userName;
  private  long submitTime;
  private  String jobConfPath;

  enum EventFields { EVENT_CATEGORY,
                     JOB_ID,
                     JOB_NAME,
                     USER_NAME,
                     SUBMIT_TIME,
                     JOB_CONF_PATH }

  /**
   * Create an event to record job submission
   * @param id The job Id of the job
   * @param jobName Name of the job
   * @param userName Name of the user who submitted the job
   * @param submitTime Time of submission
   * @param jobConfPath Path of the Job Configuration file
   */
  public JobSubmittedEvent(JobID id, String jobName, String userName,
      long submitTime, String jobConfPath) {
    this.jobid = id;
    this.jobName = jobName;
    this.userName = userName;
    this.submitTime = submitTime;
    this.jobConfPath = jobConfPath;
    this.category = EventCategory.JOB;
  }

  JobSubmittedEvent() {
  }

  /** Get the Job Id */
  public JobID getJobId() { return jobid; }
  /** Get the Job name */
  public String getJobName() { return jobName; }
  /** Get the user name */
  public String getUserName() { return userName; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the submit time */
  public long getSubmitTime() { return submitTime; }
  /** Get the Path for the Job Configuration file */
  public String getJobConfPath() { return jobConfPath; }
  /** Get the event type */
  public EventType getEventType() { return EventType.JOB_SUBMITTED; }

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
      case JOB_ID: jobid = JobID.forName(jp.getText()); break;
      case JOB_NAME: jobName = jp.getText(); break;
      case USER_NAME: userName = jp.getText(); break;
      case SUBMIT_TIME: submitTime = (long) jp.getLongValue(); break;
      case JOB_CONF_PATH: jobConfPath = jp.getText(); break;
      default: 
        throw new IOException("Unrecognized field '"+fieldName+"'!");
      }
    }
  }

  public void writeFields(JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(EventFields.EVENT_CATEGORY.toString(),
        category.toString());
    gen.writeStringField(EventFields.JOB_ID.toString(), jobid.toString());
    gen.writeStringField(EventFields.JOB_NAME.toString(), jobName);
    gen.writeStringField(EventFields.USER_NAME.toString(), userName);
    gen.writeNumberField(EventFields.SUBMIT_TIME.toString(), submitTime);
    gen.writeStringField(EventFields.JOB_CONF_PATH.toString(), jobConfPath);
    gen.writeEndObject();
  }
}
