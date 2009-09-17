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
 * Event to record changes in the submit and launch time of
 * a job
 */
public class JobInfoChangeEvent implements HistoryEvent {

  private EventCategory category;
  private JobID jobid;
  private  long submitTime;
  private  long launchTime;

  enum EventFields { EVENT_CATEGORY,
                     JOB_ID,
                     SUBMIT_TIME,
                     LAUNCH_TIME }

  /** 
   * Create a event to record the submit and launch time of a job
   * @param id Job Id 
   * @param submitTime Submit time of the job
   * @param launchTime Launch time of the job
   */
  public JobInfoChangeEvent(JobID id, long submitTime, long launchTime) {
    this.jobid = id;
    this.submitTime = submitTime;
    this.launchTime = launchTime;
    this.category = EventCategory.JOB;
  }

  JobInfoChangeEvent() { }

  /** Get the Job ID */
  public JobID getJobId() { return jobid; }
  /** Get the Job submit time */
  public long getSubmitTime() { return submitTime; }
  /** Get the Job launch time */
  public long getLaunchTime() { return launchTime; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.JOB_INFO_CHANGED;
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
        case JOB_ID:
          jobid = JobID.forName(jp.getText());
          break;
        case SUBMIT_TIME:
          submitTime = jp.getLongValue();
          break;
        case LAUNCH_TIME:
          launchTime = jp.getLongValue();
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
    gen.writeStringField(EventFields.JOB_ID.toString(), jobid.toString());
    gen.writeNumberField(EventFields.SUBMIT_TIME.toString(), submitTime);
    gen.writeNumberField(EventFields.LAUNCH_TIME.toString(), launchTime);
    gen.writeEndObject();
  }
}
