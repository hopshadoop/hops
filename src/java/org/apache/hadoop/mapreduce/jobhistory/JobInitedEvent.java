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
 * Event to record the initialization of a job
 *
 */
public class JobInitedEvent implements HistoryEvent {

  private EventCategory category;
  private JobID jobid;
  private  long launchTime;
  private  int totalMaps;
  private  int totalReduces;
  private  String jobStatus;

  enum EventFields { EVENT_CATEGORY,
                     JOB_ID,
                     LAUNCH_TIME,
                     TOTAL_MAPS,
                     TOTAL_REDUCES,
                     JOB_STATUS }
/**
 * Create an event to record job initialization
 * @param id
 * @param launchTime
 * @param totalMaps
 * @param totalReduces
 * @param jobStatus
 */
public JobInitedEvent(JobID id, long launchTime, int totalMaps,
      int totalReduces, String jobStatus) {
    this.jobid = id;
    this.launchTime = launchTime;
    this.totalMaps = totalMaps;
    this.totalReduces = totalReduces;
    this.jobStatus = jobStatus;
    this.category = EventCategory.JOB;
  }

  JobInitedEvent() { }

  /** Get the job ID */
  public JobID getJobId() { return jobid; }
  /** Get the launch time */
  public long getLaunchTime() { return launchTime; }
  /** Get the total number of maps */
  public int getTotalMaps() { return totalMaps; }
  /** Get the total number of reduces */
  public int getTotalReduces() { return totalReduces; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the status */
  public String getStatus() { return jobStatus; }
 /** Get the event type */
  public EventType getEventType() {
    return EventType.JOB_INITED;
  }

  public void readFields(JsonParser jp) throws IOException {
    if (jp.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Unexpected Token while reading, +" +
      		" expected a Start Object");
    }
    
    while (jp.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = jp.getCurrentName();
      jp.nextToken(); // Move to the value
      switch (Enum.valueOf(EventFields.class, fieldName)) {
        case EVENT_CATEGORY: 
          category = Enum.valueOf(EventCategory.class, jp.getText());
          break;
        case JOB_ID: jobid = JobID.forName(jp.getText()); break;
        case LAUNCH_TIME: launchTime = jp.getLongValue(); break;
        case TOTAL_MAPS: totalMaps = jp.getIntValue(); break;
        case TOTAL_REDUCES: totalReduces =  jp.getIntValue(); break;
        case JOB_STATUS: jobStatus = jp.getText(); break;
        default: 
        throw new IOException("Unrecognized field '"+ fieldName + "'!");
      }
    }
  }

  public void writeFields(JsonGenerator gen) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(EventFields.EVENT_CATEGORY.toString(),
        category.toString());
    gen.writeStringField(EventFields.JOB_ID.toString(), jobid.toString());
    gen.writeNumberField(EventFields.LAUNCH_TIME.toString(), launchTime);
    gen.writeNumberField(EventFields.TOTAL_MAPS.toString(), totalMaps);
    gen.writeNumberField(EventFields.TOTAL_REDUCES.toString(), totalReduces);
    gen.writeStringField(EventFields.JOB_STATUS.toString(),
        jobStatus);
    gen.writeEndObject();
  }
}
