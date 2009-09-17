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
 * Event to record Failed and Killed completion of jobs
 *
 */
public class JobUnsuccessfulCompletionEvent implements HistoryEvent {

  private EventCategory category;
  private JobID jobid;
  private  long finishTime;
  private  int finishedMaps;
  private  int finishedReduces;
  private  String jobStatus;

  enum EventFields { EVENT_CATEGORY,
    JOB_ID,
    FINISH_TIME,
    FINISHED_MAPS,
    FINISHED_REDUCES,
    JOB_STATUS }

  /**
   * Create an event to record unsuccessful completion (killed/failed) of jobs
   * @param id Job ID
   * @param finishTime Finish time of the job
   * @param finishedMaps Number of finished maps
   * @param finishedReduces Number of finished reduces
   * @param status Status of the job
   */
  public JobUnsuccessfulCompletionEvent(JobID id, long finishTime,
      int finishedMaps,
      int finishedReduces, String status) {
    this.jobid = id;
    this.finishTime = finishTime;
    this.finishedMaps = finishedMaps;
    this.finishedReduces = finishedReduces;
    this.jobStatus = status;
    this.category = EventCategory.JOB;
  }

  JobUnsuccessfulCompletionEvent() {
  }

  /** Get the Job ID */
  public JobID getJobId() { return jobid; }
  /** Get the job finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the number of finished maps */
  public int getFinishedMaps() { return finishedMaps; }
  /** Get the number of finished reduces */
  public int getFinishedReduces() { return finishedReduces; }
  /** Get the event category */
  public EventCategory getEventCategory() { return category; }
  /** Get the status */
  public String getStatus() { return jobStatus; }
  /** Get the event type */
  public EventType getEventType() {
    if ("FAILED".equals(jobStatus)) {
      return EventType.JOB_FAILED;
    } else
      return EventType.JOB_KILLED;
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
      case FINISH_TIME:
        finishTime = jp.getLongValue();
        break;
      case FINISHED_MAPS:
        finishedMaps = jp.getIntValue();
        break;
      case FINISHED_REDUCES:
        finishedReduces =  jp.getIntValue();
        break;
      case JOB_STATUS:
        jobStatus = jp.getText();
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
    gen.writeNumberField(EventFields.FINISH_TIME.toString(), finishTime);
    gen.writeNumberField(EventFields.FINISHED_MAPS.toString(), finishedMaps);
    gen.writeNumberField(EventFields.FINISHED_REDUCES.toString(),
        finishedReduces);
    gen.writeStringField(EventFields.JOB_STATUS.toString(), jobStatus);
    gen.writeEndObject();
  }
}
