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
import org.apache.hadoop.mapreduce.JobID;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
/**
 * Event to record successful completion of job
 *
 */
public class JobFinishedEvent  implements HistoryEvent {

  private EventCategory category;
  private JobID jobid;
  private long finishTime;
  private int finishedMaps;
  private int finishedReduces;
  private int failedMaps;
  private int failedReduces;
  private Counters totalCounters;
  private Counters mapCounters;
  private Counters reduceCounters;

  enum EventFields { EVENT_CATEGORY,
    JOB_ID,
    FINISH_TIME,
    FINISHED_MAPS,
    FINISHED_REDUCES,
    FAILED_MAPS,
    FAILED_REDUCES,
    MAP_COUNTERS,
    REDUCE_COUNTERS,
    TOTAL_COUNTERS }

  JobFinishedEvent() {
  }

  /** 
   * Create an event to record successful job completion
   * @param id Job ID
   * @param finishTime Finish time of the job
   * @param finishedMaps The number of finished maps
   * @param finishedReduces The number of finished reduces
   * @param failedMaps The number of failed maps
   * @param failedReduces The number of failed reduces
   * @param mapCounters Map Counters for the job
   * @param reduceCounters Reduce Counters for the job
   * @param totalCounters Total Counters for the job
   */
  public JobFinishedEvent(JobID id, long finishTime,
      int finishedMaps, int finishedReduces,
      int failedMaps, int failedReduces,
      Counters mapCounters, Counters reduceCounters,
      Counters totalCounters) {
    this.jobid = id;
    this.finishTime = finishTime;
    this.finishedMaps = finishedMaps;
    this.finishedReduces = finishedReduces;
    this.failedMaps = failedMaps;
    this.failedReduces = failedReduces;
    this.mapCounters = mapCounters;
    this.reduceCounters = reduceCounters;
    this.totalCounters = totalCounters;
    this.category = EventCategory.JOB;
  }

  /** Get the Event Category */
  public EventCategory getEventCategory() { return category; }
  /** Get the Job ID */
  public JobID getJobid() { return jobid; }
  /** Get the job finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the number of finished maps for the job */
  public int getFinishedMaps() { return finishedMaps; }
  /** Get the number of finished reducers for the job */
  public int getFinishedReduces() { return finishedReduces; }
  /** Get the number of failed maps for the job */
  public int getFailedMaps() { return failedMaps; }
  /** Get the number of failed reducers for the job */
  public int getFailedReduces() { return failedReduces; }
  /** Get the counters for the job */
  public Counters getTotalCounters() { return totalCounters; }
  /** Get the Map counters for the job */
  public Counters getMapCounters() { return mapCounters; }
  /** Get the reduce counters for the job */
  public Counters getReduceCounters() { return reduceCounters; }
  /** Get the event type */
  public EventType getEventType() { 
    return EventType.JOB_FINISHED;
  }

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
        finishedReduces = jp.getIntValue();
        break;
      case FAILED_MAPS:
        failedMaps = jp.getIntValue();
        break;
      case FAILED_REDUCES:
        failedReduces = jp.getIntValue();
        break;
      case MAP_COUNTERS:
        mapCounters = EventReader.readCounters(jp);
        break;
      case REDUCE_COUNTERS:
        reduceCounters = EventReader.readCounters(jp);
        break;
      case TOTAL_COUNTERS:
        totalCounters = EventReader.readCounters(jp);
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
    gen.writeStringField(EventFields.JOB_ID.toString(), jobid.toString());
    gen.writeNumberField(EventFields.FINISH_TIME.toString(), finishTime);
    gen.writeNumberField(EventFields.FINISHED_MAPS.toString(), finishedMaps);
    gen.writeNumberField(EventFields.FINISHED_REDUCES.toString(), 
        finishedReduces);
    gen.writeNumberField(EventFields.FAILED_MAPS.toString(), failedMaps);
    gen.writeNumberField(EventFields.FAILED_REDUCES.toString(),
        failedReduces);
    EventWriter.writeCounters(EventFields.MAP_COUNTERS.toString(),
        mapCounters, gen);
    EventWriter.writeCounters(EventFields.REDUCE_COUNTERS.toString(),
        reduceCounters, gen);
    EventWriter.writeCounters(EventFields.TOTAL_COUNTERS.toString(),
        totalCounters, gen);
    gen.writeEndObject();
  }
}
