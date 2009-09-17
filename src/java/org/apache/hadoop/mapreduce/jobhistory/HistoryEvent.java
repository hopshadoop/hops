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

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;

/**
 * The interface all job history events implement
 *
 */
public interface HistoryEvent {

  // The category that history event belongs to
  enum EventCategory {
    JOB, TASK, TASK_ATTEMPT
  }
  
  /**
   * Serialize the Fields of the event to the JsonGenerator
   * @param gen JsonGenerator to write to
   * @throws IOException
   */
  void writeFields (JsonGenerator gen) throws IOException;
  
  /**
   * Deserialize the fields of the event from the JsonParser
   * @param parser JsonParser to read from
   * @throws IOException
   */
  void readFields(JsonParser parser) throws IOException;
  
  /** Return the event type */
  EventType getEventType();
  
  /** Retun the event category */
  EventCategory getEventCategory();
}
