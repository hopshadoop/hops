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
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.jobhistory.EventReader.CounterFields;
import org.apache.hadoop.mapreduce.jobhistory.EventReader.GroupFields;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * Event Writer is an utility class used to write events to the underlying
 * stream. Typically, one event writer (which translates to one stream) 
 * is created per job 
 * 
 */
class EventWriter {

  static final JsonFactory FACTORY = new JsonFactory();
  private JsonGenerator gen; 
  
  EventWriter(FSDataOutputStream out) throws IOException {
    gen = FACTORY.createJsonGenerator(out, JsonEncoding.UTF8);
    // Prefix all log files with the version
    writeVersionInfo();
  }
  
  private void writeVersionInfo() throws IOException {
    gen.writeStartObject();
    gen.writeStringField("HISTORY_VERSION", JobHistory.HISTORY_VERSION);
    gen.writeEndObject();
    gen.writeRaw("\n");
  }
  
  synchronized void write(HistoryEvent event)
  throws IOException { 
    writeEventType(gen, event.getEventType());
    event.writeFields(gen);
    gen.writeRaw("\n");
  }
  
  void flush() throws IOException { 
    gen.flush();
  }

  void close() throws IOException {
    gen.close();
  }
  
  /**
   * Write the event type to the JsonGenerator
   * @param gen
   * @param type
   * @throws IOException
   */
  private void writeEventType(JsonGenerator gen, EventType type) 
  throws IOException {
    gen.writeStartObject();
    gen.writeStringField("EVENT_TYPE", type.toString());
    gen.writeEndObject();
  }  
  
  static void writeCounters(Counters counters, JsonGenerator gen)
  throws IOException {
    writeCounters("COUNTERS", counters, gen);
  }
  
  static void writeCounters(String name, Counters counters, JsonGenerator gen)
  throws IOException {
    gen.writeFieldName(name);
    gen.writeStartArray(); // Start of all groups
    Iterator<CounterGroup> groupItr = counters.iterator();
    while (groupItr.hasNext()) {
      writeOneGroup(gen, groupItr.next());
    }
    gen.writeEndArray(); // End of all groups
  }
  
  static void writeOneGroup (JsonGenerator gen, CounterGroup grp)
  throws IOException {
    gen.writeStartObject(); // Start of this group
    gen.writeStringField(GroupFields.ID.toString(), grp.getName());
    gen.writeStringField(GroupFields.NAME.toString(), grp.getDisplayName());
  
    // Write out the List of counters
    gen.writeFieldName(GroupFields.LIST.toString());
    gen.writeStartArray(); // Start array of counters
    Iterator<Counter> ctrItr = grp.iterator();
    while (ctrItr.hasNext()) {
      writeOneCounter(gen, ctrItr.next());
    }
    gen.writeEndArray(); // End of all counters

    gen.writeEndObject(); // End of this group
  }

  static void writeOneCounter(JsonGenerator gen, Counter ctr)
  throws IOException{
    gen.writeStartObject();
    gen.writeStringField(CounterFields.ID.toString(), ctr.getName());
    gen.writeStringField(CounterFields.NAME.toString(), ctr.getDisplayName());
    gen.writeNumberField("VALUE", ctr.getValue());
    gen.writeEndObject();
  }
}
