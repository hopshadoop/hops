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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class EventReader {

  static final JsonFactory FACTORY = new JsonFactory();

  enum GroupFields { ID, NAME, LIST }
  enum CounterFields { ID, NAME, VALUE }

  private final JsonParser parser;
  private FSDataInputStream in;
  
  private String version = null;

  /**
   * Create a new Event Reader
   * @param fs
   * @param name
   * @throws IOException
   */
  public EventReader(FileSystem fs, Path name) throws IOException {
    this (fs.open(name));
  }

  /**
   * Create a new Event Reader
   * @param in
   * @throws IOException
   */
  public EventReader(FSDataInputStream in) throws IOException {
    this.in = in;
    parser = FACTORY.createJsonParser(in);
    readVersionInfo();
  }

  private void readVersionInfo() throws IOException {
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException("Unexpected Token while reading");
    }
    
    parser.nextToken(); // Key
    parser.nextToken(); // Value
    
    this.version = parser.getText();
    
    parser.nextToken(); // Consume the End Object
  }
  
  /**
   * Return the current history version
   */
  public String getHistoryVersion() { return version; }
  
  /**
   * Get the next event from the stream
   * @return the next event
   * @throws IOException
   */
  public HistoryEvent getNextEvent() throws IOException {
    EventType type = getHistoryEventType();

    if (type == null) {
      return null;
    }

    Class<? extends HistoryEvent> clazz = type.getKlass();

    if (clazz == null) {
      throw new IOException("CLass not known for " + type);
    }

    HistoryEvent ev = null;
    try {
      ev = clazz.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Error Instantiating new object");
    }

    ev.readFields(parser);
    return ev;
  }

  /**
   * Close the Event reader
   * @throws IOException
   */
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
    in = null;
  }

 
  /**
   * Read the next JSON Object  to identify the event type.
   * @param jp
   * @return EventType
   * @throws IOException
   */
  private EventType getHistoryEventType()
  throws IOException {

    if (parser.nextToken() == null) { // Verify the Start Object
      return null; 
    }

    parser.nextToken();// Get the Event type

    String fieldname = parser.getCurrentName();

    if (!"EVENT_TYPE".equals(fieldname)) {
      throw new IOException("Unexpected event type: " + fieldname);
    }

    parser.nextToken(); // Go to the value
    String type = parser.getText();

    parser.nextToken(); // Consume the end object

    return Enum.valueOf(EventType.class, type);
  }


  static Counters readCounters(JsonParser jp) throws IOException {
    Counters counters = new Counters();
    while (jp.nextToken() !=JsonToken.END_ARRAY) {
      readOneGroup(counters, jp);
    }
    return counters;
  }

  static void readOneGroup(Counters counters, JsonParser jp)
  throws IOException {

    jp.nextToken(); 

    String fieldname = jp.getCurrentName();

    if (!Enum.valueOf(GroupFields.class, fieldname).equals(GroupFields.ID)) {
      throw new IOException("Internal error");
    }
    
    jp.nextToken(); // Get the value
    
    CounterGroup grp = counters.getGroup(jp.getText());

    while (jp.nextToken() != JsonToken.END_OBJECT) {
      fieldname = jp.getCurrentName();
      jp.nextToken(); // move to value
      switch(Enum.valueOf(GroupFields.class, fieldname)) {
      case NAME: 
        break;
      case LIST: 
        while (jp.nextToken() != JsonToken.END_ARRAY) {
          readOneCounter(grp, jp);
        }
        break;
      default:
        throw new IOException("Unrecognized field '" + fieldname + "'!");
      }
    }    
  }

  static void readOneCounter(CounterGroup grp, JsonParser jp)
  throws IOException {
    String name = null;
    String displayName = null;
    long value = 0;
    
    while (jp.nextToken() != JsonToken.END_OBJECT) {
      String fieldname = jp.getCurrentName();
      jp.nextToken();
      switch (Enum.valueOf(CounterFields.class, fieldname)) {
      case ID: name = jp.getText(); break;
      case NAME: displayName = jp.getText(); break;
      case VALUE: value = jp.getLongValue(); break;
      default:
        throw new IOException("Unrecognized field '"+ fieldname + "'!");
      }
    }
    
    Counter ctr = grp.findCounter(name, displayName);
    ctr.increment(value);
  }

}
