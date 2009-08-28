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
package org.apache.hadoop.tools.rumen;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig;

/**
 * {@link Parser} is an interface to the underlining JSON files. To use the
 * parser, create a Parser object, and Parser.getNextJob() will return next job
 * in the trace every time it is called.
 */
public class Parser implements Closeable {
  private final JsonParser parser;
  private final ObjectMapper mapper;
  private LoggedNetworkTopology topology;

  // DEBUG
  public Parser() {
    parser = null;
    mapper = null;
  }

  public Parser(java.io.Reader digest) throws IOException {
    mapper = new ObjectMapper();
    mapper.configure(
        DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    parser = mapper.getJsonFactory().createJsonParser(digest);
  }

  public Parser(InputStream stream) throws JsonParseException, IOException {
    mapper = new ObjectMapper();
    mapper.configure(
        DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    parser = mapper.getJsonFactory().createJsonParser(stream);
  }

  public JobStory getNextJob() throws IOException {
    try {
      final LoggedJob job = mapper.readValue(parser, LoggedJob.class);
      return null == job ? null : new ZombieJob(job, topology);
    } catch (EOFException e) {
      return null;
    } catch (JsonProcessingException e) {
      throw new IOException(e);
    }
    // System.out.println(job.getJobID() + ": user-" + job.getUser() + ": "
    // + job.getMapTasks().size() + "-"
    // + job.getReduceTasks().size() + "-"
    // + job.getOtherTasks().size() + ", @ "
    // + (new Date(job.getLaunchTime())).toString());
  }

  public LoggedNetworkTopology readTopology(File topologyFile)
      throws JsonParseException, JsonMappingException, IOException {
    this.topology = mapper.readValue(topologyFile, LoggedNetworkTopology.class);
    return this.topology;
  }

  public LoggedNetworkTopology readTopology(InputStream stream)
      throws JsonParseException, JsonMappingException, IOException {
    this.topology = mapper.readValue(stream, LoggedNetworkTopology.class);
    return this.topology;
  }

  public void close() throws IOException {
    parser.close();
  }
}
