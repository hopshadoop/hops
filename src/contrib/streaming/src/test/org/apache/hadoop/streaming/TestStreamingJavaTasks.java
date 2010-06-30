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

package org.apache.hadoop.streaming;

import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests stream job with java tasks(not commands; So task is the child process
 * of TaskTracker) in MapReduce local mode.
 * Validates if user-set config properties
 * {@link MRJobConfig#MAP_OUTPUT_KEY_CLASS} and
 * {@link MRJobConfig#OUTPUT_KEY_CLASS} are honored by streaming jobs for the
 * case of java mapper/reducer(mapper and reducer are not commands).
 */
public class TestStreamingJavaTasks extends TestStreaming {

  public TestStreamingJavaTasks() throws IOException {
    super();
    input = "one line dummy input\n";
    map = "org.apache.hadoop.mapred.lib.IdentityMapper";
  }

  @Override
  protected String[] genArgs() {
    args.clear();
    // set the testcase-specific config properties first and the remaining
    // arguments are set in TestStreaming.genArgs().
    args.add("-jobconf");
    args.add(MRJobConfig.MAP_OUTPUT_KEY_CLASS +
        "=org.apache.hadoop.io.LongWritable");
    args.add("-jobconf");
    args.add(MRJobConfig.OUTPUT_KEY_CLASS +
        "=org.apache.hadoop.io.LongWritable");

    // Using SequenceFileOutputFormat here because with TextOutputFormat, the
    // mapred.output.key.class set in JobConf (which we want to test here) is
    // not read/used at all.
    args.add("-outputformat");
    args.add("org.apache.hadoop.mapred.SequenceFileOutputFormat");

    return super.genArgs();
  }

  @Override
  protected void checkOutput() throws IOException {
    // No need to validate output for the test cases in this class
  }

  // Check with IdentityMapper, IdentityReducer
  @Override
  @Test
  public void testCommandLine() throws Exception {
    reduce = "org.apache.hadoop.mapred.lib.IdentityReducer";
    super.testCommandLine();
  }

  // Check the case of Reducer = "NONE"
  @Test
  public void testStreamingJavaTasksWithReduceNone() throws Exception {
    reduce = "NONE";
    super.testCommandLine();
  }
}
