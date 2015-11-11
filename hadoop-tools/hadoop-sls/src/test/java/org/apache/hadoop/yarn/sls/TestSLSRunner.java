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

package org.apache.hadoop.yarn.sls;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.YarnAPIStorageFactory;

import java.io.File;
import java.util.UUID;

public class TestSLSRunner {

  @Test
  @SuppressWarnings("all")
  public void testSimulatorRunning() throws Exception {
    Configuration conf = new Configuration();
    HdfsStorageFactory.setConfiguration(conf);
    RMStorageFactory.setConfiguration(conf);
    YarnAPIStorageFactory.setConfiguration(conf);

    File tempDir = new File("target", UUID.randomUUID().toString());

    // start the simulator
    File slsOutputDir = new File(tempDir.getAbsolutePath() + "/slsoutput/");
    String args[] = new String[]{
            "-inputsls", "src/test/resources/sls-jobs.json",
            "-output", slsOutputDir.getAbsolutePath(),
            "-nodes", "src/test/resources/sls-nodes.json"};
    SLSRunner.main(args);

    // wait for 45 seconds before stop
    Thread.sleep(45 * 1000);
    SLSRunner.getRunner().stop();
  }

}
