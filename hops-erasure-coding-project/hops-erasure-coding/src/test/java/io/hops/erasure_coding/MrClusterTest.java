/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;

public abstract class MrClusterTest extends ClusterTest {
  protected MiniMRYarnCluster mrCluster;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Configuration conf = new Configuration(getConfig());
    mrCluster = new MiniMRYarnCluster(this.getClass().getName(), numDatanode);
    conf.set("fs.defaultFS", fs.getUri().toString());
    mrCluster.init(conf);
    mrCluster.start();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (mrCluster != null) {
      mrCluster.stop();
    }
  }
}
