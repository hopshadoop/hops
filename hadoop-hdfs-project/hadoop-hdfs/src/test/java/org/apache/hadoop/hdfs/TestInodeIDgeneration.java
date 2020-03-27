/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the cd Apache License, Version 2.0 (the
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
package org.apache.hadoop.hdfs;

import io.hops.common.IDsGenerator;
import io.hops.common.IDsGeneratorFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * This class tests various cases during file creation.
 */
public class TestInodeIDgeneration {

  private static final Log LOG =
          LogFactory.getLog(TestInodeIDgeneration.class);
  {
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger(TestInodeIDgeneration.class).setLevel(Level.INFO);
    Logger.getLogger(IDsGenerator.class).setLevel(Level.INFO);
  }

  @Test
  public void testRenameUnderReplicatedFile() throws Exception {

    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_INODEID_BATCH_SIZE, 10);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
            .newInstance(fs.getUri(), fs.getConf());
    try {
      for(int i = 0; i < 100; i++){
        LOG.info("InodeID: "+IDsGeneratorFactory.getInstance().getUniqueINodeID());
      }


    } finally {
      cluster.shutdown();
    }


  }
}

