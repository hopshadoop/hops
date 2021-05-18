/*
 * Copyright (C) 2021 Logical Clocks AB
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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.log4j.Level;
import org.junit.Test;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;

public class TestMultiNNURI extends junit.framework.TestCase {

  static final Log LOG = LogFactory.getLog(TestMultiNNURI.class);
  @Test
  public void testFailover() throws IOException {
    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getLogger(TestMultiNNURI.class).setLevel(Level.ALL);

    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    int NUM_NAMENODES = 2;
    int NUM_DATANODES = 1;
    final int NN0 = 0, NN1 = 1;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES))
          .numDataNodes(NUM_DATANODES).build();
      cluster.waitActive();
      LOG.info("Started Cluster");

      String file = "/file";
      DistributedFileSystem dfs0 = (DistributedFileSystem) cluster.getNewFileSystemInstance(NN0);
      DistributedFileSystem dfs1 = (DistributedFileSystem) cluster.getNewFileSystemInstance(NN1);
      dfs0.create(new Path(file)).close();

      LOG.info(DFSConfigKeys.DFS_NAMENODES_RPC_ADDRESS_KEY+" : "+
              conf.get(DFSConfigKeys.DFS_NAMENODES_RPC_ADDRESS_KEY,
                      DFSConfigKeys.DFS_NAMENODES_RPC_ADDRESS_DEFAULT));
      String fileNN0URI = cluster.getFileSystem(NN0).getUri()+file;
      String fileNN1URI = cluster.getFileSystem(NN1).getUri()+file;

      LOG.info("NN0 URI "+fileNN0URI);
      LOG.info("NN1 URI "+fileNN1URI);

      try {
        dfs0.getFileStatus(new Path(file));
        LOG.info("Reading file using dfs0 works");

        dfs1.getFileStatus(new Path(file));
        LOG.info("Reading file using dfs1 works");

        dfs0.getFileStatus(new Path(fileNN0URI));
        LOG.info("Reading fileNN0URI using dfs0 works");

        dfs1.getFileStatus(new Path(fileNN1URI));
        LOG.info("Reading fileNN1URI using dfs1 works");

        dfs0.getFileStatus(new Path(fileNN1URI));
        LOG.info("Reading fileNN1URI using dfs0 works");

        dfs1.getFileStatus(new Path(fileNN0URI));
        LOG.info("Reading fileNN0URI using dfs1 works");

      } catch (IllegalArgumentException e) {
        e.printStackTrace();
        fail();
      }

      try{
        String wrongURI = "hdfs://localhost:9999"+file;
        dfs0.getFileStatus(new Path(wrongURI));
        fail(); // this should have failed
      } catch (IllegalArgumentException e){
      }


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}