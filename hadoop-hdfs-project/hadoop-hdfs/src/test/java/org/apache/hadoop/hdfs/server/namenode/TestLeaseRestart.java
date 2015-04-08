package org.apache.hadoop.hdfs.server.namenode;
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
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;

import static org.junit.Assert.assertTrue;

public class TestLeaseRestart {

  Configuration getConf(){
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, /*default 15*/ 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY, /*default 10*/ 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY, /*default 500*/ 500);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY, /*default 15000*/1000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY, /*default 0*/ 0);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            /*default 0*/0);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, /*default
    45*/ 2);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, /*default 10*/ 1);
    conf.set(HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY,"1000,2");

    return conf;
  }

  /*
  After cluster failure, we ended up in a situation where for some
  leases there were no corresponding inodes. Which resulted in an NPE
  and the NN failed to start. We have yet to find the cause of orphan
  leases. This JIRA only fixed the NPE symptom of the cause.
   */
  @Test
  public void testMissingInodeLease() throws Exception {
    final int NN_COUNT=1;
    final String fileName="testFile";
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(getConf())
            .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
            .numDataNodes(1).format(true).build();

    cluster.setLeasePeriod(3*1000, 5*1000);
    Random rand = new Random(System.currentTimeMillis());
    DistributedFileSystem fs = cluster.getFileSystem(rand.nextInt(NN_COUNT));
    FSDataOutputStream out = fs.create(new Path("/"+fileName));
    try {
      out.write(1);
      out.hflush();
      cluster.shutdownNameNodes();
      fs.getClient().getLeaseRenewer().interruptAndJoin();
      IOUtils.closeStream(out);
      fs.getClient().close();

      new LightWeightRequestHandler(HDFSOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
          da.deleteInode(fileName);
          return null;
        }
      }.handle();

      cluster.restartNameNodes();
      cluster.setLeasePeriod(3*1000, 5*1000);

      Thread.sleep(10000);

      assertTrue("The lease should eventually be removed by the lease manager",
              cluster.getNamesystem().getLeaseManager().getNumSortedLeases()==0);

      // remove the inode for the file
    } catch (IOException e ){
      e.printStackTrace();
    } finally {
      cluster.shutdown();
    }
  }

}

