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
package org.apache.hadoop.hdfs.server.namenode.ha;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.base.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Static utility functions useful for testing HA.
 */
public abstract class HATestUtil {
  private static Log LOG = LogFactory.getLog(HATestUtil.class);
  
  private static final String LOGICAL_HOSTNAME = "ha-nn-uri-%d";
  

  /**
   * Wait for the datanodes in the cluster to process any block
   * deletions that have already been asynchronously queued.
   */
  public static void waitForDNDeletions(final MiniDFSCluster cluster)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        for (DataNode dn : cluster.getDataNodes()) {
          if (DataNodeTestUtils.getPendingAsyncDeletions(dn) > 0) {
            return false;
          }
        }
        return true;
      }
    }, 1000, 10000);
    
  }

  /**
   * Wait for the NameNode to issue any deletions that are already
   * pending (i.e. for the pendingDeletionBlocksCount to go to 0)
   */
  public static void waitForNNToIssueDeletions(final NameNode nn)
      throws Exception {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        LOG.info("Waiting for NN to issue block deletions to DNs");
        try {
          return nn.getNamesystem().getBlockManager().getPendingDeletionBlocksCount() == 0;
        } catch (IOException ex) {
          Logger.getLogger(HATestUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
      }
    }, 250, 10000);
  }

  public static String getLogicalHostname(MiniDFSCluster cluster) {
    return String.format(LOGICAL_HOSTNAME, cluster.getInstanceId());
  }
  
  public static URI getLogicalUri(MiniDFSCluster cluster)
      throws URISyntaxException {
    return new URI(HdfsConstants.HDFS_URI_SCHEME + "://" +
        getLogicalHostname(cluster));
  }
  
}
