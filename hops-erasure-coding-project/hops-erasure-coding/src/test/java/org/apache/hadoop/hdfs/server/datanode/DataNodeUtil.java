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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.IOException;
import java.util.List;

public class DataNodeUtil {

  public static void loseBlock(MiniDFSCluster cluster, LocatedBlock lb)
      throws IOException {
    List<DataNode> dataNodes = cluster.getDataNodes();
    String blockPoolId = lb.getBlock().getBlockPoolId();
    Block block = lb.getBlock().getLocalBlock();
    Block[] toDelete = new Block[]{block};
    for (DataNode dataNode : dataNodes) {
      try {
        dataNode.data.invalidate(blockPoolId, toDelete);
      } catch (IOException e) {
        // Thrown because not all DataNodes have all blocks
      }
    }
    waitForDelete(cluster, lb);
    assert (0 == cluster.getAllBlockFiles(lb.getBlock()).length);
  }

  public static void waitForDelete(MiniDFSCluster cluster, LocatedBlock lb) {
    // Deleting of blocks is asynchronous. Bussy waiting is my best approach for now
    while (cluster.getAllBlockFiles(lb.getBlock()).length > 0) {
      ;
    }
  }
}
