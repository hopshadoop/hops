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
package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.exception.StorageException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * This interface is used for choosing the desired number of targets
 * for placing block replicas.
 */
@InterfaceAudience.Private
public abstract class BlockPlacementPolicy {
  static final Log LOG = LogFactory.getLog(BlockPlacementPolicy.class);

  @InterfaceAudience.Private
  public static class NotEnoughReplicasException extends Exception {
    private static final long serialVersionUID = 1L;

    NotEnoughReplicasException(String msg) {
      super(msg);
    }
  }

  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i>
   * to re-replicate a block with size <i>blocksize</i>
   * If not, return as many as we can.
   *
   * @param srcPath
   *     the file to which this chooseTargets is being invoked.
   * @param numOfReplicas
   *     additional number of replicas wanted.
   * @param writer
   *     the writer's machine, null if not in the cluster.
   * @param chosenStorages
   *     datanodes that have been chosen as targets.
   * @param blocksize
   *     size of the data to be written.
   * @return array of DatanodeStorageInfo instances chosen as target
   * and sorted as a pipeline.
   */
  abstract DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
      DatanodeStorageInfo writer, List<DatanodeStorageInfo> chosenStorages,
      long blocksize);

  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i>
   * to re-replicate a block with size <i>blocksize</i>
   * If not, return as many as we can.
   *
   * @param srcPath
   *     the file to which this chooseTargets is being invoked.
   * @param numOfReplicas
   *     additional number of replicas wanted.
   * @param writer
   *     the writer's machine, null if not in the cluster.
   * @param chosenStorages
   *     storages that have been chosen as targets.
   * @param returnChosenStorages
   *     decide if the chosenStorages are returned.
   * @param excludedStorages
   *     storages that should not be considered as targets.
   * @param blocksize
   *     size of the data to be written.
   * @return array of DatanodeStorageInfo instances chosen as target
   * and sorted as a pipeline.
   */
  public abstract DatanodeStorageInfo[] chooseTarget(String srcPath,
      int numOfReplicas, DatanodeStorageInfo writer,
      List<DatanodeStorageInfo> chosenStorages, boolean returnChosenStorages,
      HashMap<Node, Node> excludedStorages, long blocksize);

  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i>
   * If not, return as many as we can.
   * The base implemenatation extracts the pathname of the file from the
   * specified srcBC, but this could be a costly operation depending on the
   * file system implementation. Concrete implementations of this class should
   * override this method to avoid this overhead.
   *
   * @param srcBC
   *     block collection of file for which chooseTarget is invoked.
   * @param numOfReplicas
   *     additional number of replicas wanted.
   * @param writer
   *     the writer's machine, null if not in the cluster.
   * @param chosenStorages
   *     storages that have been chosen as targets.
   * @param blocksize
   *     size of the data to be written.
   * @return array of DatanodeStorageInfo instances chosen as target
   * and sorted as a pipeline.
   */
  DatanodeStorageInfo[] chooseTarget(BlockCollection srcBC, int numOfReplicas,
      DatanodeStorageInfo writer, List<DatanodeStorageInfo> chosenStorages,
      HashMap<Node, Node> excludedStorages, long blocksize)
      throws StorageException {
    //HOP: [M] srcPath is not used 
    return chooseTarget(/*srcBC.getName()*/null, numOfReplicas, writer,
        chosenStorages, false, excludedStorages, blocksize);
  }

  /**
   * Verify that the block is replicated on at least minRacks different racks
   * if there is more than minRacks rack in the system.
   *
   * @param srcPath
   *     the full pathname of the file to be verified
   * @param lBlk
   *     block with locations
   * @param minRacks
   *     number of racks the block should be replicated to
   * @return the difference between the required and the actual number of racks
   * the block is replicated to.
   */
  abstract public int verifyBlockPlacement(String srcPath, LocatedBlock lBlk,
      int minRacks);

  /**
   * Decide whether deleting the specified replica of the block still makes
   * the block conform to the configured block placement policy.
   *
   * @param srcBC
   *     block collection of file to which block-to-be-deleted belongs
   * @param block
   *     The block to be deleted
   * @param replicationFactor
   *     The required number of replicas for this block
   * @param existingReplicas
   *     The replica locations of this block that are present
   *     on at least two unique racks.
   * @param moreExistingReplicas
   *     Replica locations of this block that are not
   *     listed in the previous parameter.
   * @return the storage that contains the replica that is the best candidate
   * for deletion
   */
  abstract public DatanodeStorageInfo chooseReplicaToDelete(
      BlockCollection srcBC, Block block, short replicationFactor,
      Collection<DatanodeStorageInfo> existingReplicas,
      Collection<DatanodeStorageInfo> moreExistingReplicas);

  /**
   * Used to setup a BlockPlacementPolicy object. This should be defined by
   * all implementations of a BlockPlacementPolicy.
   *
   * @param conf
   *     the configuration object
   * @param stats
   *     retrieve cluster status from here
   * @param clusterMap
   *     cluster topology
   */
  abstract protected void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap);

  /**
   * Get an instance of the configured Block Placement Policy based on the
   * value of the configuration paramater dfs.block.replicator.classname.
   *
   * @param conf
   *     the configuration to be used
   * @param stats
   *     an object that is used to retrieve the load on the cluster
   * @param clusterMap
   *     the network topology of the cluster
   * @return an instance of BlockPlacementPolicy
   */
  public static BlockPlacementPolicy getInstance(Configuration conf,
      FSClusterStats stats, NetworkTopology clusterMap) {
    Class<? extends BlockPlacementPolicy> replicatorClass =
        conf.getClass("dfs.block.replicator.classname",
            BlockPlacementPolicyDefault.class, BlockPlacementPolicy.class);
    BlockPlacementPolicy replicator = (BlockPlacementPolicy) ReflectionUtils
        .newInstance(replicatorClass, conf);
    replicator.initialize(conf, stats, clusterMap);
    return replicator;
  }

}
