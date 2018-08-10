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

package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;
import org.apache.hadoop.io.retry.Idempotent;

/**
 * **************************************************************************
 * Protocol that a secondary NameNode uses to communicate with the NameNode.
 * It's used to get part of the name node state
 * ***************************************************************************
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface NamenodeProtocol {
  /**
   * Until version 6L, this class served as both
   * the client interface to the NN AND the RPC protocol used to
   * communicate with the NN.
   * <p/>
   * This class is used by both the DFSClient and the
   * NN server side to insulate from the protocol serialization.
   * <p/>
   * If you are adding/changing NN's interface then you need to
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in NamenodeProtocol.proto.
   * <p/>
   * For more details on protocol buffer wire protocol, please see
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   * <p/>
   * 6: Switch to txid-based file naming for image and edits
   */
  public static final long versionID = 6L;

  // Error codes passed by errorReport().
  final static int NOTIFY = 0;
  final static int FATAL = 1;

  public final static int ACT_UNKNOWN = 0;    // unknown action   
  public final static int ACT_SHUTDOWN = 50;   // shutdown node
  public final static int ACT_CHECKPOINT = 51;   // do checkpoint

  /**
   * Get a list of blocks belonging to <code>datanode</code>
   * whose total size equals <code>size</code>.
   *
   * @param datanode
   *     a data node
   * @param size
   *     requested size
   * @return a list of blocks & their locations
   * @throws IOException
   *     if size is less than or equal to 0 or
   *     datanode does not exist
   * @see org.apache.hadoop.hdfs.server.balancer.Balancer
   */
  @Idempotent
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
      throws IOException;

  /**
   * Get the current block keys
   *
   * @return ExportedBlockKeys containing current block keys
   * @throws IOException
   */
  @Idempotent
  public ExportedBlockKeys getBlockKeys() throws IOException;

  /**
   * Request name-node version and storage information.
   *
   * @return {@link NamespaceInfo} identifying versions and storage information
   * of the name-node
   * @throws IOException
   */
  @Idempotent
  public NamespaceInfo versionRequest() throws IOException;
}

