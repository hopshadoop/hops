/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HA_DT_SERVICE_PREFIX;


public class HAUtil {

  /**
   * Get the service name used in the delegation token for the given logical
   * HA service.
   *
   * @param uri the logical URI of the cluster
   * @return the service name
   */
  public static Text buildTokenServiceForLogicalUri(URI uri) {
    return new Text(HA_DT_SERVICE_PREFIX + uri.getHost());
  }
  
  /**
   * Get the internet address of the currently-Leader NN.This should rarely be
   * used, since callers of this method who connect directly to the NN using the
   * resulting InetSocketAddress will not be able to connect to the leader NN if
   * a failover were to occur after this method has been called.
   * 
   * @param fs the file system to get the active address of.
   * @return the internet address of the currently-active NN.
   * @throws IOException if an error occurs while resolving the active NN.
   */
  @SuppressWarnings("deprecation")
  public static InetSocketAddress getAddressOfActive(FileSystem fs)
      throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs + " is not a DFS.");
    }
    // force client address resolution.
    fs.exists(new Path("/"));
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    DFSClient dfsClient = dfs.getClient();
    return dfsClient.getActiveNodes().getLeader().getRpcServerAddressForClients();
  }
}
