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
package org.apache.hadoop.hdfs;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HA_DT_SERVICE_PREFIX;

public class HAUtil {

  private static final Log LOG =
    LogFactory.getLog(HAUtil.class);

  private static final DelegationTokenSelector tokenSelector =
      new DelegationTokenSelector();

  private HAUtil() { /* Hidden constructor */ }

  /**
   * Returns true if HA for namenode is configured for the given nameservice
   * 
   * @param conf Configuration
   * @param nsId nameservice, or null if no federated NS is configured
   * @return true if HA is configured in the configuration; else false.
   */
  public static boolean isHAEnabled(Configuration conf, String nsId) {
    Map<String, Map<String, InetSocketAddress>> addresses =
      DFSUtil.getHaNnRpcAddresses(conf);
    if (addresses == null) return false;
    Map<String, InetSocketAddress> nnMap = addresses.get(nsId);
    return nnMap != null && nnMap.size() > 1;
  }

//  /**
//   * Returns true if HA is using a shared edits directory.
//   *
//   * @param conf Configuration
//   * @return true if HA config is using a shared edits dir, false otherwise.
//   */
//  public static boolean usesSharedEditsDir(Configuration conf) {
//    return null != conf.get(DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
//  }

//  /**
//   * Get the namenode Id by matching the {@code addressKey}
//   * with the the address of the local node.
//   *
//   * If {@link DFSConfigKeys#DFS_HA_NAMENODE_ID_KEY} is not specifically
//   * configured, this method determines the namenode Id by matching the local
//   * node's address with the configured addresses. When a match is found, it
//   * returns the namenode Id from the corresponding configuration key.
//   *
//   * @param conf Configuration
//   * @return namenode Id on success, null on failure.
//   * @throws HadoopIllegalArgumentException on error
//   */
//  public static String getNameNodeId(Configuration conf, String nsId) {
//    String namenodeId = conf.getTrimmed(DFS_HA_NAMENODE_ID_KEY);
//    if (namenodeId != null) {
//      return namenodeId;
//    }
//
//    String suffixes[] = DFSUtil.getSuffixIDs(conf, DFS_NAMENODE_RPC_ADDRESS_KEY,
//        nsId, null, DFSUtil.LOCAL_ADDRESS_MATCHER);
//    if (suffixes == null) {
//      String msg = "Configuration " + DFS_NAMENODE_RPC_ADDRESS_KEY +
//          " must be suffixed with nameservice and namenode ID for HA " +
//          "configuration.";
//      throw new HadoopIllegalArgumentException(msg);
//    }
//
//    return suffixes[1];
//  }

  /**
   * Similar to
   * {@link DFSUtil#getNameServiceIdFromAddress(Configuration, 
   * InetSocketAddress, String...)}
   */
  public static String getNameNodeIdFromAddress(final Configuration conf, 
      final InetSocketAddress address, String... keys) {
    // Configuration with a single namenode and no nameserviceId
    String[] ids = DFSUtil.getSuffixIDs(conf, address, keys);
    if (ids != null && ids.length > 1) {
      return ids[1];
    }
    return null;
  }
  
//  /**
//   * Get the NN ID of the other node in an HA setup.
//   *
//   * @param conf the configuration of this node
//   * @return the NN ID of the other node in this nameservice
//   */
//  public static String getNameNodeIdOfOtherNode(Configuration conf, String nsId) {
//    Preconditions.checkArgument(nsId != null,
//        "Could not determine namespace id. Please ensure that this " +
//        "machine is one of the machines listed as a NN RPC address, " +
//        "or configure " + DFSConfigKeys.DFS_NAMESERVICE_ID);
//
//    Collection<String> nnIds = DFSUtil.getNameNodeIds(conf, nsId);
//    String myNNId = conf.get(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY);
//    Preconditions.checkArgument(nnIds != null,
//        "Could not determine namenode ids in namespace '%s'. " +
//        "Please configure " +
//        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
//            nsId),
//        nsId);
//    Preconditions.checkArgument(nnIds.size() == 2,
//        "Expected exactly 2 NameNodes in namespace '%s'. " +
//        "Instead, got only %s (NN ids were '%s'",
//        nsId, nnIds.size(), Joiner.on("','").join(nnIds));
//    Preconditions.checkState(myNNId != null && !myNNId.isEmpty(),
//        "Could not determine own NN ID in namespace '%s'. Please " +
//        "ensure that this node is one of the machines listed as an " +
//        "NN RPC address, or configure " + DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY,
//        nsId);
//
//    ArrayList<String> nnSet = Lists.newArrayList(nnIds);
//    nnSet.remove(myNNId);
//    assert nnSet.size() == 1;
//    return nnSet.get(0);
//  }

//  /**
//   * Given the configuration for this node, return a Configuration object for
//   * the other node in an HA setup.
//   *
//   * @param myConf the configuration of this node
//   * @return the configuration of the other node in an HA setup
//   */
//  public static Configuration getConfForOtherNode(
//      Configuration myConf) {
//
//    String nsId = DFSUtil.getNamenodeNameServiceId(myConf);
//    String otherNn = getNameNodeIdOfOtherNode(myConf, nsId);
//
//    // Look up the address of the active NN.
//    Configuration confForOtherNode = new Configuration(myConf);
//    NameNode.initializeGenericKeys(confForOtherNode, nsId, otherNn);
//    return confForOtherNode;
//  }

  /**
   * This is used only by tests at the moment.
   * @return true if the NN should allow read operations while in standby mode.
   */
  public static boolean shouldAllowStandbyReads(Configuration conf) {
    return conf.getBoolean("dfs.ha.allow.stale.reads", false);
  }

  public static void setAllowStandbyReads(Configuration conf, boolean val) {
    conf.setBoolean("dfs.ha.allow.stale.reads", val);
  }

  /**
   * @return true if the given nameNodeUri appears to be a logical URI.
   * This is the case if there is a failover proxy provider configured
   * for it in the given configuration.
   */
  public static boolean isLogicalUri(
      Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    String configKey = DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "."
        + host;
    return conf.get(configKey) != null;
  }

  /**
   * Parse the HDFS URI out of the provided token.
   * @throws IOException if the token is invalid
   */
  public static URI getServiceUriFromToken(
      Token<DelegationTokenIdentifier> token)
      throws IOException {
    String tokStr = token.getService().toString();

    if (tokStr.startsWith(HA_DT_SERVICE_PREFIX)) {
      tokStr = tokStr.replaceFirst(HA_DT_SERVICE_PREFIX, "");
    }

    try {
      return new URI(HdfsConstants.HDFS_URI_SCHEME + "://" +
          tokStr);
    } catch (URISyntaxException e) {
      throw new IOException("Invalid token contents: '" +
          tokStr + "'");
    }
  }

  /**
   * Get the service name used in the delegation token for the given logical
   * HA service.
   * @param uri the logical URI of the cluster
   * @return the service name
   */
  public static Text buildTokenServiceForLogicalUri(URI uri) {
    return new Text(HA_DT_SERVICE_PREFIX + uri.getHost());
  }

  /**
   * @return true if this token corresponds to a logical nameservice
   * rather than a specific namenode.
   */
  public static boolean isTokenForLogicalUri(
      Token<DelegationTokenIdentifier> token) {
    return token.getService().toString().startsWith(HA_DT_SERVICE_PREFIX);
  }

//  /**
//   * Locate a delegation token associated with the given HA cluster URI, and if
//   * one is found, clone it to also represent the underlying namenode address.
//   * @param ugi the UGI to modify
//   * @param haUri the logical URI for the cluster
//   * @param nnAddrs collection of NNs in the cluster to which the token
//   * applies
//   */
//  public static void cloneDelegationTokenForLogicalUri(
//          UserGroupInformation ugi, URI haUri,
//          Collection<InetSocketAddress> nnAddrs) {
//    Text haService = HAUtil.buildTokenServiceForLogicalUri(haUri);
//    Token<DelegationTokenIdentifier> haToken =
//            tokenSelector.selectToken(haService, ugi.getTokens());
//    if (haToken != null) {
//      for (InetSocketAddress singleNNAddr : nnAddrs) {
//        // this is a minor hack to prevent physical HA tokens from being
//        // exposed to the user via UGI.getCredentials(), otherwise these
//        // cloned tokens may be inadvertently propagated to jobs
//        Token<DelegationTokenIdentifier> specificToken =
//                new Token.PrivateToken<DelegationTokenIdentifier>(haToken);
//        SecurityUtil.setTokenService(specificToken, singleNNAddr);
//        Text alias =
//                new Text(HA_DT_SERVICE_PREFIX + "//" + specificToken.getService());
//        ugi.addToken(alias, specificToken);
//        LOG.debug("Mapped HA service delegation token for logical URI " +
//                haUri + " to namenode " + singleNNAddr);
//      }
//    } else {
//      LOG.debug("No HA service delegation token found for logical URI " +
//              haUri);
//    }
//  }


  /**
   * Get the internet address of the currently-active NN. This should rarely be
   * used, since callers of this method who connect directly to the NN using the
   * resulting InetSocketAddress will not be able to connect to the active NN if
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
    return RPC.getServerAddress(dfsClient.getNamenode());
  }
}
