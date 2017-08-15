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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.BlockingService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;

@InterfaceAudience.Private
public class DFSUtil {
  public static final Log LOG = LogFactory.getLog(DFSUtil.class.getName());
  
  private DFSUtil() { /* Hidden constructor */ }

  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };
  
  private static final ThreadLocal<SecureRandom> SECURE_RANDOM =
      new ThreadLocal<SecureRandom>() {
        @Override
        protected SecureRandom initialValue() {
          return new SecureRandom();
        }
      };

  /**
   * @return a pseudo random number generator.
   */
  public static Random getRandom() {
    return RANDOM.get();
  }
  
  /**
   * @return a pseudo secure random number generator.
   */
  public static SecureRandom getSecureRandom() {
    return SECURE_RANDOM.get();
  }

  /**
   * Compartor for sorting DataNodeInfo[] based on decommissioned states.
   * Decommissioned nodes are moved to the end of the array on sorting with
   * this compartor.
   */
  public static final Comparator<DatanodeInfo> DECOM_COMPARATOR =
      new Comparator<DatanodeInfo>() {
        @Override
        public int compare(DatanodeInfo a, DatanodeInfo b) {
          return a.isDecommissioned() == b.isDecommissioned() ? 0 :
              a.isDecommissioned() ? 1 : -1;
        }
      };


  /**
   * Comparator for sorting DataNodeInfo[] based on decommissioned/stale
   * states.
   * Decommissioned/stale nodes are moved to the end of the array on sorting
   * with this comparator.
   */
  @InterfaceAudience.Private
  public static class DecomStaleComparator implements Comparator<DatanodeInfo> {
    private long staleInterval;

    /**
     * Constructor of DecomStaleComparator
     *
     * @param interval
     *     The time interval for marking datanodes as stale is passed from
     *     outside, since the interval may be changed dynamically
     */
    public DecomStaleComparator(long interval) {
      this.staleInterval = interval;
    }

    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      // Decommissioned nodes will still be moved to the end of the list
      if (a.isDecommissioned()) {
        return b.isDecommissioned() ? 0 : 1;
      } else if (b.isDecommissioned()) {
        return -1;
      }
      // Stale nodes will be moved behind the normal nodes
      boolean aStale = a.isStale(staleInterval);
      boolean bStale = b.isStale(staleInterval);
      return aStale == bStale ? 0 : (aStale ? 1 : -1);
    }
  }

  /**
   * Address matcher for matching an address to local address
   */
  static final AddressMatcher LOCAL_ADDRESS_MATCHER = new AddressMatcher() {
    @Override
    public boolean match(InetSocketAddress s) {
      return NetUtils.isLocalAddress(s.getAddress());
    }

    ;
  };
  
  /**
   * Whether the pathname is valid.  Currently prohibits relative paths,
   * names which contain a ":" or "//", or other non-canonical paths.
   */
  public static boolean isValidName(String src) {
    // Path must be absolute.
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }

    // Check for ".." "." ":" "/"
    String[] components = StringUtils.split(src, '/');
    for (int i = 0; i < components.length; i++) {
      String element = components[i];
      if (element.equals("..") ||
          element.equals(".") ||
          (element.indexOf(":") >= 0) ||
          (element.indexOf("/") >= 0)) {
        return false;
      }
      
      // The string may start or end with a /, but not have
      // "//" in the middle.
      if (element.isEmpty() && i != components.length - 1 &&
          i != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  public static String bytes2String(byte[] bytes) {
    try {
      return new String(bytes, "UTF8");
    } catch (UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    return str.getBytes(Charsets.UTF_8);
  }

  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2String(byte[][] pathComponents) {
    if (pathComponents.length == 0) {
      return "";
    }
    if (pathComponents.length == 1 && pathComponents[0].length == 0) {
      return Path.SEPARATOR;
    }
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < pathComponents.length; i++) {
      result.append(new String(pathComponents[i], Charsets.UTF_8));
      if (i < pathComponents.length - 1) {
        result.append(Path.SEPARATOR_CHAR);
      }
    }
    return result.toString();
  }

  /**
   * Splits the array of bytes into array of arrays of bytes
   * on byte separator
   *
   * @param bytes
   *     the array of bytes to split
   * @param separator
   *     the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes, byte separator) {
    return bytes2byteArray(bytes, bytes.length, separator);
  }

  /**
   * Splits first len bytes in bytes to array of arrays of bytes
   * on byte separator
   *
   * @param bytes
   *     the byte array to split
   * @param len
   *     the number of bytes to split
   * @param separator
   *     the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes, int len,
      byte separator) {
    assert len <= bytes.length;
    int splits = 0;
    if (len == 0) {
      return new byte[][]{null};
    }
    // Count the splits. Omit multiple separators and the last one
    for (int i = 0; i < len; i++) {
      if (bytes[i] == separator) {
        splits++;
      }
    }
    int last = len - 1;
    while (last > -1 && bytes[last--] == separator) {
      splits--;
    }
    if (splits == 0 && bytes[0] == separator) {
      return new byte[][]{null};
    }
    splits++;
    byte[][] result = new byte[splits][];
    int startIndex = 0;
    int nextIndex = 0;
    int index = 0;
    // Build the splits
    while (index < splits) {
      while (nextIndex < len && bytes[nextIndex] != separator) {
        nextIndex++;
      }
      result[index] = new byte[nextIndex - startIndex];
      System.arraycopy(bytes, startIndex, result[index], 0,
          nextIndex - startIndex);
      index++;
      startIndex = nextIndex + 1;
      nextIndex = startIndex;
    }
    return result;
  }
  
  /**
   * Convert a LocatedBlocks to BlockLocations[]
   *
   * @param blocks
   *     a LocatedBlocks
   * @return an array of BlockLocations
   */
  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    return locatedBlocks2Locations(blocks.getLocatedBlocks());
  }
  
  /**
   * Convert a List<LocatedBlock> to BlockLocation[]
   *
   * @param blocks
   *     A List<LocatedBlock> to be converted
   * @return converted array of BlockLocation
   */
  public static BlockLocation[] locatedBlocks2Locations(
      List<LocatedBlock> blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.size();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    if (nrBlocks == 0) {
      return blkLocations;
    }
    int idx = 0;
    for (LocatedBlock blk : blocks) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] xferAddrs = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        xferAddrs[hCnt] = locations[hCnt].getXferAddr();
        NodeBase node =
            new NodeBase(xferAddrs[hCnt], locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] =
          new BlockLocation(xferAddrs, hosts, racks, blk.getStartOffset(),
              blk.getBlockSize(), blk.isCorrupt());
      idx++;
    }
    return blkLocations;
  }

  /**
   * Substitute a default host in the case that an address has been configured
   * with a wildcard. This is used, for example, when determining the HTTP
   * address of the NN -- if it's configured to bind to 0.0.0.0, we want to
   * substitute the hostname from the filesystem URI rather than trying to
   * connect to 0.0.0.0.
   *
   * @param configuredAddress
   *     the address found in the configuration
   * @param defaultHost
   *     the host to substitute with, if configuredAddress
   *     is a local/wildcard address.
   * @return the substituted address
   * @throws IOException
   *     if it is a wildcard address and security is enabled
   */
  public static String substituteForWildcardAddress(String configuredAddress,
      String defaultHost) throws IOException {
    InetSocketAddress sockAddr = NetUtils.createSocketAddr(configuredAddress);
    InetSocketAddress defaultSockAddr =
        NetUtils.createSocketAddr(defaultHost + ":0");
    if (sockAddr.getAddress().isAnyLocalAddress()) {
      if (UserGroupInformation.isSecurityEnabled() &&
          defaultSockAddr.getAddress().isAnyLocalAddress()) {
        throw new IOException("Cannot use a wildcard address with security. " +
            "Must explicitly set bind address for Kerberos");
      }
      return defaultHost + ":" + sockAddr.getPort();
    } else {
      return configuredAddress;
    }
  }

  /**
   * Return used as percentage of capacity
   */
  public static float getPercentUsed(long used, long capacity) {
    return capacity <= 0 ? 100 : (used * 100.0f) / capacity;
  }
  
  /**
   * Return remaining as percentage of capacity
   */
  public static float getPercentRemaining(long remaining, long capacity) {
    return capacity <= 0 ? 0 : (remaining * 100.0f) / capacity;
  }

  /**
   * Convert percentage to a string.
   */
  public static String percent2String(double percentage) {
    return StringUtils.format("%.2f%%", percentage);
  }

  /**
   * Round bytes to GiB (gibibyte)
   *
   * @param bytes
   *     number of bytes
   * @return number of GiB
   */
  public static int roundBytesToGB(long bytes) {
    return Math.round((float) bytes / 1024 / 1024 / 1024);
  }
  
  /**
   * Create a {@link ClientDatanodeProtocol} proxy
   */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      boolean connectToDnViaHostname, LocatedBlock locatedBlock)
      throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(datanodeid, conf,
        socketTimeout, connectToDnViaHostname, locatedBlock);
  }
  
  /**
   * Create {@link ClientDatanodeProtocol} proxy using kerberos ticket
   */
  static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      boolean connectToDnViaHostname) throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(datanodeid, conf,
        socketTimeout, connectToDnViaHostname);
  }
  
  /**
   * Create a {@link ClientDatanodeProtocol} proxy
   */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory) throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(addr, ticket, conf, factory);
  }


  private interface AddressMatcher {
    public boolean match(InetSocketAddress s);
  }

  /**
   * Create a URI from the scheme and address
   */
  public static URI createUri(String scheme, InetSocketAddress address) {
    try {
      return new URI(scheme, null, address.getHostName(), address.getPort(),
          null, null, null);
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException(ue);
    }
  }
  
  /**
   * Add protobuf based protocol to the {@link org.apache.hadoop.ipc.RPC.Server}
   *
   * @param conf
   *     configuration
   * @param protocol
   *     Protocol interface
   * @param service
   *     service that implements the protocol
   * @param server
   *     RPC server to which the protocol & implementation is added to
   * @throws IOException
   */
  public static void addPBProtocol(Configuration conf, Class<?> protocol,
      BlockingService service, RPC.Server server) throws IOException {
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine.class);
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, service);
  }

  public static Options helpOptions = new Options();
  public static Option helpOpt =
      new Option("h", "help", false, "get help information");

  static {
    helpOptions.addOption(helpOpt);
  }

  /**
   * Parse the arguments for commands
   *
   * @param args
   *     the argument to be parsed
   * @param helpDescription
   *     help information to be printed out
   * @param out
   *     Printer
   * @param printGenericCommandUsage
   *     whether to print the
   *     generic command usage defined in ToolRunner
   * @return true when the argument matches help option, false if not
   */
  public static boolean parseHelpArgument(String[] args, String helpDescription,
      PrintStream out, boolean printGenericCommandUsage) {
    if (args.length == 1) {
      try {
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = parser.parse(helpOptions, args);
        if (cmdLine.hasOption(helpOpt.getOpt()) ||
            cmdLine.hasOption(helpOpt.getLongOpt())) {
          // should print out the help information
          out.println(helpDescription + "\n");
          if (printGenericCommandUsage) {
            ToolRunner.printGenericCommandUsage(out);
          }
          return true;
        }
      } catch (ParseException pe) {
        return false;
      }
    }
    return false;
  }
  
  /**
   * Get DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION from configuration.
   *
   * @param conf
   *     Configuration
   * @return Value of DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION
   */
  public static float getInvalidateWorkPctPerIteration(Configuration conf) {
    float blocksInvalidateWorkPct = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION,
        DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION_DEFAULT);
    Preconditions.checkArgument(
        (blocksInvalidateWorkPct > 0 && blocksInvalidateWorkPct <= 1.0f),
        DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION +
            " = '" + blocksInvalidateWorkPct + "' is invalid. " +
            "It should be a positive, non-zero float value, not greater than 1.0f, " +
            "to indicate a percentage.");
    return blocksInvalidateWorkPct;
  }

  /**
   * Get DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION from
   * configuration.
   *
   * @param conf
   *     Configuration
   * @return Value of DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION
   */
  public static int getReplWorkMultiplier(Configuration conf) {
    int blocksReplWorkMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION_DEFAULT);
    Preconditions.checkArgument((blocksReplWorkMultiplier > 0),
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION +
            " = '" + blocksReplWorkMultiplier + "' is invalid. " +
            "It should be a positive, non-zero integer value.");
    return blocksReplWorkMultiplier;
  }
  
  /**
   * Get SPNEGO keytab Key from configuration
   *
   * @param conf
   *     Configuration
   * @param defaultKey
   * @return DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY if the key is not empty
   * else return defaultKey
   */
  public static String getSpnegoKeytabKey(Configuration conf,
      String defaultKey) {
    String value =
        conf.get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY);
    return (value == null || value.isEmpty()) ? defaultKey :
        DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY;
  }


  public static List<InetSocketAddress> getNameNodesServiceRpcAddresses(
      Configuration conf) throws IOException {
    List<InetSocketAddress> addresses = getNameNodesRPCAddresses(conf,
        DFSConfigKeys.DFS_NAMENODES_SERVICE_RPC_ADDRESS_KEY,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
    if (addresses.isEmpty()) {
      addresses = getNameNodesRPCAddresses(conf);
    }

    if (addresses.isEmpty()) {
      String defaultAddress = getFSDefaultNameAsHostPortString(conf);
      if (defaultAddress != null) {
        URI uri = DFSUtil.createHDFSUri(defaultAddress);
        addresses.add(new InetSocketAddress(uri.getHost(), uri.getPort()));
      }
    }

    if (addresses.isEmpty()) {
      throw new IOException("Incorrect configuration: namenode address " +
          DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + " or " +
          DFS_NAMENODE_RPC_ADDRESS_KEY + " is not configured.");
    }
    return addresses;
  }

  public static List<InetSocketAddress> getNameNodesRPCAddresses(
      Configuration conf) {
    return getNameNodesRPCAddresses(conf,
        DFSConfigKeys.DFS_NAMENODES_RPC_ADDRESS_KEY,
        DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  private static List<InetSocketAddress> getNameNodesRPCAddresses(
      Configuration conf, String listKey, String singleKey) {
    List<InetSocketAddress> addresses = new ArrayList<>();
    for (URI uri : getNameNodesRPCAddressesAsURIs(conf, listKey, singleKey)) {
      addresses.add(new InetSocketAddress(uri.getHost(), uri.getPort()));
    }
    return addresses;
  }

  public static List<URI> getNameNodesRPCAddressesAsURIs(Configuration conf) {
    return getNameNodesRPCAddressesAsURIs(conf,
        DFSConfigKeys.DFS_NAMENODES_RPC_ADDRESS_KEY,
        DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  private static List<URI> getNameNodesRPCAddressesAsURIs(Configuration conf,
      String listKey, String singleKey) {
    List<URI> uris = new ArrayList<>();
    for (String nn : getNameNodesRPCAddressesInternal(conf, listKey,
        singleKey)) {
      uris.add(DFSUtil.createHDFSUri(nn));
    }
    return uris;
  }

  public static String joinNameNodesRpcAddresses(
      List<InetSocketAddress> namenodes) {
    List<String> nnstr = Lists.newArrayList();
    for (InetSocketAddress nn : namenodes) {
      nnstr.add(NetUtils.getHostPortString(nn));
    }
    return joinNameNodesHostPortString(nnstr);
  }

  public static String joinNameNodesHostPortString(List<String> namenodes) {
    return Joiner.on(",").join(namenodes);
  }

  private static Set<String> getNameNodesRPCAddressesInternal(
      Configuration conf, String listKey, String singleKey) {
    String namenodes = conf.get(listKey);
    Set<String> namenodesSet = Sets.newHashSet();

    if (namenodes != null && !namenodes.isEmpty()) {
      String[] nnsStr = namenodes.split(",");
      for (String nn : nnsStr) {
        if (!nn.isEmpty()) {
          namenodesSet.add(nn);
        }
      }
    }

//    String defaultAddress = getFSDefaultNameAsHostPortString(conf);
//    if (defaultAddress != null) {
//      namenodesSet.add(defaultAddress);
//    }

    String singleNameNode = conf.get(singleKey);
    if (singleNameNode != null && !singleNameNode.isEmpty()) {
      namenodesSet.add(singleNameNode);
    }

    return namenodesSet;
  }

  private static String getFSDefaultNameAsHostPortString(Configuration conf) {
    String defaultAddress = conf.get(DFSConfigKeys.FS_DEFAULT_NAME_KEY);
    if (defaultAddress != null && !defaultAddress.isEmpty() &&
        defaultAddress.startsWith(HdfsConstants.HDFS_URI_SCHEME)) {
      URI defaultUri = createHDFSUri(defaultAddress);
      return defaultUri.getHost() + ":" + defaultUri.getPort();
    }
    return null;
  }

  //FIXME: fix for https?
  public static String getInfoServer(FileSystem fs, boolean httpsAddress)
      throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs + " is not a DFS.");
    }
    // force client address resolution.
    fs.exists(new Path("/"));
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    DFSClient dfsClient = dfs.getClient();
    return dfsClient.getActiveNodes().getLeader().getHttpAddress();
  }

  public static List<URI> getNsServiceRpcUris(Configuration conf)
      throws URISyntaxException {
    return getNameNodesRPCAddressesAsURIs(conf,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
  }

  /**
   * @return true if the given nameNodeUri appears to be a logical URI.
   * This is the case if there is a failover proxy provider configured
   * for it in the given configuration.
   */
  public static boolean isLogicalUri(Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    String configKey =
        DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." +
            host;
    return conf.get(configKey) != null;
  }

  /**
   * Parse the HDFS URI out of the provided token.
   *
   * @throws IOException
   *     if the token is invalid
   */
  public static URI getServiceUriFromToken(
      Token<DelegationTokenIdentifier> token) throws IOException {
    String tokStr = token.getService().toString();

    //FIXME:
    if (tokStr.startsWith(HdfsConstants.HA_DT_SERVICE_PREFIX)) {
      tokStr = tokStr.replaceFirst(HdfsConstants.HA_DT_SERVICE_PREFIX, "");
    }

    try {
      return new URI(HdfsConstants.HDFS_URI_SCHEME + "://" +
          tokStr);
    } catch (URISyntaxException e) {
      throw new IOException("Invalid token contents: '" +
          tokStr + "'");
    }
  }

  public static URI createHDFSUri(String server) {
    if (server.startsWith(HdfsConstants.HDFS_URI_SCHEME)) {
      return URI.create(server);
    } else {
      return URI.create(HdfsConstants.HDFS_URI_SCHEME + "://" + server);
    }
  }
}

