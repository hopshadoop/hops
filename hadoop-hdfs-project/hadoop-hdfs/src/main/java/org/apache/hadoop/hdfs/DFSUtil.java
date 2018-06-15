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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
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
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer3;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authorize.AccessControlList;

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
  
  /** Shuffle the elements in the given array. */
  public static <T> T[] shuffle(final T[] array) {
    if (array != null && array.length > 0) {
      final Random random = getRandom();
      for (int n = array.length; n > 1; ) {
        final int randomIndex = random.nextInt(n);
        n--;
        if (n != randomIndex) {
          final T tmp = array[randomIndex];
          array[randomIndex] = array[n];
          array[n] = tmp;
        }
      }
    }
    return array;
  }

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
      if (element.equals(".")  ||
          (element.indexOf(":") >= 0) ||
          (element.indexOf("/") >= 0)) {
        return false;
      }
      // ".." is allowed in path starting with /.reserved/.inodes
      if (element.equals("..")) {
        if (components.length > 4
            && components[1].equals(FSDirectory.DOT_RESERVED_STRING)
            && components[2].equals(FSDirectory.DOT_INODES_STRING)) {
          continue;
        }
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
  public static String byteArray2PathString(byte[][] pathComponents) {
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
  
  /** Convert an object representing a path to a string. */
  public static String path2String(final Object path) {
    return path == null? null
        : path instanceof String? (String)path
        : path instanceof byte[][]? byteArray2PathString((byte[][])path)
        : path.toString();
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
   * Returns collection of nameservice Ids from the configuration.
   * @param conf configuration
   * @return collection of nameservice Ids, or null if not specified
   */
  public static Collection<String> getNameServiceIds(Configuration conf) {
    return conf.getTrimmedStringCollection(DFS_NAMESERVICES);
  }

  /**
   * @return <code>coll</code> if it is non-null and non-empty. Otherwise,
   * returns a list with a single null value.
   */
  private static Collection<String> emptyAsSingletonNull(Collection<String> coll) {
    if (coll == null || coll.isEmpty()) {
      return Collections.singletonList(null);
    } else {
      return coll;
    }
  }
  
  /**
   * Namenode HighAvailability related configuration.
   * Returns collection of namenode Ids from the configuration. One logical id
   * for each namenode in the in the HA setup.
   * 
   * @param conf configuration
   * @param nsId the nameservice ID to look at, or null for non-federated 
   * @return collection of namenode Ids
   */
  public static Collection<String> getNameNodeIds(Configuration conf, String nsId) {
    String key = addSuffix(DFS_HA_NAMENODES_KEY_PREFIX, nsId);
    return conf.getTrimmedStringCollection(key);
  }

  /** Add non empty and non null suffix to a key */
  private static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
      "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }
  
  /** Concatenate list of suffix strings '.' separated */
  private static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }
  
  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = concatSuffixes(suffixes);
    return addSuffix(key, keySuffix);
  }
  
  /**
   * Resolve an HDFS URL into real INetSocketAddress. It works like a DNS resolver
   * when the URL points to an non-HA cluster. When the URL points to an HA
   * cluster, the resolver further resolves the logical name (i.e., the authority
   * in the URL) into real namenode addresses.
   */
  public static InetSocketAddress resolveWebHdfsUri(URI uri, Configuration conf)
      throws IOException {
    int defaultPort;
    String scheme = uri.getScheme();
    if (WebHdfsFileSystem.SCHEME.equals(scheme)) {
      defaultPort = DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
    } else if (SWebHdfsFileSystem.SCHEME.equals(scheme)) {
      defaultPort = DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT;
    } else {
      throw new IllegalArgumentException("Unsupported scheme: " + scheme);
    }
    return NetUtils.createSocketAddr(uri.getAuthority(), defaultPort);
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
  @VisibleForTesting
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
  
  private static String getSuffixedConf(Configuration conf,
      String key, String defaultVal, String[] suffixes) {
    String ret = conf.get(DFSUtil.addKeySuffixes(key, suffixes));
    if (ret != null) {
      return ret;
    }
    return conf.get(key, defaultVal);
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

  /**
   * Returns nameservice Id and namenode Id when the local host matches the
   * configuration parameter {@code addressKey}.<nameservice Id>.<namenode Id>
   * 
   * @param conf Configuration
   * @param addressKey configuration key corresponding to the address.
   * @param knownNsId only look at configs for the given nameservice, if not-null
   * @param knownNNId only look at configs for the given namenode, if not null
   * @param matcher matching criteria for matching the address
   * @return Array with nameservice Id and namenode Id on success. First element
   *         in the array is nameservice Id and second element is namenode Id.
   *         Null value indicates that the configuration does not have the the
   *         Id.
   * @throws HadoopIllegalArgumentException on error
   */
  static String[] getSuffixIDs(final Configuration conf, final String addressKey,
      String knownNsId, String knownNNId,
      final AddressMatcher matcher) {
    String nameserviceId = null;
    String namenodeId = null;
    int found = 0;
    
    Collection<String> nsIds = getNameServiceIds(conf);
    for (String nsId : emptyAsSingletonNull(nsIds)) {
      if (knownNsId != null && !knownNsId.equals(nsId)) {
        continue;
      }
      
      Collection<String> nnIds = getNameNodeIds(conf, nsId);
      for (String nnId : emptyAsSingletonNull(nnIds)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("addressKey: %s nsId: %s nnId: %s",
              addressKey, nsId, nnId));
        }
        if (knownNNId != null && !knownNNId.equals(nnId)) {
          continue;
        }
        String key = addKeySuffixes(addressKey, nsId, nnId);
        String addr = conf.get(key);
        if (addr == null) {
          continue;
        }
        InetSocketAddress s = null;
        try {
          s = NetUtils.createSocketAddr(addr);
        } catch (Exception e) {
          LOG.warn("Exception in creating socket address " + addr, e);
          continue;
        }
        if (!s.isUnresolved() && matcher.match(s)) {
          nameserviceId = nsId;
          namenodeId = nnId;
          found++;
        }
      }
    }
    if (found > 1) { // Only one address must match the local address
      String msg = "Configuration has multiple addresses that match "
          + "local node's address. Please configure the system with "
          + DFS_NAMESERVICE_ID;
      throw new HadoopIllegalArgumentException(msg);
    }
    return new String[] { nameserviceId, namenodeId };
  }
  
  /**
   * For given set of {@code keys} adds nameservice Id and or namenode Id
   * and returns {nameserviceId, namenodeId} when address match is found.
   * @see #getSuffixIDs(Configuration, String, AddressMatcher)
   */
  static String[] getSuffixIDs(final Configuration conf,
      final InetSocketAddress address, final String... keys) {
    AddressMatcher matcher = new AddressMatcher() {
     @Override
      public boolean match(InetSocketAddress s) {
        return address.equals(s);
      } 
    };
    
    for (String key : keys) {
      String[] ids = getSuffixIDs(conf, key, null, null, matcher);
      if (ids != null && (ids [0] != null || ids[1] != null)) {
        return ids;
      }
    }
    return null;
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

  /**
   * Get http policy. Http Policy is chosen as follows:
   * <ol>
   * <li>If hadoop.ssl.enabled is set, http endpoints are not started. Only
   * https endpoints are started on configured https ports</li>
   * <li>This configuration is overridden by dfs.https.enable configuration, if
   * it is set to true. In that case, both http and https endpoints are stared.</li>
   * <li>All the above configurations are overridden by dfs.http.policy
   * configuration. With this configuration you can set http-only, https-only
   * and http-and-https endpoints.</li>
   * </ol>
   * See hdfs-default.xml documentation for more details on each of the above
   * configuration settings.
   */
  public static HttpConfig.Policy getHttpPolicy(Configuration conf) {
    String policyStr = conf.get(DFSConfigKeys.DFS_HTTP_POLICY_KEY);
    if (policyStr == null) {
      boolean https = conf.getBoolean(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY,
          DFSConfigKeys.DFS_HTTPS_ENABLE_DEFAULT);

      boolean hadoopSsl = conf.getBoolean(
          CommonConfigurationKeys.HADOOP_SSL_ENABLED_KEY,
          CommonConfigurationKeys.HADOOP_SSL_ENABLED_DEFAULT);

      if (hadoopSsl) {
        LOG.warn(CommonConfigurationKeys.HADOOP_SSL_ENABLED_KEY
            + " is deprecated. Please use " + DFSConfigKeys.DFS_HTTP_POLICY_KEY
            + ".");
      }
      if (https) {
        LOG.warn(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY
            + " is deprecated. Please use " + DFSConfigKeys.DFS_HTTP_POLICY_KEY
            + ".");
      }
      
      return (hadoopSsl || https) ? HttpConfig.Policy.HTTP_AND_HTTPS
          : HttpConfig.Policy.HTTP_ONLY;
    }

    HttpConfig.Policy policy = HttpConfig.Policy.fromString(policyStr);
    if (policy == null) {
      throw new HadoopIllegalArgumentException("Unregonized value '"
          + policyStr + "' for " + DFSConfigKeys.DFS_HTTP_POLICY_KEY);
    }

    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, policy.name());
    return policy;
  }

  public static HttpServer3.Builder loadSslConfToHttpServerBuilder(HttpServer3.Builder builder,
      Configuration sslConf) {
    return builder
        .needsClientAuth(
            sslConf.getBoolean(DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT))
        .keyPassword(sslConf.get("ssl.server.keystore.keypassword"))
        .keyStore(sslConf.get("ssl.server.keystore.location"),
            sslConf.get("ssl.server.keystore.password"),
            sslConf.get("ssl.server.keystore.type", "jks"))
        .trustStore(sslConf.get("ssl.server.truststore.location"),
            sslConf.get("ssl.server.truststore.password"),
            sslConf.get("ssl.server.truststore.type", "jks"));
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

  public static URI getInfoServer(InetSocketAddress namenodeAddr,
      Configuration conf, String scheme) throws IOException {

    String[] suffixes = null;
    if (namenodeAddr != null) {
      // if non-default namenode, try reverse look up 
      // the nameServiceID if it is available
      suffixes = getSuffixIDs(conf, namenodeAddr,
          DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    }

    String authority;
    if ("http".equals(scheme)) {
      authority = getSuffixedConf(conf, DFS_NAMENODE_HTTP_ADDRESS_KEY,
          DFS_NAMENODE_HTTP_ADDRESS_DEFAULT, suffixes);
    } else if ("https".equals(scheme)) {
      authority = getSuffixedConf(conf, DFS_NAMENODE_HTTPS_ADDRESS_KEY,
          DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT, suffixes);
    } else {
      throw new IllegalArgumentException("Invalid scheme:" + scheme);
    }
    if (namenodeAddr != null) {
      authority = substituteForWildcardAddress(authority,
          namenodeAddr.getHostName());
    }
    return URI.create(scheme + "://" + authority);

  }

  /**
   * Lookup the HTTP / HTTPS address of the namenode, and replace its hostname
   * with defaultHost when it found out that the address is a wildcard / local
   * address.
   *
   * @param defaultHost
   *          The default host name of the namenode.
   * @param conf
   *          The configuration
   * @param scheme
   *          HTTP or HTTPS
   * @throws IOException
   */
  public static URI getInfoServerWithDefaultHost(String defaultHost,
      Configuration conf, final String scheme) throws IOException {
    URI configuredAddr = getInfoServer(null, conf, scheme);
    String authority = substituteForWildcardAddress(
        configuredAddr.getAuthority(), defaultHost);
    return URI.create(scheme + "://" + authority);
  }

  /**
   * Determine whether HTTP or HTTPS should be used to connect to the remote
   * server. Currently the client only connects to the server via HTTPS if the
   * policy is set to HTTPS_ONLY.
   *
   * @return the scheme (HTTP / HTTPS)
   */
  public static String getHttpClientScheme(Configuration conf) {
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    return policy == HttpConfig.Policy.HTTPS_ONLY ? "https" : "http";
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
  
  /**
   * Load HTTPS-related configuration.
   */
  public static Configuration loadSslConfiguration(Configuration conf) {
    Configuration sslConf = new Configuration(false);

    sslConf.addResource(conf.get(
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT));

    boolean requireClientAuth = conf.getBoolean(DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
        DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
    sslConf.setBoolean(DFS_CLIENT_HTTPS_NEED_AUTH_KEY, requireClientAuth);
    return sslConf;
  }

  /**
   * Return a HttpServer.Builder that the journalnode / namenode / secondary
   * namenode can use to initialize their HTTP / HTTPS server.
   * <p>
   */
  public static HttpServer3.Builder httpServerTemplateForNNAndJN(
      Configuration conf, final InetSocketAddress httpAddr,
      final InetSocketAddress httpsAddr, String name, String spnegoUserNameKey,
      String spnegoKeytabFileKey) throws IOException {
    HttpConfig.Policy policy = getHttpPolicy(conf);

    HttpServer3.Builder builder = new HttpServer3.Builder().setName(name)
        .setConf(conf).setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")))
        .setSecurityEnabled(UserGroupInformation.isSecurityEnabled())
        .setUsernameConfKey(spnegoUserNameKey)
        .setKeytabConfKey(getSpnegoKeytabKey(conf, spnegoKeytabFileKey));

    // initialize the webserver for uploading/downloading files.
    LOG.info("Starting web server as: "
        + SecurityUtil.getServerPrincipal(conf.get(spnegoUserNameKey),
            httpAddr.getHostName()));

    if (policy.isHttpEnabled()) {
      if (httpAddr.getPort() == 0) {
        builder.setFindPort(true);
      }

      URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddr));
      builder.addEndpoint(uri);
      LOG.info("Starting Web-server for " + name + " at: " + uri);
    }

    if (policy.isHttpsEnabled() && httpsAddr != null) {
      Configuration sslConf = loadSslConfiguration(conf);
      loadSslConfToHttpServerBuilder(builder, sslConf);

      if (httpsAddr.getPort() == 0) {
        builder.setFindPort(true);
      }

      URI uri = URI.create("https://" + NetUtils.getHostPortString(httpsAddr));
      builder.addEndpoint(uri);
      LOG.info("Starting Web-server for " + name + " at: " + uri);
    }
    return builder;
  }
}

