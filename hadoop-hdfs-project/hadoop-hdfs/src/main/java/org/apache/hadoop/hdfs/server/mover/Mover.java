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
package org.apache.hadoop.hdfs.server.mover;

import com.google.common.annotations.VisibleForTesting;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.*;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.Matcher;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@InterfaceAudience.Private
public class Mover {
  static final Log LOG = LogFactory.getLog(Mover.class);

  static final Path MOVER_ID_PATH = new Path("/system/mover.id");

  private static class StorageMap {
    private final StorageGroupMap<Source> sources
            = new StorageGroupMap<Source>();
    private final StorageGroupMap<StorageGroup> targets
            = new StorageGroupMap<StorageGroup>();
    private final EnumMap<StorageType, List<StorageGroup>> targetStorageTypeMap
            = new EnumMap<StorageType, List<StorageGroup>>(StorageType.class);

    private StorageMap() {
      for(StorageType t : StorageType.getMovableTypes()) {
        targetStorageTypeMap.put(t, new LinkedList<StorageGroup>());
      }
    }

    private void add(Source source, StorageGroup target) {
      sources.put(source);
      if (target != null) {
        targets.put(target);
        getTargetStorages(target.getStorageType()).add(target);
      }
    }

    private Source getSource(MLocation ml) {
      return get(sources, ml);
    }

    private StorageGroup getTarget(String uuid, StorageType storageType) {
      return targets.get(uuid, storageType);
    }

    private static <G extends StorageGroup> G get(StorageGroupMap<G> map, MLocation ml) {
      return map.get(ml.datanode.getDatanodeUuid(), ml.storageType);
    }

    private List<StorageGroup> getTargetStorages(StorageType t) {
      return targetStorageTypeMap.get(t);
    }
  }

  private final Dispatcher dispatcher;
  private final StorageMap storages;
  private final List<Path> targetPaths;
  private final int retryMaxAttempts;
  private final AtomicInteger retryCount;

  private final BlockStoragePolicy[] blockStoragePolicies;

  Mover(NameNodeConnector nnc, Configuration conf, AtomicInteger retryCount) {
    final long movedWinWidth = conf.getLong(
            DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_KEY,
            DFSConfigKeys.DFS_MOVER_MOVEDWINWIDTH_DEFAULT);
    final int moverThreads = conf.getInt(
            DFSConfigKeys.DFS_MOVER_MOVERTHREADS_KEY,
            DFSConfigKeys.DFS_MOVER_MOVERTHREADS_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
            DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    this.retryMaxAttempts = conf.getInt(
            DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_KEY,
            DFSConfigKeys.DFS_MOVER_RETRY_MAX_ATTEMPTS_DEFAULT);
    this.retryCount = retryCount;
    this.dispatcher = new Dispatcher(nnc, Collections.<String> emptySet(),
            Collections.<String> emptySet(), movedWinWidth, moverThreads, 0,
            maxConcurrentMovesPerNode, conf);
    this.storages = new StorageMap();
    this.targetPaths = nnc.getTargetPaths();
    this.blockStoragePolicies = new BlockStoragePolicy[1 <<
            BlockStoragePolicySuite.ID_BIT_LENGTH];
  }

  void init() throws IOException {
    initStoragePolicies();
    final List<DatanodeStorageReport> reports = dispatcher.init();
    for(DatanodeStorageReport r : reports) {
      final DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
      for(StorageType t : StorageType.getMovableTypes()) {
        final Source source = dn.addSource(t, Long.MAX_VALUE, dispatcher);
        final long maxRemaining = getMaxRemaining(r, t);
        final StorageGroup target = maxRemaining > 0L ? dn.addTarget(t,
                maxRemaining) : null;
        storages.add(source, target);
      }
    }
  }

  private void initStoragePolicies() throws IOException {
    BlockStoragePolicy[] policies = dispatcher.getDistributedFileSystem()
            .getStoragePolicies();
    for (BlockStoragePolicy policy : policies) {
      this.blockStoragePolicies[policy.getId()] = policy;
    }
  }

  private ExitStatus run() {
    try {
      init();
      return new Processor().processNamespace().getExitStatus();
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.IO_EXCEPTION;
    } finally {
      dispatcher.shutdownNow();
    }
  }

  DBlock newDBlock(Block block, List<MLocation> locations) {
    final DBlock db = new DBlock(block);
    for(MLocation ml : locations) {
      StorageGroup source = storages.getSource(ml);
      if (source != null) {
        db.addLocation(source);
      }
    }
    return db;
  }

  private static long getMaxRemaining(DatanodeStorageReport report, StorageType t) {
    long max = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        if (r.getRemaining() > max) {
          max = r.getRemaining();
        }
      }
    }
    return max;
  }

  class Processor {
    private final DFSClient dfs;
    private final List<String> snapshottableDirs = new ArrayList<String>();

    Processor() {
      dfs = dispatcher.getDistributedFileSystem().getClient();
    }

    /**
     * @return whether there is still remaining migration work for the next
     *         round
     */
    private Result processNamespace() throws IOException {
      Result result = new Result();
      for (Path target : targetPaths) {
        processPath(target.toUri().getPath(), result);
      }
      // wait for pending move to finish and retry the failed migration
      boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.targets
              .values());
      boolean hasSuccess = Dispatcher.checkForSuccess(storages.targets
              .values());
      if (hasFailed && !hasSuccess) {
        if (retryCount.get() == retryMaxAttempts) {
          result.setRetryFailed();
          LOG.error("Failed to move some block's after "
                  + retryMaxAttempts + " retries.");
          return result;
        } else {
          retryCount.incrementAndGet();
        }
      } else {
        // Reset retry count if no failure.
        retryCount.set(0);
      }
      result.updateHasRemaining(hasFailed);
      return result;
    }

    /**
     * @return whether there is still remaing migration work for the next
     *         round
     */
    private void processPath(String fullPath, Result result) {
      for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;;) {
        final DirectoryListing children;
        try {
          children = dfs.listPaths(fullPath, lastReturnedName, true);
        } catch(IOException e) {
          LOG.warn("Failed to list directory " + fullPath
                  + ". Ignore the directory and continue.", e);
          return;
        }
        if (children == null) {
          return;
        }
        for (HdfsFileStatus child : children.getPartialListing()) {
          processRecursively(fullPath, child, result);
        }
        if (children.hasMore()) {
          lastReturnedName = children.getLastName();
        } else {
          return;
        }
      }
    }

    /** @return whether the migration requires next round */
    private void processRecursively(String parent, HdfsFileStatus status,
                                    Result result) {
      String fullPath = status.getFullName(parent);
      if (status.isDir()) {
        if (!fullPath.endsWith(Path.SEPARATOR)) {
          fullPath = fullPath + Path.SEPARATOR;
        }

        processPath(fullPath, result);
        // process snapshots if this is a snapshottable directory
      } else if (!status.isSymlink()) { // file
        // the full path is a snapshot path but it is also included in the
        // current directory tree, thus ignore it.
        processFile(fullPath, (HdfsLocatedFileStatus) status, result);
      }
    }

    /** @return true if it is necessary to run another round of migration */
    private void processFile(String fullPath, HdfsLocatedFileStatus status,
                             Result result) {
      byte policyId = status.getStoragePolicy();
      if (policyId == HdfsConstantsClient.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
        return;
      }
      final BlockStoragePolicy policy = blockStoragePolicies[policyId];
      if (policy == null) {
        LOG.warn("Failed to get the storage policy of file " + fullPath);
        return;
      }
      final List<StorageType> types = policy.chooseStorageTypes(
              status.getReplication());

      final LocatedBlocks locatedBlocks = status.getBlockLocations();
      final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
      List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
      for (int i = 0; i < lbs.size(); i++) {
        if (i == lbs.size() - 1 && !lastBlkComplete) {
          // last block is incomplete, skip it
          continue;
        }
        LocatedBlock lb = lbs.get(i);
        final StorageTypeDiff diff = new StorageTypeDiff(types,
                lb.getStorageTypes());
        if (!diff.removeOverlap(true)) {
          if (scheduleMoves4Block(diff, lb)) {
            result.updateHasRemaining(diff.existing.size() > 1
                    && diff.expected.size() > 1);
            // One block scheduled successfully, set noBlockMoved to false
            result.setNoBlockMoved(false);
          } else {
            result.updateHasRemaining(true);
          }
        }
      }
    }

    boolean scheduleMoves4Block(StorageTypeDiff diff, LocatedBlock lb) {
      final List<MLocation> locations = MLocation.toLocations(lb);
      Collections.shuffle(locations);
      final DBlock db = newDBlock(lb.getBlock().getLocalBlock(), locations);

      for (final StorageType t : diff.existing) {
        for (final MLocation ml : locations) {
          final Source source = storages.getSource(ml);
          if (ml.storageType == t && source != null) {
            // try to schedule one replica move.
            if (scheduleMoveReplica(db, source, diff.expected)) {
              return true;
            }
          }
        }
      }
      return false;
    }

    @VisibleForTesting
    boolean scheduleMoveReplica(DBlock db, MLocation ml,
                                List<StorageType> targetTypes) {
      final Source source = storages.getSource(ml);
      return source == null ? false : scheduleMoveReplica(db, source,
              targetTypes);
    }

    boolean scheduleMoveReplica(DBlock db, Source source,
                                List<StorageType> targetTypes) {
      // Match storage on the same node
      if (chooseTargetInSameNode(db, source, targetTypes)) {
        return true;
      }

      if (dispatcher.getCluster().isNodeGroupAware()) {
        if (chooseTarget(db, source, targetTypes, Matcher.SAME_NODE_GROUP)) {
          return true;
        }
      }

      // Then, match nodes on the same rack
      if (chooseTarget(db, source, targetTypes, Matcher.SAME_RACK)) {
        return true;
      }
      // At last, match all remaining nodes
      return chooseTarget(db, source, targetTypes, Matcher.ANY_OTHER);
    }

    /**
     * Choose the target storage within same Datanode if possible.
     */
    boolean chooseTargetInSameNode(DBlock db, Source source,
                                   List<StorageType> targetTypes) {
      for (StorageType t : targetTypes) {
        StorageGroup target = storages.getTarget(source.getDatanodeInfo()
                .getDatanodeUuid(), t);
        if (target == null) {
          continue;
        }
        final PendingMove pm = source.addPendingMove(db, target);
        if (pm != null) {
          dispatcher.executePendingMove(pm);
          return true;
        }
      }
      return false;
    }

    boolean chooseTarget(DBlock db, Source source,
                         List<StorageType> targetTypes, Matcher matcher) {
      final NetworkTopology cluster = dispatcher.getCluster();
      for (StorageType t : targetTypes) {
        final List<StorageGroup> targets = storages.getTargetStorages(t);
        Collections.shuffle(targets);
        for (StorageGroup target : targets) {
          if (matcher.match(cluster, source.getDatanodeInfo(),
                  target.getDatanodeInfo())) {
            final PendingMove pm = source.addPendingMove(db, target);
            if (pm != null) {
              dispatcher.executePendingMove(pm);
              return true;
            }
          }
        }
      }
      return false;
    }
  }

  static class MLocation {
    final DatanodeInfo datanode;
    final StorageType storageType;
    final long size;

    MLocation(DatanodeInfo datanode, StorageType storageType, long size) {
      this.datanode = datanode;
      this.storageType = storageType;
      this.size = size;
    }

    static List<MLocation> toLocations(LocatedBlock lb) {
      final DatanodeInfo[] datanodeInfos = lb.getLocations();
      final StorageType[] storageTypes = lb.getStorageTypes();
      final long size = lb.getBlockSize();
      final List<MLocation> locations = new LinkedList<MLocation>();
      for(int i = 0; i < datanodeInfos.length; i++) {
        locations.add(new MLocation(datanodeInfos[i], storageTypes[i], size));
      }
      return locations;
    }
  }

  @VisibleForTesting
  static class StorageTypeDiff {
    final List<StorageType> expected;
    final List<StorageType> existing;

    StorageTypeDiff(List<StorageType> expected, StorageType[] existing) {
      this.expected = new LinkedList<StorageType>(expected);
      this.existing = new LinkedList<StorageType>(Arrays.asList(existing));
    }

    /**
     * Remove the overlap between the expected types and the existing types.
     * @param  ignoreNonMovable ignore non-movable storage types
     *         by removing them from both expected and existing storage type list
     *         to prevent non-movable storage from being moved.
     * @returns if the existing types or the expected types is empty after
     *         removing the overlap.
     */
    boolean removeOverlap(boolean ignoreNonMovable) {
      for(Iterator<StorageType> i = existing.iterator(); i.hasNext(); ) {
        final StorageType t = i.next();
        if (expected.remove(t)) {
          i.remove();
        }
      }
      if (ignoreNonMovable) {
        removeNonMovable(existing);
        removeNonMovable(expected);
      }
      return expected.isEmpty() || existing.isEmpty();
    }

    void removeNonMovable(List<StorageType> types) {
      for (Iterator<StorageType> i = types.iterator(); i.hasNext(); ) {
        final StorageType t = i.next();
        if (!t.isMovable()) {
          i.remove();
        }
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "{expected=" + expected
              + ", existing=" + existing + "}";
    }
  }

  static int run(Map<URI, List<Path>> namenodes, Configuration conf)
          throws IOException, InterruptedException {
    final long sleeptime =
            conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
                    DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 2000 +
                    conf.getLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
                            DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000;
    AtomicInteger retryCount = new AtomicInteger(0);
    LOG.info("namenodes = " + namenodes);

    List<NameNodeConnector> connectors = Collections.emptyList();
    try {
      connectors = NameNodeConnector.newNameNodeConnectors(namenodes,
              Mover.class.getSimpleName(), MOVER_ID_PATH, conf,
              NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS);

      while (connectors.size() > 0) {
        Collections.shuffle(connectors);
        Iterator<NameNodeConnector> iter = connectors.iterator();
        while (iter.hasNext()) {
          NameNodeConnector nnc = iter.next();
          final Mover m = new Mover(nnc, conf, retryCount);
          final ExitStatus r = m.run();

          if (r == ExitStatus.SUCCESS) {
            IOUtils.cleanup(LOG, nnc);
            iter.remove();
          } else if (r != ExitStatus.IN_PROGRESS) {
            if (r == ExitStatus.NO_MOVE_PROGRESS) {
              System.err.println("Failed to move some blocks after "
                      + m.retryMaxAttempts + " retries. Exiting...");
            } else if (r == ExitStatus.NO_MOVE_BLOCK) {
              System.err.println("Some blocks can't be moved. Exiting...");
            } else {
              System.err.println("Mover failed. Exiting with status " + r
                      + "... ");
            }
            // must be an error statue, return
            return r.getExitCode();
          }
        }
        Thread.sleep(sleeptime);
      }
      System.out.println("Mover Successful: all blocks satisfy"
              + " the specified storage policy. Exiting...");
      return ExitStatus.SUCCESS.getExitCode();
    } finally {
      for (NameNodeConnector nnc : connectors) {
        IOUtils.cleanup(LOG, nnc);
      }
    }
  }

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs mover "
            + "[-p <files/dirs> | -f <local file>]"
            + "\n\t-p <files/dirs>\ta space separated list of HDFS files/dirs to migrate."
            + "\n\t-f <local file>\ta local file containing a list of HDFS files/dirs to migrate.";

    private static Options buildCliOptions() {
      Options opts = new Options();
      Option file = OptionBuilder.withArgName("pathsFile").hasArg()
              .withDescription("a local file containing files/dirs to migrate")
              .create("f");
      Option paths = OptionBuilder.withArgName("paths").hasArgs()
              .withDescription("specify space separated files/dirs to migrate")
              .create("p");
      OptionGroup group = new OptionGroup();
      group.addOption(file);
      group.addOption(paths);
      opts.addOptionGroup(group);
      return opts;
    }

    private static String[] readPathFile(String file) throws IOException {
      List<String> list = Lists.newArrayList();
      BufferedReader reader = new BufferedReader(
              new InputStreamReader(new FileInputStream(file), "UTF-8"));
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          if (!line.trim().isEmpty()) {
            list.add(line);
          }
        }
      } finally {
        IOUtils.cleanup(LOG, reader);
      }
      return list.toArray(new String[list.size()]);
    }

    private static Map<URI, List<Path>> getNameNodePaths(CommandLine line,
                                                         Configuration conf) throws Exception {
      Map<URI, List<Path>> map = Maps.newHashMap();
      String[] paths = null;
      if (line.hasOption("f")) {
        paths = readPathFile(line.getOptionValue("f"));
      } else if (line.hasOption("p")) {
        paths = line.getOptionValues("p");
      }
      Collection<URI> namenodes = DFSUtil.getNameNodesRPCAddressesAsURIs(conf);
      if (paths == null || paths.length == 0) {
        for (URI namenode : namenodes) {
          map.put(namenode, null);
        }
        return map;
      }
      final URI singleNs = namenodes.size() == 1 ?
              namenodes.iterator().next() : null;
      for (String path : paths) {
        Path target = new Path(path);
        if (!target.isUriPathAbsolute()) {
          throw new IllegalArgumentException("The path " + target
                  + " is not absolute");
        }
        URI targetUri = target.toUri();
        if ((targetUri.getAuthority() == null || targetUri.getScheme() ==
                null) && singleNs == null) {
          // each path must contains both scheme and authority information
          // unless there is only one name service specified in the
          // configuration
          throw new IllegalArgumentException("The path " + target
                  + " does not contain scheme and authority thus cannot identify"
                  + " its name service");
        }
        URI key = singleNs;
        if (singleNs == null) {
          key = new URI(targetUri.getScheme(), targetUri.getAuthority(),
                  null, null, null);
          if (!namenodes.contains(key)) {
            throw new IllegalArgumentException("Cannot resolve the path " +
                    target + ". The namenode services specified in the " +
                    "configuration: " + namenodes);
          }
        }
        List<Path> targets = map.get(key);
        if (targets == null) {
          targets = Lists.newArrayList();
          map.put(key, targets);
        }
        targets.add(Path.getPathWithoutSchemeAndAuthority(target));
      }
      return map;
    }

    @VisibleForTesting
    static Map<URI, List<Path>> getNameNodePathsToMove(Configuration conf,
                                                       String... args) throws Exception {
      final Options opts = buildCliOptions();
      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(opts, args, true);
      return getNameNodePaths(commandLine, conf);
    }

    @Override
    public int run(String[] args) throws Exception {
      final long startTime = Time.monotonicNow();
      final Configuration conf = getConf();

      try {
        final Map<URI, List<Path>> map = getNameNodePathsToMove(conf, args);
        return Mover.run(map, conf);
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } catch (ParseException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } catch (IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } finally {
        System.out.format("%-24s ", DateFormat.getDateTimeInstance().format(new Date()));
        System.out.println("Mover took " + StringUtils.formatTime(Time.monotonicNow()-startTime));
      }
    }
  }

  private static class Result {

    private boolean hasRemaining;
    private boolean noBlockMoved;
    private boolean retryFailed;

    Result() {
      hasRemaining = false;
      noBlockMoved = true;
      retryFailed = false;
    }

    boolean isHasRemaining() {
      return hasRemaining;
    }

    boolean isNoBlockMoved() {
      return noBlockMoved;
    }

    void updateHasRemaining(boolean hasRemaining) {
      this.hasRemaining |= hasRemaining;
    }

    void setNoBlockMoved(boolean noBlockMoved) {
      this.noBlockMoved = noBlockMoved;
    }

    void setRetryFailed() {
      this.retryFailed = true;
    }

    /**
     * @return NO_MOVE_PROGRESS if no progress in move after some retry. Return
     *         SUCCESS if all moves are success and there is no remaining move.
     *         Return NO_MOVE_BLOCK if there moves available but all the moves
     *         cannot be scheduled. Otherwise, return IN_PROGRESS since there
     *         must be some remaining moves.
     */
    ExitStatus getExitStatus() {
      if (retryFailed) {
        return ExitStatus.NO_MOVE_PROGRESS;
      } else {
        return !isHasRemaining() ? ExitStatus.SUCCESS
                : isNoBlockMoved() ? ExitStatus.NO_MOVE_BLOCK
                : ExitStatus.IN_PROGRESS;
      }
    }

  }
  /**
   * Run a Mover in command line.
   *
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, Cli.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting " + Mover.class.getSimpleName()
              + " due to an exception", e);
      System.exit(-1);
    }
  }
}
