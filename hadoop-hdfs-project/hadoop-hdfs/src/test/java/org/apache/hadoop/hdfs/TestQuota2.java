/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.DirectoryWithQuotaFeatureDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.namenode.AbstractFileTree;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.PathInformation;
import org.apache.hadoop.hdfs.server.namenode.QuotaUpdateManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A class for testing quota-related commands
 */
public class TestQuota2 {
  public static final Log LOG = LogFactory.getLog(TestQuota2.class);
  private final int NUM_NAMENODES = 2;
  private Random rand = new Random(System.currentTimeMillis());


  static {
    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getLogger(TestQuota2.class).setLevel(Level.ALL);
    Logger.getLogger(QuotaUpdateManager.class).setLevel(Level.ALL);
  }

  @Test
  public void testQuotaPriotizedUpdates() {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 3;
      final int BLKSIZE = 4 * 1024 * 1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
        nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES)).
        format(true).build();
      cluster.waitActive();

      List<MiniDFSCluster.DataNodeProperties> dnpList =
        new CopyOnWriteArrayList(cluster.getDataNodeProperties());
      assert dnpList.size() == NUM_DN;

      DistributedFileSystem dfs = cluster.getFileSystem(rand.nextInt(NUM_NAMENODES));

      dfs.setStoragePolicy(new Path("/"), "DB");

      String baseDir = "/some/dir/for/writing";
      printQuota(dfs);

      dfs.mkdirs(new Path(baseDir));
      long nsQ = 1024;
      long dsQ = 10 * 1024 * 1024;
      dfs.setQuota(new Path("/some/dir/for/writing"), nsQ, dsQ);
      dfs.setQuota(new Path("/some/dir/for"), nsQ, dsQ);
      dfs.setQuota(new Path("/some/dir"), nsQ, dsQ);
      dfs.setQuota(new Path("/some"), nsQ, dsQ);

      Thread.sleep(3000);
      printQuota(dfs);
      showQuotaTable("After creating Dirs");

      getActiveQuotaManager(cluster).pauseAsyncOps = true;

      for (int i = 0; i < 55; i++) {
        FSDataOutputStream out = dfs.create(new Path(baseDir + "/large-file" + i), (short) 3);
        HopsFilesTestHelper.writeData(out, 0, 1024);
        out.close();
      }
      Thread.sleep(3000);
      printQuota(dfs);
      showQuotaTable("After adding files");


      final FSNamesystem fsn = cluster.getNamesystem(rand.nextInt(NUM_NAMENODES));
      final FSDirectory fsd = fsn.getFSDirectory();
      PathInformation pathInfo = fsn.getPathExistingINodesFromDB("/",
        false, null, FsAction.WRITE, null, null);
      INodeIdentifier subtreeRoot = INodeDirectory.getRootIdentifier();
      List<AclEntry> nearestDefaultsForSubtree = fsn.calculateNearestDefaultAclForSubtree(pathInfo);
      AbstractFileTree.FileTree fileTree = new AbstractFileTree.FileTree(fsn, subtreeRoot,
        FsAction.ALL, true, nearestDefaultsForSubtree, subtreeRoot.getStoragePolicy());
      fileTree.buildUp(fsd.getBlockStoragePolicySuite());

      for (int i = fileTree.getHeight(); i > 0; i--) {
        Collection<ProjectedINode> dirs = fileTree.getDirsByLevel(i);
        List<Long> dirIDs = new ArrayList<>();
        for (ProjectedINode dir : dirs) {
          dirIDs.add(dir.getId());
        }

        Iterator<Long> itr = dirIDs.iterator();
        synchronized (itr) {
          getActiveQuotaManager(cluster).addPrioritizedUpdates(itr);
          itr.wait();
          LOG.error("Waiting for prioritized update for level " + i);
        }
        showQuotaTable("Prioritized update for level " + i + " applied. INode IDs: " + Arrays.toString(dirIDs.toArray()));
      }


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testQuotaPrioritizedUpdatesWithDepricatedRename() {
    testQuotaPrioritizedUpdatesWithRename(true);
  }

  @Test
  public void testQuotaPrioritizedUpdatesWithNewRename() {
    testQuotaPrioritizedUpdatesWithRename(false);
  }

  public void testQuotaPrioritizedUpdatesWithRename(boolean depricatedRename) {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 3;
      final int BLKSIZE = 4 * 1024 * 1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
        nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES)).
        format(true).build();
      cluster.waitActive();


      List<MiniDFSCluster.DataNodeProperties> dnpList =
        new CopyOnWriteArrayList(cluster.getDataNodeProperties());
      assert dnpList.size() == NUM_DN;


      DistributedFileSystem dfs = cluster.getFileSystem(rand.nextInt(NUM_NAMENODES));

      dfs.setStoragePolicy(new Path("/"), "DB");

      String baseDir = "/some/dir/for/writing";
      String renameDest = "/some/other";

      long DSQ = 100 * 1024 * 1024;
      long NSQ = 1024;
      dfs.mkdirs(new Path(baseDir));
      dfs.setQuota(new Path("/some/dir/for/writing"), NSQ, DSQ);
      dfs.setQuota(new Path("/some/dir/for"), NSQ, DSQ);
      dfs.setQuota(new Path("/some/dir"), NSQ, DSQ);
      dfs.setQuota(new Path("/some"), NSQ, DSQ);

      dfs.mkdirs(new Path(renameDest));
      dfs.setQuota(new Path(renameDest), NSQ, DSQ);

      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After creating Dirs");

      //stop QuotaManager
      getActiveQuotaManager(cluster).pauseAsyncOps = true;

      for (int i = 0; i < 25; i++) {
        FSDataOutputStream out = dfs.create(new Path(baseDir + "/file" + i), (short) 3);
        HopsFilesTestHelper.writeData(out, 0, 1024);
        out.close();
      }

      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After adding files");


      if (depricatedRename) {
        dfs.rename(new Path(baseDir), new Path(renameDest));
      } else {
        dfs.rename(new Path(baseDir), new Path(renameDest), Options.Rename.OVERWRITE);
      }

      Thread.sleep(3000);
      printQuota(dfs);
      TestQuota2.showQuotaTable("After moving files ");


      getActiveQuotaManager(cluster).pauseAsyncOps = false;
      Thread.sleep(5000);
      printQuota(dfs);
      TestQuota2.showQuotaTable("After unpause Quota manager ");
      TestQuota2.checkAllQuotaEntries();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  //Note: there is no testQuotaRename(true, true) as
  //deprecated rename does not overwrite

  @Test
  public void testQuotaNonDeprecatedRenameFileWithOverwrite() throws Exception {
    testQuotaRename(false, true);
  }

  @Test
  public void testQuotaDeprecatedRenameFile() throws Exception {
    testQuotaRename(true, false);
  }

  @Test
  public void testQuotaNonDeprecatedRenameFile() throws Exception {
    testQuotaRename(false, false);
  }

  public void testQuotaRename(boolean deprecatedRename, boolean overwrite) throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 3;
      final int BLKSIZE = 4 * 1024 * 1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
        nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES)).
        format(true).build();
      cluster.waitActive();


      List<MiniDFSCluster.DataNodeProperties> dnpList =
        new CopyOnWriteArrayList(cluster.getDataNodeProperties());
      assert dnpList.size() == NUM_DN;


      DistributedFileSystem dfs = cluster.getFileSystem(rand.nextInt(NUM_NAMENODES));

      dfs.setStoragePolicy(new Path("/"), "DB");

      String baseDir = "/some/dir/for/writing";
      String srcFile = baseDir + "/file";
      String renameDestDir = "/some/other";
      String renameDestFile = "/some/other/file";

      long DSQ = 100 * 1024 * 1024;
      long NSQ = 1024;
      final int FILE_SIZE = 1024;
      dfs.mkdirs(new Path(baseDir));
      dfs.setQuota(new Path("/some/dir/for/writing"), NSQ, DSQ);
      dfs.setQuota(new Path("/some/dir/for"), NSQ, DSQ);
      dfs.setQuota(new Path("/some/dir"), NSQ, DSQ);
      dfs.setQuota(new Path("/some"), NSQ, DSQ);

      dfs.mkdirs(new Path(renameDestDir));
      dfs.setQuota(new Path(renameDestDir), NSQ, DSQ);

      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After creating Dirs");

      //stop QuotaManager
      getActiveQuotaManager(cluster).pauseAsyncOps = true;

      FSDataOutputStream out = dfs.create(new Path(srcFile), (short) 3);
      HopsFilesTestHelper.writeData(out, 0, FILE_SIZE);
      out.close();

      if (overwrite) {
        out = dfs.create(new Path(renameDestFile), (short) 3);
        HopsFilesTestHelper.writeData(out, 0, FILE_SIZE);
        out.close();
      }

      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After addin files");


      if (deprecatedRename) {
        assert dfs.rename(new Path(srcFile), new Path(renameDestFile));
      } else {
        dfs.rename(new Path(srcFile), new Path(renameDestFile), Options.Rename.OVERWRITE);
      }

      Thread.sleep(3000);
      printQuota(dfs);
      TestQuota2.showQuotaTable("After moving files ");


      getActiveQuotaManager(cluster).pauseAsyncOps = false;
      Thread.sleep(5000);
      printQuota(dfs);
      TestQuota2.showQuotaTable("After unpause Quota manager ");

      QuotaUsage qu = dfs.getLastUpdatedContentSummary(new Path(renameDestDir));
      assertTrue("Expecting: " + FILE_SIZE + " Got: " + qu.getSpaceConsumed(),
        qu.getSpaceConsumed() == FILE_SIZE);
      assertTrue("Expecting: 1 Got: " + qu.getFileAndDirectoryCount(),
        qu.getFileAndDirectoryCount() == 2);

      qu = dfs.getLastUpdatedContentSummary(new Path("/"));
      assertTrue("Expecting: " + FILE_SIZE + " Got: " + qu.getSpaceConsumed(),
        qu.getSpaceConsumed() == FILE_SIZE);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testQuotaDeleteFile() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 3;
      final int BLKSIZE = 4 * 1024 * 1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
        nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES)).
        format(true).build();
      cluster.waitActive();


      List<MiniDFSCluster.DataNodeProperties> dnpList =
        new CopyOnWriteArrayList(cluster.getDataNodeProperties());
      assert dnpList.size() == NUM_DN;


      DistributedFileSystem dfs = cluster.getFileSystem(rand.nextInt(NUM_NAMENODES));

      String baseDir = "/some/dir/for/writing";
      String srcFile = baseDir + "/file";

      long DSQ = 100 * 1024 * 1024;
      long NSQ = 1024;
      final int FILE_SIZE = 1024;
      dfs.mkdirs(new Path(baseDir));
      dfs.setQuota(new Path("/some/dir/for/writing"), NSQ, DSQ);
      dfs.setQuota(new Path("/some/dir/for"), NSQ, DSQ);
      dfs.setQuota(new Path("/some/dir"), NSQ, DSQ);
      dfs.setQuota(new Path("/some"), NSQ, DSQ);

      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After creating Dirs");

      //stop QuotaManager
      getActiveQuotaManager(cluster).pauseAsyncOps = true;

      FSDataOutputStream out = dfs.create(new Path(srcFile), (short) 3);
      HopsFilesTestHelper.writeData(out, 0, FILE_SIZE);
      out.close();


      dfs.delete(new Path(srcFile), true);


      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After deleting file");

      getActiveQuotaManager(cluster).pauseAsyncOps = false;
      Thread.sleep(3000);
      printQuota(dfs);
      TestQuota2.showQuotaTable("After unpause Quota manager ");

      QuotaUsage qu = dfs.getLastUpdatedContentSummary(new Path(baseDir));
      assertTrue("Expecting: " + 0 + " Got: " + qu.getSpaceConsumed(),
        qu.getSpaceConsumed() == 0);
      assertTrue("Expecting: 1 Got: " + qu.getFileAndDirectoryCount(),
        qu.getFileAndDirectoryCount() == 1);

      qu = dfs.getLastUpdatedContentSummary(new Path("/"));
      assertTrue("Expecting: " + 0 + " Got: " + qu.getSpaceConsumed(),
        qu.getSpaceConsumed() == 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testQuotaOverwriteFile() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 3;
      final int BLKSIZE = 4 * 1024 * 1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
        nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES)).
        format(true).build();
      cluster.waitActive();


      List<MiniDFSCluster.DataNodeProperties> dnpList =
        new CopyOnWriteArrayList(cluster.getDataNodeProperties());
      assert dnpList.size() == NUM_DN;


      DistributedFileSystem dfs = cluster.getFileSystem(rand.nextInt(NUM_NAMENODES));

      dfs.setStoragePolicy(new Path("/"), "DB");

      String baseDir = "/some/dir/for/writing";
      String srcFile = baseDir + "/file";

      long DSQ = 100 * 1024 * 1024;
      long NSQ = 1024;
      final int FILE_SIZE = 1024;
      dfs.mkdirs(new Path(baseDir));
      dfs.setQuota(new Path("/some/dir/for/writing"), NSQ, DSQ);
      dfs.setQuota(new Path("/some/dir/for"), NSQ, DSQ);
      dfs.setQuota(new Path("/some/dir"), NSQ, DSQ);
      dfs.setQuota(new Path("/some"), NSQ, DSQ);

      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After creating Dirs");

      //stop QuotaManager
      getActiveQuotaManager(cluster).pauseAsyncOps = true;

      FSDataOutputStream out = dfs.create(new Path(srcFile), (short) 3);
      HopsFilesTestHelper.writeData(out, 0, FILE_SIZE);
      out.close();


      out = dfs.create(new Path(srcFile), (short) 3);
      HopsFilesTestHelper.writeData(out, 0, FILE_SIZE / 2);
      out.close();


      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After overwriting file");

      getActiveQuotaManager(cluster).pauseAsyncOps = false;
      Thread.sleep(3000);
      printQuota(dfs);
      TestQuota2.showQuotaTable("After unpause Quota manager ");

      QuotaUsage qu = dfs.getLastUpdatedContentSummary(new Path(baseDir));
      assertTrue("Expecting: " + FILE_SIZE / 2 + " Got: " + qu.getSpaceConsumed(),
        qu.getSpaceConsumed() == FILE_SIZE / 2);
      assertTrue("Expecting: 1 Got: " + qu.getFileAndDirectoryCount(),
        qu.getFileAndDirectoryCount() == 2);

      qu = dfs.getLastUpdatedContentSummary(new Path("/"));
      assertTrue("Expecting: " + FILE_SIZE / 2 + " Got: " + qu.getSpaceConsumed(),
        qu.getSpaceConsumed() == FILE_SIZE / 2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test
  public void testQuotaRenameDstDir() {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 3;
      final int BLKSIZE = 4 * 1024 * 1024;
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
        nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NUM_NAMENODES)).
        format(true).build();
      cluster.waitActive();


      List<MiniDFSCluster.DataNodeProperties> dnpList =
        new CopyOnWriteArrayList(cluster.getDataNodeProperties());
      assert dnpList.size() == NUM_DN;


      DistributedFileSystem dfs = cluster.getFileSystem(rand.nextInt(NUM_NAMENODES));

      dfs.setStoragePolicy(new Path("/"), "DB");

      String baseDir = "/some/dir/for/writing";
      String renameDest = "/some/other/writing";

      dfs.mkdirs(new Path(baseDir));
      dfs.mkdirs(new Path(renameDest));

      Thread.sleep(3000);
      TestQuota2.showQuotaTable("After creating Dirs");

      //stop QuotaManager

      for (int i = 0; i < 5; i++) {
        FSDataOutputStream out = dfs.create(new Path(baseDir + "/file" + i), (short) 3);
        HopsFilesTestHelper.writeData(out, 0, 1024);
        out.close();
      }

      for (int i = 0; i < 5; i++) {
        FSDataOutputStream out = dfs.create(new Path(renameDest + "/file" + i), (short) 3);
        HopsFilesTestHelper.writeData(out, 0, 1024 * 2);
        out.close();
      }

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      TestQuota2.showQuotaTable("After adding files");

      getActiveQuotaManager(cluster).pauseAsyncOps = true;
      Thread.sleep(3000);

      // delete all files in the dst folder
      for (int i = 0; i < 5; i++) {
        dfs.delete(new Path(renameDest + "/file" + i));
      }

      TestQuota2.showQuotaTable("After deleting files");

      // now the destination folder is empty, we can overwrite it using rename operation
      dfs.rename(new Path(baseDir), new Path(renameDest), Options.Rename.OVERWRITE);

      // not testing deprecate rename as it can not overwrite

      Thread.sleep(3000);
      printQuota(dfs);
      TestQuota2.showQuotaTable("After moving folder ");

      getActiveQuotaManager(cluster).pauseAsyncOps = false;

      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      TestQuota2.showQuotaTable("After unpause Quota manager ");

      //delete every thing
      dfs.delete(new Path("/some"));
      DFSTestUtil.waitForQuotaUpdatesToBeApplied();
      printQuota(dfs);
      TestQuota2.showQuotaTable("After deleting everything");
      QuotaUsage qu = dfs.getLastUpdatedContentSummary(new Path("/"));
      assert qu.getSpaceConsumed() == 0;
      assert qu.getFileAndDirectoryCount() == 1;
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public static void showQuotaTable(String msg) {
    try {
      new LightWeightRequestHandler(HDFSOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          HdfsStorageFactory.getConnector().beginTransaction();
          QuotaUpdateDataAccess da = (QuotaUpdateDataAccess) HdfsStorageFactory
            .getDataAccess(QuotaUpdateDataAccess.class);
          List<QuotaUpdate> list = da.findLimited(1000);
          LOG.info("====================== Show Tables( " + msg + " ) ======================");
          LOG.info("---------------------- Quota Updates table -----------------------------");
          int index = 0;

          Collections.sort(list, new Comparator<QuotaUpdate>() {
            @Override
            public int compare(QuotaUpdate o1, QuotaUpdate o2) {
              return Long.compare(o1.getInodeId(), o2.getInodeId());
            }
          });
          for (QuotaUpdate qu : list) {
            LOG.info(index++ + ") " + qu);
          }
          LOG.info("--------------------- INodes Quota Features Table ----------------------");

          INodeDataAccess ida =
            (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
          List<INode> allINodes = ida.allINodes();
          List<INodeCandidatePrimaryKey> qDirs = new ArrayList<>();
          for (INode inode : allINodes) {
            if (inode.isDirectory()) {
              qDirs.add(new INodeCandidatePrimaryKey(inode.getId()));
            }

          }

          DirectoryWithQuotaFeatureDataAccess dqa =
            (DirectoryWithQuotaFeatureDataAccess) HdfsStorageFactory.getDataAccess(DirectoryWithQuotaFeatureDataAccess.class);
          List<DirectoryWithQuotaFeature> quotaValues = (List<DirectoryWithQuotaFeature>) dqa.findAttributesByPkList(qDirs);

          Collections.sort(quotaValues, new Comparator<DirectoryWithQuotaFeature>() {
            @Override
            public int compare(DirectoryWithQuotaFeature o1, DirectoryWithQuotaFeature o2) {
              if (o1 != null && o2 != null) {
                return Long.compare(o1.getInodeId(), o2.getInodeId());
              }
              return 0;
            }
          });

          index = 0;
          for (DirectoryWithQuotaFeature dwqa : quotaValues) {
            if (dwqa != null) {
              LOG.info(index++ + ") ID: " + dwqa.getInodeId() + " NS: " + dwqa.getQuota().getNameSpace() + " DS " + dwqa.getSpaceConsumed());
            }
          }
          LOG.info("========================================================================");
          HdfsStorageFactory.getConnector().commit();
          return null;
        }
      }.handle();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void checkAllQuotaEntries() {
    try {
      new LightWeightRequestHandler(HDFSOperationType.TEST) {
        @Override
        public Object performTask() throws IOException {
          HdfsStorageFactory.getConnector().beginTransaction();

          INodeDataAccess ida =
            (INodeDataAccess) HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
          List<INode> allINodes = ida.allINodes();
          List<INodeCandidatePrimaryKey> qDirs = new ArrayList<>();
          for (INode inode : allINodes) {
            if (inode.isDirectory()) {
              qDirs.add(new INodeCandidatePrimaryKey(inode.getId()));
            }
          }

          DirectoryWithQuotaFeatureDataAccess dqa =
            (DirectoryWithQuotaFeatureDataAccess) HdfsStorageFactory.getDataAccess(DirectoryWithQuotaFeatureDataAccess.class);
          List<DirectoryWithQuotaFeature> quotaValues = (List<DirectoryWithQuotaFeature>) dqa.findAttributesByPkList(qDirs);

          for (DirectoryWithQuotaFeature qv : quotaValues) {
            assertTrue("Negative quota for ID: " + qv.getInodeId(),
              qv.getSpaceConsumed().getNameSpace() >= 0);
            assertTrue("Negative quota for ID: " + qv.getInodeId(),
              qv.getSpaceConsumed().getStorageSpace() >= 0);
          }

          HdfsStorageFactory.getConnector().commit();
          return null;
        }
      }.handle();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private QuotaUpdateManager getActiveQuotaManager(MiniDFSCluster cluster) {
    for (int i = 0; i < NUM_NAMENODES; i++) {
      if (cluster.getNamesystem(i).isLeader()) {
        return cluster.getNamesystem(i).getQuotaUpdateManager();
      }
    }
    throw new IllegalStateException("No leader NN found");
  }

  private void printQuota(DistributedFileSystem dfs) throws IOException {
    QuotaUsage q = dfs.getLastUpdatedContentSummary(new Path("/"));
    LOG.info("Capacity: " + dfs.getStatus().getCapacity() + " Used: " + dfs.getStatus().getUsed() +
      " Remaining: " + dfs.getStatus().getRemaining());
    LOG.info("File and dir count: " + q.getFileAndDirectoryCount());
    LOG.info("SpaceQuota: " + q.getSpaceQuota() + " Consumed: " + q.getSpaceConsumed());
  }
}
