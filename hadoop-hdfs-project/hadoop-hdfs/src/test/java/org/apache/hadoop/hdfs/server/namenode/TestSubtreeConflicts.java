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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestSubtreeConflicts extends TestCase {
  static final Log LOG = LogFactory.getLog(TestSubtreeConflicts.class);
  
  static boolean fail=false;
  static String failMessage="";


  @Test
  public void testWithOutHierarchialLock() throws IOException, InterruptedException {

    fail = false;
    failMessage = "";

    subtreeLocking1(false); // no hierarchical locking
    int count = TestSubtreeLock.countAllSubTreeLocks();
    if(count != 2){
      fail("Expecting 2 STO locks without hierarchical locking mechanism. Found "+count);
    }
  }

  @Test
  public void testHierarchialLock() throws IOException, InterruptedException {

    fail = false;
    failMessage = "";

    subtreeLocking1(true); //with hierarchial locking only one operations will succeed
    int count = TestSubtreeLock.countAllSubTreeLocks();

    if(count != 1){
      fail("Expecting 1 STO locks without hierarchical locking mechanism. Found "+count);
    }

    if(!fail){
      fail("Expecting one STO lock operation to fail. But it did not fail");
    }

  }

  public void subtreeLocking1(boolean hierarchicalLocking) throws IOException,
          InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
//      conf.setInt(DFSConfigKeys.DFS_SUBTREE_EXECUTOR_LIMIT_KEY,1);
      conf.setBoolean(DFSConfigKeys.DFS_SUBTREE_HIERARCHICAL_LOCKING_KEY,hierarchicalLocking);
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).build();
      cluster.waitActive();


      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();

      dfs.mkdir(new Path("/A"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/C"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B/D"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B/E"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/C/F"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/C/G"), FsPermission.getDefault());

      // Trying to create a scenario where two clients are able to acquire STO Lock
      // on the same subtree.

      namesystem.setTestingSTO(true);

      Thread t1 = new Thread() {
        @Override
        public void run() {
          try {
            //before committing the lock wait
            namesystem.setDelayBeforeSTOFlag(800);
            namesystem.setDelayAfterBuildingTree(2000);
            INodeIdentifier inode = namesystem.lockSubtree("/A/C", SubTreeOperation.Type.NA);
          } catch (Exception e) {
            fail=true;
            failMessage=e.toString();
          }
        }
      };


      Thread t2 = new Thread() {
        @Override
        public void run() {
          try {
            //before committing the lock wait
            namesystem.setDelayBeforeSTOFlag(100);
            namesystem.setDelayAfterBuildingTree(2000);
            INodeIdentifier inode = namesystem.lockSubtree("/A", SubTreeOperation.Type.NA);
          } catch (Exception e) {
            fail=true;
            failMessage=e.toString();
          }
        }
      };


      t1.start();
      Thread.sleep(100);
      t2.start();

      t1.join();
      t2.join();


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  @Test
  public void testConcurrentSTOandInodeOps() throws IOException, InterruptedException {
    
    fail = false;
    failMessage = "";

    subtreeLocking2();
    assertFalse(failMessage,fail);
  }

  /*
  Testing concurrent sub tree delete operation and inode delete operation.
   */
  public void subtreeLocking2() throws IOException,
          InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
//      conf.setInt(DFSConfigKeys.DFS_SUBTREE_EXECUTOR_LIMIT_KEY,1);
      conf.setInt(DFSConfigKeys.DFS_DIR_DELETE_BATCH_SIZE, 0); // do not batch delete ops

      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).build();
      cluster.waitActive();


      final DistributedFileSystem dfs = cluster.getFileSystem();
      final FSNamesystem namesystem = cluster.getNamesystem();

      dfs.mkdir(new Path("/A"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/C"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B/D"), FsPermission.getDefault());
      dfs.mkdir(new Path("/A/B/E"), FsPermission.getDefault());
      TestFileCreation.create(dfs, new Path("/A/C/F"),1 ).close();

      // Trying to create a scenario where two file system operations
      // run concurrently on the same sub tree. One of the FS operation is a
      // subtree operation while the other is an inode operation.

      // /A is dir and /A/C/F is a file.
      // Delete /A/C/F operation is already running, however, it has not
      // yet acquired write lock on the /A/C/F file. The subtree operation
      // starts and it quiesces the subtree. As the /A/C/F has not write
      // locked the file therefore it is possible that the deletion of the file
      // happens after the subtree operation has quiesced the subtree.
      // The subtree operation must not fail.


      namesystem.setTestingSTO(true);
      Thread t1 = new Thread() {
        @Override
        public void run() {
          try {
            namesystem.setDelayAfterBuildingTree(5000);
            if(!dfs.delete(new Path("/A"))){
              fail=true;
              failMessage="Deleting /A should not have failed";
            }
          } catch (Exception e) {
            fail=true;
            failMessage=e.toString();
          }
        }
      };


      t1.start();
      Thread.sleep(3000); // sleep to make sure that the tree is quiesced
      deleteINode("F" );
      namesystem.setDelayAfterBuildingTree(0);

      t1.join();


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public static void deleteINode(final String inodeName) throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
                        .getDataAccess(INodeDataAccess.class);
                ida.deleteInode(inodeName);
                LOG.debug("Testing STO Deleted inode "+inodeName);
                return null;
              }
            };
    subTreeLockChecker.handle();
  }
}
