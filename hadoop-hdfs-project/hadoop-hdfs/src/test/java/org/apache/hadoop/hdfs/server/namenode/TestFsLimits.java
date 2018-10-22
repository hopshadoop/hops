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

package org.apache.hadoop.hdfs.server.namenode;

import io.hops.common.IDsMonitor;
import io.hops.exception.StorageException;
import io.hops.leader_election.node.SortedActiveNodeListPBImpl;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.security.UsersGroups;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.INodeLock;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hadoop.util.Time.now;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFsLimits {
  static Configuration conf;
  static INode[] inodes;
  static FSDirectory fs;
  static boolean fsIsReady;
  
  static PermissionStatus perms =
      new PermissionStatus("admin", "admin", FsPermission.getDefault());

  
  static private FSNamesystem getMockNamesystem() {
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(fsn.createFsOwnerPermissions((FsPermission) anyObject())).thenReturn(
        new PermissionStatus("root", "wheel", FsPermission.getDefault()));
    NameNode nn = mock(NameNode.class);
    when(nn.getActiveNameNodes())
        .thenReturn(new SortedActiveNodeListPBImpl(Collections.EMPTY_LIST));
    when(fsn.getNameNode()).thenReturn(nn);
    return fsn;
  }
  
  private void initFS() throws StorageException, IOException {
    HdfsStorageFactory.setConfiguration(conf);
    assert (HdfsStorageFactory.formatStorage());
    UsersGroups.addUserToGroup(perms.getUserName(), perms.getGroupName());
    IDsMonitor.getInstance().start();
    NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    fs = null;
    fsIsReady = true;
  }

  private static class MockFSDirectory extends FSDirectory {
    public MockFSDirectory() throws IOException {
      super(getMockNamesystem(), conf);
      setReady(fsIsReady);
    }
    
  }

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_QUOTA_ENABLED_KEY,
            false);
    initFS();
  }

  @Test
  public void testNoLimits() throws Exception {
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", null);
    mkdirs("/4444", null);
    mkdirs("/55555", null);
  }

  @Test
  public void testMaxComponentLength() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", PathComponentTooLongException.class);
    mkdirs("/4444", PathComponentTooLongException.class);
  }

  @Test
  public void testMaxComponentLengthRename() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 2);
    mkdirs("/5", null);
    rename("/5", "/555", PathComponentTooLongException.class);
    rename("/5", "/55", null);
    mkdirs("/6", null);
    deprecatedRename("/6", "/666", PathComponentTooLongException.class);
    deprecatedRename("/6", "/66", null);
  }

  @Test
  public void testMaxDirItems() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", MaxDirectoryItemsExceededException.class);
    mkdirs("/4444", MaxDirectoryItemsExceededException.class);
  }
  
    @Test
  public void testMaxDirItemsRename() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/2", null);
    mkdirs("/2/A", null);
    rename("/2/A", "/A", MaxDirectoryItemsExceededException.class);
    rename("/2/A", "/1/A", null);
    mkdirs("/2/B", null);
    deprecatedRename("/2/B", "/B", MaxDirectoryItemsExceededException.class);
    deprecatedRename("/2/B", "/1/B", null);
    rename("/1", "/3", null);
    deprecatedRename("/2", "/4", null);
  }
  
  @Test
  public void testMaxDirItemsLimits() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 0);
    try {
      mkdirs("/1", null);
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Cannot set dfs", e);
    }
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 64*100*1024);
    try {
      mkdirs("/1", null);
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Cannot set dfs", e);
    }
  }

  @Test
  public void testMaxComponentsAndMaxDirItems() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", MaxDirectoryItemsExceededException.class);
    mkdirs("/4444", PathComponentTooLongException.class);
  }

  @Test
  public void testDuringEditLogs() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    fsIsReady = false;
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", null);
    mkdirs("/4444", null);
  }

  private static long id = 1 + INodeDirectory.ROOT_INODE_ID;

//  private void mkdirs(String name, Class<?> expected)
//      throws Exception {
//    HopsTransactionalRequestHandler handler =
//        new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
//
//          @Override
//          public void acquireLock(TransactionLocks locks) throws IOException {
//            LockFactory lf = LockFactory.getInstance();
//            INodeLock il = lf.getINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
//                    TransactionLockTypes.INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN, "/", "/" + name)
//                    .setNameNodeID(getMockNamesystem().getNameNode().getId()).setActiveNameNodes(getMockNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
//            locks.add(il);
//          }
//
//          @Override
//          public Object performTask() throws StorageException, IOException {
//            // have to create after the caller has had a chance to set conf values
//            if (fs == null) {
//              fs = new MockFSDirectory();
//            }
//
//            INode child = new INodeDirectory(id++, name, perms, true);
//            child.setLocalName(name);
//            child.setPartitionIdNoPersistance(INodeDirectory.ROOT_INODE_ID);
//
//            Class<?> generated = null;
//            try {
//              fs.verifyFsLimits(inodes, 1, child);
//              INodeDirectory.getRootDir().addChild(child, false);
//            } catch (QuotaExceededException e) {
//              generated = e.getClass();
//            }
//            assertEquals(expected, generated);
//            return null;
//          }
//        };
//
//    handler.handle();
//    
//  }
  
  private void mkdirs(final String name, final Class<?> expected)
      throws Exception {
    lazyInitFSDirectory();
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getINodeLock( TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT, TransactionLockTypes.INodeResolveType.PATH, name)
                    .setNameNodeID(getMockNamesystem().getNameNode().getId())
                    .setActiveNameNodes(getMockNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
            locks.add(il);
            locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        Class<?> generated = null;
        try {
          fs.mkdirs(name, perms, false, now());
        } catch (Throwable e) {
          generated = e.getClass();
        }
        assertEquals(expected, generated);
        return null;
      }
    };

    handler.handle();
  }

  private void rename(final String src, final String dst, final Class<?> expected)
      throws Exception {
    lazyInitFSDirectory();
    new HopsTransactionalRequestHandler(HDFSOperationType.RENAME, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getRenameINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            TransactionLockTypes.INodeResolveType.PATH, src, dst)
            .setNameNodeID(getMockNamesystem().getNameNode().getId())
            .setActiveNameNodes(getMockNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        Class<?> generated = null;
        try {
          fs.renameTo(src, dst, now(), new INode.DirCounts(), new INode.DirCounts(), new Rename[]{});
        } catch (Throwable e) {
          LOG.error(e,e);
          generated = e.getClass();
        }
        assertEquals(expected, generated);
        return null;
      }
    }.handle();
  }

  @SuppressWarnings("deprecation")
  private void deprecatedRename(final String src, final String dst, final Class<?> expected)
      throws Exception {
    lazyInitFSDirectory();
    new HopsTransactionalRequestHandler(HDFSOperationType.RENAME, src) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        INodeLock il = lf.getRenameINodeLock(TransactionLockTypes.INodeLockType.WRITE_ON_TARGET_AND_PARENT,
            TransactionLockTypes.INodeResolveType.PATH, src, dst)
            .setNameNodeID(getMockNamesystem().getNameNode().getId())
            .setActiveNameNodes(getMockNamesystem().getNameNode().getActiveNameNodes().getActiveNodes());
        locks.add(il);
        locks.add(lf.getAcesLock());
      }

      @Override
      public Object performTask() throws IOException {
        Class<?> generated = null;
        try {
          fs.renameTo(src, dst, now(), new INode.DirCounts(), new INode.DirCounts());
        } catch (Throwable e) {
          generated = e.getClass();
        }
        assertEquals(expected, generated);
        return null;
      }
    }.handle();
  }

  private static void lazyInitFSDirectory() throws IOException {
    // have to create after the caller has had a chance to set conf values
    if (fs == null) {
      fs = new MockFSDirectory();
    }
  }
}
