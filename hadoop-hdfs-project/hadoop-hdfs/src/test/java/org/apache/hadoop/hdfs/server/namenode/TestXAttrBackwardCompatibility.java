/*
 * Copyright (C) 2021 LogicalClocks AB.
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
import io.hops.metadata.hdfs.dal.XAttrDataAccess;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;


/*
 * After migrating to NDB 8 the row size of hdfs_xattrs has been increased to 30KB
 * Here are some tests to make sure that hopsfs can read and write old data
 */
public class TestXAttrBackwardCompatibility {
  final int OLD_MAX_XATTR_VALUE_ROW_SIZE = 13500;
  final int NEW_MAX_XATTR_VALUE_ROW_SIZE = 29500;
  final int XATTR_NAME_LENGTH = XAttrStorage.getMaxXAttrNameSize();

  @Test
  public void testBackwardComp1() throws Exception {
    int initialMaxXattrSize, updatedMaxXattrSize, initialSize,  updatedSize;

    // dfs.namenode.fs-limits.max-xattr-size does not change
    // overwrite xattr with large value
    initialMaxXattrSize = 1024*1024;
    updatedMaxXattrSize = initialMaxXattrSize;
    initialSize = 64 * 1024;
    updatedSize = 64 * 1024;
    testBackwardComp(initialMaxXattrSize, // initial max xattrs size,
            updatedMaxXattrSize, // updated max xattrs size,
            initialSize, // initial data length
            (int) Math.ceil((double) initialSize / OLD_MAX_XATTR_VALUE_ROW_SIZE),
            updatedSize, // updated data length
            (int) Math.ceil((double) updatedSize / NEW_MAX_XATTR_VALUE_ROW_SIZE)
    );

    // dfs.namenode.fs-limits.max-xattr-size does not change
    // overwrite xattr with smaller value
    initialMaxXattrSize = 1024*1024;
    updatedMaxXattrSize = initialMaxXattrSize;
    initialSize = 256 * 1024;
    updatedSize = 64 * 1024;
    testBackwardComp(initialMaxXattrSize, // initial max xattrs size,
            updatedMaxXattrSize, // updated max xattrs size,
            initialSize, // initial data length
            (int) Math.ceil((double) initialSize / OLD_MAX_XATTR_VALUE_ROW_SIZE),
            updatedSize, // updated data length
            (int) Math.ceil((double) updatedSize / NEW_MAX_XATTR_VALUE_ROW_SIZE)
    );


    // dfs.namenode.fs-limits.max-xattr-size is increased
    // overwrite xattr with larger value
    initialMaxXattrSize = 1024*1024;
    updatedMaxXattrSize = 2*initialMaxXattrSize;
    initialSize = 256 * 1024;
    updatedSize = updatedMaxXattrSize - XATTR_NAME_LENGTH;
    testBackwardComp(initialMaxXattrSize, // initial max xattrs size,
            updatedMaxXattrSize, // updated max xattrs size,
            initialSize, // initial data length
            (int) Math.ceil((double) initialSize / OLD_MAX_XATTR_VALUE_ROW_SIZE),
            updatedSize, // updated data length
            (int) Math.ceil((double) updatedSize / NEW_MAX_XATTR_VALUE_ROW_SIZE)
    );


    // dfs.namenode.fs-limits.max-xattr-size is increased
    // overwrite xattr with smaller value
    initialMaxXattrSize = 1024*1024;
    updatedMaxXattrSize = 2*initialMaxXattrSize;
    initialSize = initialMaxXattrSize - XATTR_NAME_LENGTH;
    updatedSize = initialSize/2;
    testBackwardComp(initialMaxXattrSize, // initial max xattrs size,
            updatedMaxXattrSize, // updated max xattrs size,
            initialSize, // initial data length
            (int) Math.ceil((double) initialSize / OLD_MAX_XATTR_VALUE_ROW_SIZE),
            updatedSize, // updated data length
            (int) Math.ceil((double) updatedSize / NEW_MAX_XATTR_VALUE_ROW_SIZE)
    );
  }

  public void testBackwardComp(int initialMaxXattrsSize, int updatedMaxXattrsSize,
                               int initialDataLen, int initialXattrRowsCount,
                               int updatedDataLen, int updatedXattrRowsCount) throws Exception {
    MiniDFSCluster cluster = null;
    Path dir0 = new Path("/dir0");
    String name = "user.test";
    byte[] initialValue = XAttrTestHelpers.generateRandomByteArray(initialDataLen);
    byte[] updatedValue = XAttrTestHelpers.generateRandomByteArray(updatedDataLen);

    StoredXAttr.MAX_XATTR_VALUE_ROW_SIZE = OLD_MAX_XATTR_VALUE_ROW_SIZE;
    //----------- write some xattrs with older row size ------------
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 5);
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, initialMaxXattrsSize);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();

      DistributedFileSystem dfs = cluster.getFileSystem();
      dfs.mkdirs(dir0);

      // setXAttrs
      dfs.setXAttr(dir0, name, initialValue);
      checkXattrRowsCount(initialXattrRowsCount);

      byte[] returnedValue = dfs.getXAttr(dir0, name);
      assert Arrays.equals(initialValue, returnedValue);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }

    cluster = null;
    StoredXAttr.MAX_XATTR_VALUE_ROW_SIZE = NEW_MAX_XATTR_VALUE_ROW_SIZE;
    //----------- write some xattrs with older row size ------------
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 5);
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, updatedMaxXattrsSize);
      cluster = new MiniDFSCluster.Builder(conf).format(false).numDataNodes(1).build();

      DistributedFileSystem dfs = cluster.getFileSystem();


      //read the old data
      checkXattrRowsCount(initialXattrRowsCount);
      byte[] returnedValue = dfs.getXAttr(dir0, name);
      assert Arrays.equals(initialValue, returnedValue);

      // setXAttrs
      dfs.setXAttr(dir0, name, updatedValue);
      returnedValue = dfs.getXAttr(dir0, name);
      assert Arrays.equals(updatedValue, returnedValue);
      checkXattrRowsCount(updatedXattrRowsCount);

      dfs.delete(dir0, true);
      checkXattrRowsCount(0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  void checkXattrRowsCount(int expected) throws IOException {
    int got = (int) (new LightWeightRequestHandler(
            HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        XAttrDataAccess da = (XAttrDataAccess) HdfsStorageFactory
                .getDataAccess(XAttrDataAccess.class);
        return da.count();
      }
    }.handle());

    assertTrue("Expected: " + expected + " Got: " + got, got == expected);
  }
}
