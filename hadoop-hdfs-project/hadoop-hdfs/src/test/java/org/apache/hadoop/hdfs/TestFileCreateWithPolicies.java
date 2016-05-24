/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.StorageType.*;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFileCreateWithPolicies {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;

  static final int numDatanodes = 4;
  static final int storagesPerDN = 3;
  static final StorageType[][] storageTypes = {
      {DISK, DISK, DISK}, // sid 0,1,2
      {DISK, DISK, SSD}, // sid 3,4,5
      {DISK, DISK, SSD}, // sid 6,7,8
      {SSD, SSD, SSD}, // sid 9,10,11
  };

  static final Log LOG = LogFactory.getLog(TestFileCreateWithPolicies.class);

  // creates a file but does not close it
  public static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl) throws IOException {
    System.out.println("createFile: Created " + name + " with " + repl + " replica.");

    FSDataOutputStream stm = fileSys.create(name, true, 4096, (short) repl, blockSize);

    return stm;
  }

  String[] policyNames = {
      ALLSSD_STORAGE_POLICY_NAME,
      WARM_STORAGE_POLICY_NAME,
      ONESSD_STORAGE_POLICY_NAME
  };

  /**
   * Test setting the storage policy of the root folder
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testConfigKeyEnabled() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();

    try {
      cluster.waitActive();
      cluster.getFileSystem().setStoragePolicy(new Path("/"), COLD_STORAGE_POLICY_NAME);
    } finally {
      cluster.shutdown();
    }
  }

  @Test (timeout=300000)
  public void testGetStorageType() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();

    cluster.waitActive();

    DistributedFileSystem dfs = cluster.getFileSystem();
    DFSClient client = dfs.getClient();

    final byte defaultPolicy = BlockStoragePolicySuite.getDefaultPolicy().getId();

    try {
      dfs.mkdir(new Path("/foo"), FsPermission.getDirDefault());
      dfs.mkdir(new Path("/foo/bar"), FsPermission.getDirDefault());
      dfs.mkdir(new Path("/foo/bar2"), FsPermission.getDirDefault());
      dfs.create(new Path("/foo/bar/file1.txt"));
      dfs.create(new Path("/foo/bar/file2.txt"));
      dfs.create(new Path("/foo/bar2/file3.txt"));

      check(client, "/", defaultPolicy);
      check(client, "/foo", defaultPolicy);
      check(client, "/foo/bar", defaultPolicy);
      check(client, "/foo/bar/file1.txt", defaultPolicy);
      check(client, "/foo/bar/file2.txt", defaultPolicy);

      // Now change some policies
      dfs.setStoragePolicy(new Path("/foo/bar/file1.txt"), ALLSSD_STORAGE_POLICY_NAME);
      check(client, "/foo/bar", defaultPolicy);
      check(client, "/foo/bar/file1.txt", ALLSSD_STORAGE_POLICY_ID);
      check(client, "/foo/bar/file2.txt", defaultPolicy);

      // Now set the folder storage policy
      dfs.setStoragePolicy(new Path("/foo/bar"), COLD_STORAGE_POLICY_NAME);
      check(client, "/foo/bar", COLD_STORAGE_POLICY_ID);
      check(client, "/foo/bar/file1.txt", ALLSSD_STORAGE_POLICY_ID);
      check(client, "/foo/bar/file2.txt", COLD_STORAGE_POLICY_ID);

      // Now set the root folder storage policy
      dfs.setStoragePolicy(new Path("/"), ONESSD_STORAGE_POLICY_NAME);
      check(client, "/", ONESSD_STORAGE_POLICY_ID);
      // Inherit root
      check(client, "/foo", ONESSD_STORAGE_POLICY_ID);
      // Its own policy
      check(client, "/foo/bar", COLD_STORAGE_POLICY_ID);
      // Its own policy
      check(client, "/foo/bar/file1.txt", ALLSSD_STORAGE_POLICY_ID);
      // Inherit /foo/bar
      check(client, "/foo/bar/file2.txt", COLD_STORAGE_POLICY_ID);
      // Inherit root
      check(client, "/foo/bar2", ONESSD_STORAGE_POLICY_ID);
      check(client, "/foo/bar2/file3.txt", ONESSD_STORAGE_POLICY_ID);
    } finally {
      client.close();
      cluster.shutdown();
    }
  }

  private static void check(DFSClient client, String path, byte policy)
      throws IOException {
    LOG.debug("Checking path " + path + " to have policy " +
        BlockStoragePolicySuite.getPolicy(policy));
    assertEquals(client.getFileInfo(path).getStoragePolicy(), policy);
  }

  /**
   * Test whether the BlockStoragePolicy returns the expected storage types
   */
  @Test
  public void testChooseStorageTypesSimple() {
    HashMap<String, StorageType[][]> expected = new HashMap<String, StorageType[][]>();

    expected.put(ALLSSD_STORAGE_POLICY_NAME, new StorageType[][] {
        new StorageType[]{},
        new StorageType[]{SSD},
        new StorageType[]{SSD, SSD},
        new StorageType[]{SSD, SSD, SSD}
    });

    expected.put(ONESSD_STORAGE_POLICY_NAME, new StorageType[][] {
        new StorageType[]{},
        new StorageType[]{SSD},
        new StorageType[]{SSD, DISK},
        new StorageType[]{SSD, DISK, DISK}
    });

    expected.put(HOT_STORAGE_POLICY_NAME, new StorageType[][] {
        new StorageType[]{},
        new StorageType[]{DISK},
        new StorageType[]{DISK, DISK},
        new StorageType[]{DISK, DISK, DISK}
    });

    expected.put(WARM_STORAGE_POLICY_NAME, new StorageType[][] {
        new StorageType[]{},
        new StorageType[]{DISK},
        new StorageType[]{DISK, ARCHIVE},
        new StorageType[]{DISK, ARCHIVE, ARCHIVE}
    });
  
    expected.put(COLD_STORAGE_POLICY_NAME, new StorageType[][] {
        new StorageType[]{},
        new StorageType[]{ARCHIVE},
        new StorageType[]{ARCHIVE, ARCHIVE},
        new StorageType[]{ARCHIVE, ARCHIVE, ARCHIVE}
    });
  
    expected.put(RAID5_STORAGE_POLICY_NAME, new StorageType[][] {
        new StorageType[]{},
        new StorageType[]{RAID5},
        new StorageType[]{RAID5, RAID5},
        new StorageType[]{RAID5, RAID5, RAID5}
    });

    for(Map.Entry<String, StorageType[][]> policy : expected.entrySet()) {
      BlockStoragePolicy bsp = BlockStoragePolicySuite.getPolicy(policy.getKey());
  
      for(int repl = 0; repl < policy.getValue().length; repl++) {
        List<StorageType> types = bsp.chooseStorageTypes((byte) repl);
        StorageType[] actual = types.toArray(new StorageType[0]);

        assertArrayEquals(policy.getValue()[repl], actual);
      }
    }
  }

  @Test
  public void testSimple() throws IOException {
    Configuration conf = new HdfsConfiguration();

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDatanodes)
        .storagesPerDatanode(storagesPerDN)
        .storageTypes(storageTypes)
        .build();

    try {
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ArrayList<DataNode> dns = cluster.getDataNodes();
      for(int i = 0; i < dns.size(); i++) {
        LOG.debug("datanode " + i + ": " + dns.get(i));
      }

      for(int i = 0; i < 1; i++) {
        FSNamesystem.LOG.debug("Starting file iteration " + i);
        // create a new file in home directory. Do not close it.
        Path file = new Path("/file_" + i + ".dat");
        FSDataOutputStream stm = createFile(dfs, file, 3);

        // Set the policy to all ssd
        dfs.setReplication(file, (short) 4);
        dfs.setStoragePolicy(file, policyNames[i % policyNames.length]);

        // verify that file exists in FS namespace
        assertTrue(file + " should be a file", dfs.getFileStatus(file).isFile());

        // write to file
        byte[] buffer = AppendTestUtil.randomBytes(seed, fileSize);
        stm.write(buffer, 0, fileSize);

        stm.close();

        String[] hosts = dfs.getFileBlockLocations(file, 0, fileSize)[0].getHosts();

      }
    } finally {
      cluster.shutdown();
    }
  }
}




























