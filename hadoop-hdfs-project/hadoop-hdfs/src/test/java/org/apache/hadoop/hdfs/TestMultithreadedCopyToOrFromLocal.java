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

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
/**
 * This class tests commands from DFSShell.
 */
public class TestMultithreadedCopyToOrFromLocal {
  private static final Log LOG = LogFactory.getLog(TestMultithreadedCopyToOrFromLocal.class);
  private final int SUCCESS = 0;
  private final int ERROR = 1;

  static final String TEST_ROOT_DIR =
      new Path(System.getProperty("test.build.data", "/tmp")).toString()
          .replace(' ', '+');

  static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    out.writeBytes("dhruba: " + f);
    out.close();
    assertTrue(fs.exists(f));
    return f;
  }

  @Test
  public void testCopyFromLocal() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ENABLE_RETRY_CACHE_KEY,true);
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FsShell shell = null;
    DistributedFileSystem fs = null;
    final File localDir = new File(TEST_ROOT_DIR, "localDir");
    final File copiedDir = new File(TEST_ROOT_DIR, "copiedDir");
    final File copiedBackDir = new File(TEST_ROOT_DIR, "copiedBackDir");
    String numThreads = "10";

    if(copiedDir.exists()){
      FileUtils.deleteDirectory(copiedDir);
    }

    if(copiedBackDir.exists()){
      FileUtils.deleteDirectory(copiedDir);
    }

    if(localDir.exists()){
      FileUtils.deleteDirectory(localDir);
    }

    final String  hdfsTestDirStr = TEST_ROOT_DIR;
    final Path hdfsTestDir = new Path(hdfsTestDirStr);
    try {
      fs = cluster.getFileSystem();

      createLocalDir(localDir);

      fs.mkdirs(hdfsTestDir);
      fs.setStoragePolicy(hdfsTestDir, "DB");
      shell = new FsShell(conf);

      long startTime = System.currentTimeMillis();
      String[] argv = new String[]{"-copyFromLocal", "-t", numThreads,
              "-d", localDir.getAbsolutePath(), hdfsTestDirStr+"/copiedDir"};
      int res = ToolRunner.run(shell, argv);
      assertEquals("copyFromLocal command should have succeeded", SUCCESS, res);
      LOG.info("Time taken copyFromLocal "+(System.currentTimeMillis() - startTime)/1000+" sec");


      startTime = System.currentTimeMillis();
      argv = new String[]{"-copyToLocal","-t", numThreads, hdfsTestDirStr+"/copiedDir",
              copiedBackDir.getAbsolutePath()};
      res = ToolRunner.run(shell, argv);
      assertEquals("copyToLocal command should have succeeded", SUCCESS, res);
      LOG.info("Time taken copyToLocal "+(System.currentTimeMillis() - startTime)/1000+" sec");

      //Verify
      verify(localDir, copiedBackDir);

    } finally {
      if (null != shell) {
        shell.close();
      }
      cluster.shutdown();
    }
  }

  private void createLocalDir(File base) throws IOException {
    createLocalDir(base,0);
  }

  private void verify(File base1, File base2) throws IOException {
    verify(base1, base2,0);
  }

  int filesPerDir = 10;
  int dirPerDir = 2;
  int maxDepth = 5;

  private void createLocalDir(File base, int depth) throws IOException {
    if (depth >= maxDepth) {
      return;
    }

    base.mkdir();
    for (int dir = 0; dir < dirPerDir; dir++) {
      final File dirPath = new File(base, "dir_depth_" + depth + "_count_" + dir);
      dirPath.mkdir();
      LOG.info(" DIR: "+dirPath);
      createLocalDir(dirPath, depth+1);
      for (int file = 0; file < filesPerDir; file++) {
        final File filePath = new File(dirPath, "file_" + file);
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
        writer.write(filePath.toString());
        writer.close();
        LOG.info("FILE: "+filePath);
      }
    }
  }

  private void verify(File base1, File base2, int depth) throws IOException {
    if (depth >= maxDepth) {
      return;
    }

    for (int dir = 0; dir < dirPerDir; dir++) {
      final File dirPath1 = new File(base1, "dir_depth_" + depth + "_count_" + dir);
      final File dirPath2 = new File(base2, "dir_depth_" + depth + "_count_" + dir);
      assertTrue(dirPath1.isDirectory());
      assertTrue(dirPath2.isDirectory());
      verify(dirPath1, dirPath2, depth+1);
      for (int file = 0; file < filesPerDir; file++) {
        final File filePath1 = new File(dirPath1, "file_" + file);
        final File filePath2 = new File(dirPath2, "file_" + file);
        assertTrue(filePath1.isFile());
        assertTrue(filePath2.isFile());

        BufferedReader reader1 = new BufferedReader(new FileReader(filePath1));
        String f1Data = reader1.readLine();
        reader1.close();

        BufferedReader reader2 = new BufferedReader(new FileReader(filePath1));
        String f2Data = reader2.readLine();
        reader2.close();

        assertTrue(f1Data.equals(f2Data));
      }
    }
  }

  @Test
  public void testCopyFromLocal2() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FsShell shell = null;
    DistributedFileSystem fs = null;
    final File localFile = new File(TEST_ROOT_DIR, "localFile");
    final File copiedFile = new File(TEST_ROOT_DIR, "copiedFile");
    final File copiedBackFile = new File(TEST_ROOT_DIR, "copiedBackFile");
    String numThreads = "10";

    if(copiedFile.exists()){
      FileUtils.forceDelete(copiedFile);
    }

    if(localFile.exists()){
      FileUtils.forceDelete(localFile);
    }

    if(copiedBackFile.exists()){
      FileUtils.forceDelete(copiedBackFile);
    }

    final String  hdfsTestDirStr = TEST_ROOT_DIR;
    final Path hdfsTestDir = new Path(hdfsTestDirStr);
    try {
      fs = cluster.getFileSystem();

      createLocalFile(localFile);

      fs.mkdirs(hdfsTestDir);
      fs.setStoragePolicy(hdfsTestDir, "DB");

      shell = new FsShell(conf);

      long startTime = System.currentTimeMillis();
      String[] argv = new String[]{"-copyFromLocal", "-t", numThreads,
              "-d", localFile.getAbsolutePath(), copiedFile.getAbsolutePath()};
      int res = ToolRunner.run(shell, argv);
      assertEquals("copyFromLocal command should have succeeded", SUCCESS, res);
      LOG.info("Time taken copyFromLocal "+(System.currentTimeMillis() - startTime)/1000+" sec");


      startTime = System.currentTimeMillis();
      argv = new String[]{"-copyToLocal","-t", numThreads, copiedFile.getAbsolutePath(),
              copiedBackFile.getAbsolutePath()};
      res = ToolRunner.run(shell, argv);
      assertEquals("copyToLocal command should have succeeded", SUCCESS, res);
      LOG.info("Time taken copyToLocal "+(System.currentTimeMillis() - startTime)/1000+" sec");

      //Verify
      verifyFiles(localFile, copiedBackFile);

    } finally {
      if (null != shell) {
        shell.close();
      }
      cluster.shutdown();
    }
  }


  void createLocalFile(File filePath) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
    writer.write(filePath.toString());
    writer.close();
  }

  void verifyFiles(File filePath1, File filePath2) throws IOException {
    BufferedReader reader1 = new BufferedReader(new FileReader(filePath1));
    String f1Data = reader1.readLine();
    reader1.close();

    BufferedReader reader2 = new BufferedReader(new FileReader(filePath1));
    String f2Data = reader2.readLine();
    reader2.close();

    assertTrue(f1Data.equals(f2Data));

  }
}
