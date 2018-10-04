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
public class TestMultithreadedCopyFromLocal {
  private static final Log LOG = LogFactory.getLog(TestMultithreadedCopyFromLocal.class);
  private static AtomicInteger counter = new AtomicInteger();
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
    conf.setInt(CommonConfigurationKeys.DFS_CLIENT_COPY_FROM_LOCAL_PARALLEL_THREADS, 10);
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FsShell shell = null;
    FileSystem fs = null;
    final File localDir = new File(TEST_ROOT_DIR, "localDir");

    final String  hdfsTestDirStr = TEST_ROOT_DIR;
    final Path hdfsTestDir = new Path(hdfsTestDirStr);
    try {
      fs = cluster.getFileSystem();

      createLocalDir(localDir);

      fs.mkdirs(hdfsTestDir);
      shell = new FsShell(conf);

      String[] argv = new String[]{"-copyFromLocal", localDir.getAbsolutePath(), hdfsTestDirStr+"/copiedDir"};
      int res = ToolRunner.run(shell, argv);
      assertEquals("copyFromLocal command should have succeeded", SUCCESS, res);

    } finally {
      if (null != shell) {
        shell.close();
      }

      if (localDir.exists()) {
        localDir.delete();
      }

      cluster.shutdown();
    }
  }

  private void createLocalDir(File base) throws IOException {
    int filesPerLevel=10;
    base.mkdir();
    for(int i = 0; i < filesPerLevel; i++){
      File localFile = new File(base, "localFile"+i);
      localFile.createNewFile();
    }

    final File localDir2 = new File(base, "localDirInternal");
    localDir2.mkdir();
    for(int i = 0; i < filesPerLevel; i++){
      File localFile = new File(localDir2, "localFile"+i);
      localFile.createNewFile();
    }
  }
}
