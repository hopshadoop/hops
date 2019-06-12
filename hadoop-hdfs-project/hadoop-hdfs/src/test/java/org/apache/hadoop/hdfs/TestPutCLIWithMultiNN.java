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
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests commands from DFSShell.
 */
public class TestPutCLIWithMultiNN {
  private static final Log LOG = LogFactory.getLog(TestPutCLIWithMultiNN.class);
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
  public void testPutMultiNN() throws Exception {
    final int NN_COUNT=3;
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .nnTopology(MiniDFSNNTopology.simpleHOPSTopology(NN_COUNT))
            .numDataNodes(1).format(true).build();
    FsShell shell = null;
    FileSystem fs = null;
    final File localDir = new File(TEST_ROOT_DIR, "localDir");
    final File copiedDir = new File(TEST_ROOT_DIR, "copiedDir");
    if(copiedDir.exists()){
      FileUtils.deleteDirectory(copiedDir);
    }

    if(localDir.exists()){
      FileUtils.deleteDirectory(localDir);
    }

    final String  hdfsTestDirStr = TEST_ROOT_DIR;
    final Path hdfsTestDir = new Path(hdfsTestDirStr);
    Random rand = new Random(System.currentTimeMillis());
    try {
      fs = cluster.getFileSystem(rand.nextInt(NN_COUNT));

      createLocalDir(localDir);

      fs.mkdirs(hdfsTestDir);
      shell = new FsShell(conf);

      long startTime = System.currentTimeMillis();
      String[] argv = new String[]{"-put", "-t", "10", localDir.getAbsolutePath(), hdfsTestDirStr+
              "/copiedDir"};
      int res = ToolRunner.run(shell, argv);
      assertEquals("-put command should have succeeded", SUCCESS, res);
      LOG.info("Time taken by put "+(System.currentTimeMillis() - startTime)/1000+" sec");


      startTime = System.currentTimeMillis();
      argv = new String[]{"-copyToLocal", "-t", "10", hdfsTestDirStr+"/copiedDir", TEST_ROOT_DIR};
      res = ToolRunner.run(shell, argv);
      assertEquals("copyToLocal command should have succeeded", SUCCESS, res);
      LOG.info("Time taken by copyToLocal "+(System.currentTimeMillis() - startTime)/1000+" " +"sec");

    } finally {
      if (null != shell) {
        shell.close();
      }
      cluster.shutdown();
    }
  }

  private void createLocalDir(File base) throws IOException {
    int filesPerLevel=100;
    base.mkdir();
    for(int i = 0; i < filesPerLevel; i++){
      File localFile = new File(base, "localFile"+i);
//      localFile.createNewFile();
      BufferedWriter writer = new BufferedWriter(new FileWriter(localFile));
      writer.write(i+"");
      writer.close();
    }

    final File localDir2 = new File(base, "localDirInternal");
    localDir2.mkdir();
    for(int i = 0; i < filesPerLevel; i++){
      File localFile = new File(localDir2, "localFile"+i);
//      localFile.createNewFile();
      BufferedWriter writer = new BufferedWriter(new FileWriter(localFile));
      writer.write(i+"");
      writer.close();
    }
  }
}
