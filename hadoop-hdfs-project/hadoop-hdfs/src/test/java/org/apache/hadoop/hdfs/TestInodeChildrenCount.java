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

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.*;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestInodeChildrenCount {
  private static final Log LOG = LogFactory.getLog(TestInodeChildrenCount.class);

  @Test
  public void testInodeChildrenCount() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    FsShell shell = null;
    DistributedFileSystem fs = null;
    DFSClient dfsClient = null;

    try {
      fs = cluster.getFileSystem();
      dfsClient = fs.getClient();

      //moving files in same folder
      fs.mkdirs(new Path("/test"));
      createFile(fs, "/test/test1/src");
      fs.rename(new Path("/test/test1/src"), new Path("/test/test1/dst"));
      HdfsFileStatus status = dfsClient.getFileInfo("/test/test1/dst");
      assertEquals(0, status.getChildrenNum());
      status = dfsClient.getFileInfo("/test/test1");
      assertEquals(1, status.getChildrenNum());


      //moving folder
      fs.rename(new Path("/test/test1"), new Path("/test/test2"));
      status = dfsClient.getFileInfo("/test/test2");
      assertEquals(1, status.getChildrenNum());

      //overwrite files using rename
      createFile(fs, "/test/test2/newfile");
      status = dfsClient.getFileInfo("/test/test2");
      assertEquals(2, status.getChildrenNum());

      fs.rename(new Path("/test/test2/dst"), new Path("/test/test2/newfile"), Options.Rename.OVERWRITE);
      status = dfsClient.getFileInfo("/test/test2");

      //overwrite files using create
      createFile(fs, "/test/test2/newfile"); //overwrites
      status = dfsClient.getFileInfo("/test/test2");
      assertEquals(1, status.getChildrenNum());


      //delete file
      fs.delete(new Path("/test/test2/newfile"), true);
      status = dfsClient.getFileInfo("/test/test2");
      assertEquals(0, status.getChildrenNum());

      //delete dir
      int count = 5;
      for (int i = 0; i < count; i++) {
        Path path = new Path("/test/test2/newfile" + i);
        createFile(fs, path.toString());
      }
      status = dfsClient.getFileInfo("/test/test2");
      assertEquals(5, status.getChildrenNum());
      fs.delete(new Path("/test/test2"), true);
      status = dfsClient.getFileInfo("/test");
      assertEquals(0, status.getChildrenNum());

      //concat test
      createFile(fs, "/test/concat");
      count = 5;
      Path srcs[] = new Path[5];
      for (int i = 0; i < count; i++) {
        Path path = new Path("/test/newfile" + i);
        srcs[i] = path;
        createFile(fs, path.toString());
      }

      status = dfsClient.getFileInfo("/test");
      assertEquals(6, status.getChildrenNum());
      fs.concat(new Path("/test/concat"), srcs);
      status = dfsClient.getFileInfo("/test");
      assertEquals(1, status.getChildrenNum());

      fs.delete(new Path("/test"), true);
      status = dfsClient.getFileInfo("/");
      assertEquals(0, status.getChildrenNum());

    } finally {
      if (null != shell) {
        shell.close();
      }
      cluster.shutdown();
    }
  }

  private void createFile(DistributedFileSystem dfs, String filename) throws IOException {
    FSDataOutputStream out = dfs.create(new Path(filename), true);
    out.writeBytes("testfile");
    out.close();
  }
}
