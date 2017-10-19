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
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import java.io.IOException;
import java.util.Map;

public class TestJsonUtil {
  static FileStatus toFileStatus(HdfsFileStatus f, String parent) throws IOException {
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(), f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        f.isSymlink() ? f.getSymlink() : null,
        new Path(f.getFullName(parent)));
  }

  @Test
  public void testHdfsFileStatus() throws IOException {
    final long now = Time.now();
    final String parent = "/dir";
    final HdfsFileStatus status =
        new HdfsFileStatus(INode.NON_EXISTING_ID, 1001L, false, 3, 1L << 26, now, now + 10,
            new FsPermission((short) 0644), "user", "group",
            DFSUtil.string2Bytes("bar"), DFSUtil.string2Bytes("foo"),false/*not stored in the database*/);
    final FileStatus fstatus = toFileStatus(status, parent);
    System.out.println("status  = " + status);
    System.out.println("fstatus = " + fstatus);
    final String json = JsonUtil.toJsonString(status, true);
    System.out.println("json    = " + json.replace(",", ",\n  "));
    final HdfsFileStatus s2 =
        JsonUtil.toFileStatus((Map<?, ?>) JSON.parse(json), true);
    final FileStatus fs2 = toFileStatus(s2, parent);
    System.out.println("s2      = " + s2);
    System.out.println("fs2     = " + fs2);
    Assert.assertEquals(fstatus, fs2);
  }
}
