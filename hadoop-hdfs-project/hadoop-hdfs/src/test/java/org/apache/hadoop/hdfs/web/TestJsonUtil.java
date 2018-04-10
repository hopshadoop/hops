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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.FsAction.ALL;
import static org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
import static org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.aclEntry;

public class TestJsonUtil {
  static FileStatus toFileStatus(HdfsFileStatus f, String parent) throws IOException {
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(), f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        f.isSymlink() ? new Path(f.getSymlink()) : null,
        new Path(f.getFullName(parent)));
  }

  @Test
  public void testHdfsFileStatus() throws IOException {
    final long now = Time.now();
    final String parent = "/dir";
    final HdfsFileStatus status =
        new HdfsFileStatus(1001L, false, 3, 1L << 26, now, now + 10,
            new FsPermission((short) 0644), "user", "group",
            DFSUtil.string2Bytes("bar"), DFSUtil.string2Bytes("foo"), -1,  0, false/*not stored in the database*/, (byte) 0);
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
  
  @Test
  public void testToAclStatus() {
    String jsonString =
        "{\"AclStatus\":{\"entries\":[\"user::rwx\",\"user:user1:rw-\",\"group::rw-\",\"other::r-x\"],\"group\":\"supergroup\",\"owner\":\"testuser\",\"stickyBit\":false}}";
    Map<?, ?> json = (Map<?, ?>) JSON.parse(jsonString);

    List<AclEntry> aclSpec =
        Lists.newArrayList(aclEntry(ACCESS, USER, ALL),
            aclEntry(ACCESS, USER, "user1", READ_WRITE),
            aclEntry(ACCESS, GROUP, READ_WRITE),
            aclEntry(ACCESS, OTHER, READ_EXECUTE));

    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
    aclStatusBuilder.owner("testuser");
    aclStatusBuilder.group("supergroup");
    aclStatusBuilder.addEntries(aclSpec);
    aclStatusBuilder.stickyBit(false);

    Assert.assertEquals("Should be equal", aclStatusBuilder.build(),
        JsonUtil.toAclStatus(json));
  }

  @Test
  public void testToJsonFromAclStatus() {
    String jsonString =
        "{\"AclStatus\":{\"entries\":[\"user:user1:rwx\",\"group::rw-\"],\"group\":\"supergroup\",\"owner\":\"testuser\",\"stickyBit\":false}}";
    AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
    aclStatusBuilder.owner("testuser");
    aclStatusBuilder.group("supergroup");
    aclStatusBuilder.stickyBit(false);

    List<AclEntry> aclSpec =
        Lists.newArrayList(aclEntry(ACCESS, USER,"user1", ALL),
            aclEntry(ACCESS, GROUP, READ_WRITE));

    aclStatusBuilder.addEntries(aclSpec);
    Assert.assertEquals(jsonString,
        JsonUtil.toJsonString(aclStatusBuilder.build()));

  }
}
