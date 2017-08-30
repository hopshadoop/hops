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

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.test.MockitoMaker.make;
import static org.apache.hadoop.test.MockitoMaker.stub;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestDataDirs {

  @Test
  public void testGetDataDirsFromURIs() throws Throwable {
    File localDir = make(stub(File.class).returning(true).from.exists());
    when(localDir.mkdir()).thenReturn(true);
    when(localDir.isDirectory()).thenReturn(true);
    when(localDir.canRead()).thenReturn(true);
    when(localDir.canWrite()).thenReturn(true);
    when(localDir.canExecute()).thenReturn(true);
    when(localDir.exists()).thenReturn(false);
    FsPermission normalPerm = new FsPermission("700");
    FsPermission badPerm = new FsPermission("000");
    FileStatus stat = make(
        stub(FileStatus.class).returning(normalPerm, normalPerm, badPerm).from
            .getPermission());
    when(stat.isDirectory()).thenReturn(true);
    LocalFileSystem fs = make(stub(LocalFileSystem.class).returning(stat).from
        .getFileStatus(any(Path.class)));
    when(fs.pathToFile(any(Path.class))).thenReturn(localDir);
    Collection<URI> uris = Arrays
        .asList(new URI("file:/p1/"), new URI("file:/p2/"),
            new URI("file:/p3/"));

    List<File> dirs = DataNode.getDataDirsFromURIs(uris, fs, normalPerm);

    /*
    File#exists is mocked to return false. That means that when DataNode
    #getDataDirsFromURIs is called the DiskChecker#checkDirs will call
    mkdirsWithExistsAndPermissionCheck() which will create the file will call
     FileSystem#setPermission uris.size() times.
     
     Also, since the files will be created by
     mkdirsWithExistsAndPermissionCheck() with the specified permissions,
     the FileSystem#getFileStatus will never be invoked
     */
    verify(fs, times(3)).setPermission(any(Path.class), eq(normalPerm));
    verify(fs, never()).getFileStatus(any(Path.class));
    assertEquals("number of valid data dirs", dirs.size(), 3);
  }
}
