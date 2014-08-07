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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestFsDatasetImpl {
  private static final String BASE_DIR =
      System.getProperty("test.build.dir") + "/fsdatasetimpl";
  private static final int NUM_INIT_VOLUMES = 2;
  private static final String CLUSTER_ID = "cluser-id";
  private static final String[] BLOCK_POOL_IDS = {"bpid-0", "bpid-1"};

  private DataStorage storage;
  private FsDatasetImpl dataset;

  private static void createStorageDirs(DataStorage storage, Configuration conf,
      int numDirs) throws IOException {
    List<Storage.StorageDirectory> dirs =
        new ArrayList<Storage.StorageDirectory>();
    List<String> dirStrings = new ArrayList<String>();
    for (int i = 0; i < numDirs; i++) {
      String loc = BASE_DIR + "/data" + i;
      dirStrings.add(loc);
      dirs.add(new Storage.StorageDirectory(new File(loc)));
      when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
    }

    String dataDir = StringUtils.join(",", dirStrings);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir);
    when(storage.getNumStorageDirs()).thenReturn(numDirs);
  }

  @Before
  public void setUp() throws IOException {
    final DataNode datanode = Mockito.mock(DataNode.class);
    storage = Mockito.mock(DataStorage.class);
    Configuration conf = new Configuration();
    final DNConf dnConf = new DNConf(conf);

    when(datanode.getConf()).thenReturn(conf);
    when(datanode.getDnConf()).thenReturn(dnConf);

    createStorageDirs(storage, conf, NUM_INIT_VOLUMES);
    dataset = new FsDatasetImpl(datanode, storage, conf);

    assertEquals(NUM_INIT_VOLUMES, dataset.getVolumes().size());
    assertEquals(0, dataset.getNumFailedVolumes());
  }

  //TODO: HOPS the tests will eventually be added when they catch back on the code that as already be merged in the tested class
}
