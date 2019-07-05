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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * The underlying volume used to store replica.
 * <p/>
 * It uses the {@link FsDatasetImpl} object for synchronization.
 */
@InterfaceAudience.Private
@VisibleForTesting
public class CloudFsVolumeImpl extends FsVolumeImpl {
  public static final Logger LOG =
          LoggerFactory.getLogger(CloudFsVolumeImpl.class);
  CloudFsVolumeImpl(FsDatasetImpl dataset, String storageID, File currentDir,
                    Configuration conf, StorageType storageType) throws IOException {
    super(dataset, storageID, currentDir, conf, storageType);
    LOG.info("HopsFS-Cloud. Initializing CloudFsVolumeImpl.  ");
  }

  @Override
  File createRbwFile(String bpid, Block b) throws IOException {
    LOG.info("HopsFS-Cloud. Creating Rbw File. BlockID: "+b.getBlockId()+
            " GenStamp: "+b.getGenerationStamp());
    return super.createRbwFile(bpid, b);
  }

  @Override
  public BlockIterator newBlockIterator(String bpid, String name) {
    return new BlockIteratorImpl(bpid, name);
  }


  /**
   * A BlockIterator implementation for FsVolumeImpl.
   */
  private class BlockIteratorImpl implements FsVolumeSpi.BlockIterator {

    private final String name;
    private final String bpid;

    BlockIteratorImpl(String bpid, String name) {
      this.name = name;
      this.bpid = bpid;
    }

    @Override
    public ExtendedBlock nextBlock() throws IOException {
      return null;
    }

    @Override
    public boolean atEnd() {
      return true;
    }

    @Override
    public void rewind() {
    }

    @Override
    public void save() throws IOException {
    }

    @Override
    public void setMaxStalenessMs(long maxStalenessMs) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long getIterStartMs() {
      return System.currentTimeMillis();
    }

    @Override
    public long getLastSavedMs() {
      return System.currentTimeMillis();
    }

    @Override
    public String getBlockPoolId() {
      return bpid;
    }
  }
}
