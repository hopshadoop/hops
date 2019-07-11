/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.erasure_coding;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.BlockReconstructor;

import java.io.IOException;
import java.util.List;

public class LocalBlockRepairManager extends BlockRepairManager {

  public static final Log LOG =
      LogFactory.getLog(LocalBlockRepairManager.class);

  private BlockReconstructor blockReconstructor;

  public LocalBlockRepairManager(Configuration conf) {
    super(conf);
    blockReconstructor = new BlockReconstructor(conf);
  }

  @Override
  public void repairSourceBlocks(String codecId, Path sourceFile,
      Path parityFile) {
    Codec codec = Codec.getCodec(codecId);
    Decoder decoder = new Decoder(getConf(), codec);
    try {
      blockReconstructor.processFile(sourceFile, parityFile, decoder);
    } catch (IOException e) {
      LOG.error("Exception", e);
    } catch (InterruptedException e) {
      LOG.error("Exception", e);
    }
  }

  @Override
  public void repairParityBlocks(String codecId, Path sourceFile,
      Path parityFile) {
    Codec codec = Codec.getCodec(codecId);
    Decoder decoder = new Decoder(getConf(), codec);
    try {
      blockReconstructor.processParityFile(sourceFile, parityFile, decoder);
    } catch (IOException e) {
      LOG.error("Exception", e);
    } catch (InterruptedException e) {
      LOG.error("Exception", e);
    }
  }

  @Override
  public List<Report> computeReports() {
    throw new NotImplementedException("not implemented");
  }

  @Override
  public void cancelAll() {
    throw new NotImplementedException("not implemented");
  }

  @Override
  public void cancel(String toCancel) {
    throw new NotImplementedException("not implemented");
  }
}