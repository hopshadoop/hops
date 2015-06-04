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

import io.hops.erasure_coding.Decoder;
import io.hops.erasure_coding.Helper;
import io.hops.erasure_coding.RaidUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ErasureCodingFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * this class implements the actual reconstructing functionality
 * we keep this in a separate class so that
 * the distributed block fixer can use it
 */
public class BlockReconstructor extends Configured {

  public static final Log LOG = LogFactory.getLog(BlockReconstructor.class);

  public static final String SEND_BLOCK_RETRY_COUNT_KEY =
      "io.hops.erasure_coding.send_block_retry_count";
  public static final int DEFAULT_SEND_BLOCK_RETRY_COUNT = 3;

  private final int retryClount;

  public BlockReconstructor(Configuration conf) {
    super(conf);
    retryClount =
        conf.getInt(SEND_BLOCK_RETRY_COUNT_KEY, DEFAULT_SEND_BLOCK_RETRY_COUNT);
  }

  public boolean processFile(Path sourceFile, Path parityFile, Decoder decoder)
      throws IOException, InterruptedException {
    DFSClient dfsClient = Helper.getDFS(getConf(), sourceFile).getClient();
    LocatedBlocks missingBlocks =
        dfsClient.getMissingLocatedBlocks(sourceFile.toUri().getPath());
    return processFile(sourceFile, parityFile, missingBlocks, decoder, null);
  }

  public boolean processFile(Path sourceFile, Path parityFile,
      LocatedBlocks missingBlocks, Decoder decoder, Context context)
      throws IOException, InterruptedException {
    LOG.info("Processing file " + sourceFile.toString());
    Progressable progress = context;
    if (progress == null) {
      progress = RaidUtils.NULL_PROGRESSABLE;
    }

    DistributedFileSystem srcFs = Helper.getDFS(getConf(), sourceFile);
    FileStatus sourceStatus = srcFs.getFileStatus(sourceFile);
    DistributedFileSystem parityFs = Helper.getDFS(getConf(), parityFile);
    long blockSize = sourceStatus.getBlockSize();
    long srcFileSize = sourceStatus.getLen();
    String uriPath = sourceFile.toUri().getPath();

    int numBlocksReconstructed = 0;
    for (LocatedBlock lb : missingBlocks.getLocatedBlocks()) {
      Block lostBlock = lb.getBlock().getLocalBlock();
      long lostBlockOffset = lb.getStartOffset();

      LOG.info("Found lost block " + lostBlock +
          ", offset " + lostBlockOffset);

      final long blockContentsSize =
          Math.min(blockSize, srcFileSize - lostBlockOffset);
      File localBlockFile =
          File.createTempFile(lostBlock.getBlockName(), ".tmp");
      localBlockFile.deleteOnExit();
      try {
        decoder.recoverBlockToFile(srcFs, sourceFile, parityFs, parityFile,
            blockSize, lostBlockOffset, localBlockFile, blockContentsSize,
            context);

        // Now that we have recovered the file block locally, send it.
        sendRepairedBlock(srcFs, sourceFile, parityFile, false, lb,
            localBlockFile);

        numBlocksReconstructed++;

      } finally {
        localBlockFile.delete();
      }
      progress.progress();
    }

    LOG.info(
        "Reconstructed " + numBlocksReconstructed + " blocks in " + sourceFile);
    return true;
  }

  public boolean processParityFile(Path sourceFile, Path parityFile,
      Decoder decoder) throws IOException, InterruptedException {
    return processParityFile(sourceFile, parityFile, decoder, null);
  }

  public boolean processParityFile(Path sourceFile, Path parityFile,
      Decoder decoder, Context context)
      throws IOException, InterruptedException {
    DFSClient dfsClient = Helper.getDFS(getConf(), sourceFile).getClient();
    LocatedBlocks missingBlocks =
        dfsClient.getMissingLocatedBlocks(parityFile.toUri().getPath());
    return processParityFile(sourceFile, parityFile, missingBlocks, decoder,
        context);
  }

  public boolean processParityFile(Path sourceFile, Path parityFile,
      LocatedBlocks missingBlocks, Decoder decoder, Context context)
      throws IOException, InterruptedException {
    LOG.info("Processing parity file for " + sourceFile.toString());

    Progressable progress = context;
    if (progress == null) {
      progress = RaidUtils.NULL_PROGRESSABLE;
    }

    DistributedFileSystem srcFs = Helper.getDFS(getConf(), sourceFile);
    Path srcPath = sourceFile;
    DistributedFileSystem parityFs = Helper.getDFS(getConf(), parityFile);
    Path parityPath = parityFile;
    FileStatus parityStat = parityFs.getFileStatus(parityPath);
    long blockSize = parityStat.getBlockSize();
    FileStatus srcStat = srcFs.getFileStatus(srcPath);

    int numBlocksReconstructed = 0;
    for (LocatedBlock lb : missingBlocks.getLocatedBlocks()) {
      Block lostBlock = lb.getBlock().getLocalBlock();
      long lostBlockOffset = lb.getStartOffset();

      LOG.info("Found lost block " + lostBlock +
          ", offset " + lostBlockOffset);

      File localBlockFile =
          File.createTempFile(lostBlock.getBlockName(), ".tmp");
      localBlockFile.deleteOnExit();

      try {
        decoder.recoverParityBlockToFile(srcFs, srcPath, parityFs, parityPath,
            blockSize, lostBlockOffset, localBlockFile, context);

        // Now that we have recovered the parity file block locally, send it.
        sendRepairedBlock(parityFs, sourceFile, parityFile, true, lb,
            localBlockFile);

        numBlocksReconstructed++;
      } finally {
        localBlockFile.delete();
      }
      progress.progress();
    }

    LOG.info(
        "Reconstructed " + numBlocksReconstructed + " blocks in " + parityPath);
    return true;
  }

  private void sendRepairedBlock(DistributedFileSystem dfs, Path sourceFile,
      Path parityFile, boolean isParityBlock, LocatedBlock lb, File block)
      throws IOException {
    LocatedBlock blockReceivers = dfs.getClient()
        .getRepairedBlockLocations(sourceFile.toUri().getPath(),
            parityFile.toUri().getPath(), lb, isParityBlock);

    Path destination;
    if (isParityBlock) {
      destination = parityFile;
    } else {
      destination = sourceFile;
    }

    int retries = retryClount + 1;
    do {
      try {
        HdfsDataOutputStream out =
            dfs.sendBlock(destination, blockReceivers, null, null);
        // TODO configure buffer
        FileInputStream in = new FileInputStream(block);
        byte[] buff = new byte[8192];
        int read;
        while ((read = in.read(buff)) > 0) {
          out.write(buff, 0, read);
        }
        LOG.info("Send repaired block " + lb.toString());
        try {
          out.close();
        } catch (IOException e) {
        }
        break;
      } catch (IOException e) {
        retries--;
        LOG.info("Sending repaired block failed. Attempts left: " + retries, e);
        if (retries == 0) {
          throw new IOException("Sending repaired block failed");
        }
      }
    } while (retryClount > 0);
  }
}
