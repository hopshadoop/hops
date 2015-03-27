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

package io.hops.erasure_coding;

import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class BaseEncodingManager extends EncodingManager {

  public static enum LOGTYPES {
    ONLINE_RECONSTRUCTION,
    OFFLINE_RECONSTRUCTION,
    ENCODING
  }

  ;
  public static final Log LOG = LogFactory.getLog(BaseEncodingManager.class);
  public static final Log ENCODER_METRICS_LOG =
      LogFactory.getLog("RaidMetrics");
  public static final String JOBUSER = "erasure_coding";

  public static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

  /**
   * hadoop configuration
   */
  protected Configuration conf;

  protected boolean initialized = false;  // Are we initialized?

  // statistics about RAW hdfs blocks. This counts all replicas of a block.
  public static class Statistics {
    long numProcessedBlocks; // total blocks encountered in namespace
    long processedSize;   // disk space occupied by all blocks
    long remainingSize;      // total disk space post RAID

    long numMetaBlocks;      // total blocks in metafile
    long metaSize;           // total disk space for meta files

    public void clear() {
      numProcessedBlocks = 0;
      processedSize = 0;
      remainingSize = 0;
      numMetaBlocks = 0;
      metaSize = 0;
    }

    public String toString() {
      long save = processedSize - (remainingSize + metaSize);
      long savep = 0;
      if (processedSize > 0) {
        savep = (save * 100) / processedSize;
      }
      String msg = " numProcessedBlocks = " + numProcessedBlocks +
          " processedSize = " + processedSize +
          " postRaidSize = " + remainingSize +
          " numMetaBlocks = " + numMetaBlocks +
          " metaSize = " + metaSize +
          " %save in raw disk space = " + savep;
      return msg;
    }
  }

  BaseEncodingManager(Configuration conf) throws IOException {
    super(conf);
    try {
      initialize(conf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void cleanUpDirectory(String dir, Configuration conf)
      throws IOException {
    Path pdir = new Path(dir);
    FileSystem fs = pdir.getFileSystem(conf);
    if (fs.exists(pdir)) {
      fs.delete(pdir);
    }
  }

  protected void cleanUpTempDirectory(Configuration conf) throws IOException {
    for (Codec codec : Codec.getCodecs()) {
      cleanUpDirectory(codec.tmpParityDirectory, conf);
    }
  }

  private void initialize(Configuration conf)
      throws IOException, SAXException, InterruptedException,
      ClassNotFoundException, ParserConfigurationException {
    this.conf = conf;
    // clean up temporay directory
    cleanUpTempDirectory(conf);
    initialized = true;
  }

  static long numBlocks(FileStatus stat) {
    return (long) Math.ceil(stat.getLen() * 1.0 / stat.getBlockSize());
  }

  static long numStripes(long numBlocks, int stripeSize) {
    return (long) Math.ceil(numBlocks * 1.0 / stripeSize);
  }

  /**
   * RAID a list of files / directories
   */
  void doRaid(Configuration conf, PolicyInfo info) throws IOException {
    int targetRepl = Integer.parseInt(info.getProperty("targetReplication"));
    int metaRepl = Integer.parseInt(info.getProperty("metaReplication"));
    Codec codec = Codec.getCodec(info.getCodecId());
    Path destPref = new Path(codec.parityDirectory);

    Statistics statistics = new Statistics();
    doRaid(conf, info.getSrcPath(), destPref, codec, statistics,
        RaidUtils.NULL_PROGRESSABLE, targetRepl, metaRepl);
    LOG.info("RAID statistics " + statistics.toString());
  }


  /**
   * RAID an individual file/directory
   */
  static public boolean doRaid(Configuration conf, PolicyInfo info, Path src,
      Statistics statistics, Progressable reporter) throws IOException {
    int targetRepl = Integer.parseInt(info.getProperty("targetReplication"));
    int metaRepl = Integer.parseInt(info.getProperty("metaReplication"));

    Codec codec = Codec.getCodec(info.getCodecId());
    Path parityPath = new Path(info.getProperty("parityPath"));

    return doRaid(conf, src, parityPath, codec, statistics, reporter,
        targetRepl, metaRepl);
  }
  
  public static boolean doRaid(Configuration conf, Path path, Path parityPath,
      Codec codec, Statistics statistics, Progressable reporter, int targetRepl,
      int metaRepl) throws IOException {
    long startTime = System.currentTimeMillis();
    boolean success = false;
    try {
      return doFileRaid(conf, path, parityPath, codec, statistics, reporter,
          targetRepl, metaRepl);
    } catch (IOException ioe) {
      throw ioe;
    } finally {
      long delay = System.currentTimeMillis() - startTime;
      long savingBytes = statistics.processedSize -
          statistics.remainingSize - statistics.metaSize;
      FileSystem srcFs = path.getFileSystem(conf);
      if (success) {
        logRaidEncodingMetrics("SUCCESS", codec, delay,
            statistics.processedSize, statistics.numProcessedBlocks,
            statistics.numMetaBlocks, statistics.metaSize, savingBytes, path,
            LOGTYPES.ENCODING, srcFs);
      } else {
        logRaidEncodingMetrics("FAILURE", codec, delay,
            statistics.processedSize, statistics.numProcessedBlocks,
            statistics.numMetaBlocks, statistics.metaSize, savingBytes, path,
            LOGTYPES.ENCODING, srcFs);
      }
    }
  }
  
  public static long getNumBlocks(FileStatus stat) {
    long numBlocks = stat.getLen() / stat.getBlockSize();
    if (stat.getLen() % stat.getBlockSize() == 0) {
      return numBlocks;
    } else {
      return numBlocks + 1;
    }
  }

  /**
   * RAID an individual file
   */
  public static boolean doFileRaid(Configuration conf, Path sourceFile,
      Path parityPath, Codec codec, Statistics statistics,
      Progressable reporter, int targetRepl, int metaRepl) throws IOException {
    FileSystem srcFs = sourceFile.getFileSystem(conf);
    FileStatus sourceStatus = srcFs.getFileStatus(sourceFile);

    // extract block locations from File system
    BlockLocation[] locations =
        srcFs.getFileBlockLocations(sourceFile, 0, sourceStatus.getLen());
    // if the file has fewer than 2 blocks, then nothing to do
    if (locations.length <= 2) {
      return false;
    }

    // add up the raw disk space occupied by this file
    long diskSpace = 0;
    for (BlockLocation l : locations) {
      diskSpace += (l.getLength() * sourceStatus.getReplication());
    }
    statistics.numProcessedBlocks += locations.length;
    statistics.processedSize += diskSpace;

    // generate parity file
    generateParityFile(conf, sourceStatus, targetRepl, reporter, srcFs,
        parityPath, codec, locations.length, sourceStatus.getReplication(),
        metaRepl, sourceStatus.getBlockSize());
    if (srcFs.setReplication(sourceFile, (short) targetRepl) == false) {
      LOG.info("Error in reducing replication of " + sourceFile + " to " +
          targetRepl);
      statistics.remainingSize += diskSpace;
      return false;
    }
    ;

    diskSpace = 0;
    for (BlockLocation l : locations) {
      diskSpace += (l.getLength() * targetRepl);
    }
    statistics.remainingSize += diskSpace;

    // the metafile will have this many number of blocks
    int numMeta = locations.length / codec.stripeLength;
    if (locations.length % codec.stripeLength != 0) {
      numMeta++;
    }

    // we create numMeta for every file. This metablock has metaRepl # replicas.
    // the last block of the metafile might not be completely filled up, but we
    // ignore that for now.
    statistics.numMetaBlocks += (numMeta * metaRepl);
    statistics.metaSize += (numMeta * metaRepl * sourceStatus.getBlockSize());
    return true;
  }

  /**
   * Generate parity file
   */
  static private void generateParityFile(Configuration conf,
      FileStatus sourceFile, int targetRepl, Progressable reporter,
      FileSystem inFs, Path destPath, Codec codec, long blockNum, int srcRepl,
      int metaRepl, long blockSize) throws IOException {
    Path inpath = sourceFile.getPath();
    FileSystem outFs = inFs;

    // If the parity file is already upto-date and source replication is set
    // then nothing to do.
    try {
      FileStatus stmp = outFs.getFileStatus(destPath);
      if (stmp.getModificationTime() == sourceFile.getModificationTime() &&
          srcRepl == targetRepl) {
        LOG.info("Parity file for " + inpath + "(" + blockNum +
            ") is " + destPath + " already upto-date and " +
            "file is at target replication . Nothing more to do.");
        return;
      }
    } catch (IOException e) {
      // ignore errors because the raid file might not exist yet.
    }

    Encoder encoder = new Encoder(conf, codec);
    FileStatus srcStat = inFs.getFileStatus(inpath);
    long srcSize = srcStat.getLen();
    long numBlocks = (srcSize % blockSize == 0) ? (srcSize / blockSize) :
        ((srcSize / blockSize) + 1);
    long numStripes = (numBlocks % codec.stripeLength == 0) ?
        (numBlocks / codec.stripeLength) :
        ((numBlocks / codec.stripeLength) + 1);
    StripeReader sReader =
        new FileStripeReader(conf, blockSize, codec, inFs, 0, inpath, srcSize);
    encoder.encodeFile(conf, inFs, inpath, outFs, destPath, (short) metaRepl,
        numStripes, blockSize, reporter, sReader);

    // set the modification time of the RAID file. This is done so that the modTime of the
    // RAID file reflects that contents of the source file that it has RAIDed. This should
    // also work for files that are being appended to. This is necessary because the time on
    // on the destination namenode may not be synchronised with the timestamp of the 
    // source namenode.
    outFs.setTimes(destPath, sourceFile.getModificationTime(), -1);

    FileStatus outstat = outFs.getFileStatus(destPath);
    FileStatus inStat = inFs.getFileStatus(inpath);
    if (sourceFile.getModificationTime() != inStat.getModificationTime()) {
      String msg = "Source file changed mtime during raiding from " +
          sourceFile.getModificationTime() + " to " +
          inStat.getModificationTime();
      throw new IOException(msg);
    }
    if (outstat.getModificationTime() != inStat.getModificationTime()) {
      String msg = "Parity file mtime " + outstat.getModificationTime() +
          " does not match source mtime " + inStat.getModificationTime();
      throw new IOException(msg);
    }
    LOG.info("Source file " + inpath + " of size " + inStat.getLen() +
        " Parity file " + destPath + " of size " + outstat.getLen() +
        " src mtime " + sourceFile.getModificationTime() +
        " parity mtime " + outstat.getModificationTime());
  }

  
  public static Decoder.DecoderInputStream unRaidCorruptInputStream(
      Configuration conf, Path srcPath, long blockSize, long corruptOffset,
      long limit) throws IOException {
    DistributedFileSystem srcFs =
        (DistributedFileSystem) srcPath.getFileSystem(conf);
    EncodingStatus encodingStatus =
        srcFs.getEncodingStatus(srcPath.toUri().getPath());
    Decoder decoder = new Decoder(conf,
        Codec.getCodec(encodingStatus.getEncodingPolicy().getCodec()));
    String parityFolder = conf.get(DFSConfigKeys.PARITY_FOLDER,
        DFSConfigKeys.DEFAULT_PARITY_FOLDER);
    
    return decoder.generateAlternateStream(srcFs, srcPath, srcFs,
        new Path(parityFolder + "/" + encodingStatus.getParityFileName()),
        blockSize, corruptOffset, limit, null);
  }

  /**
   * Returns current time.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  /**
   * Make an absolute path relative by stripping the leading /
   */
  static Path makeRelative(Path path) {
    if (!path.isAbsolute()) {
      return path;
    }
    String p = path.toUri().getPath();
    String relative = p.substring(1, p.length());
    return new Path(relative);
  }

  /**
   * Get the job id from the configuration
   */
  public static String getJobID(Configuration conf) {
    return conf.get("mapred.job.id", "localRaid" + df.format(new Date()));
  }
  
  static public void logRaidEncodingMetrics(String result, Codec codec,
      long delay, long numReadBytes, long numReadBlocks, long metaBlocks,
      long metaBytes, long savingBytes, Path srcPath, LOGTYPES type,
      FileSystem fs) {
    try {
      JSONObject json = new JSONObject();
      json.put("result", result);
      json.put("code", codec.id);
      json.put("delay", delay);
      json.put("readbytes", numReadBytes);
      json.put("readblocks", numReadBlocks);
      json.put("metablocks", metaBlocks);
      json.put("metabytes", metaBytes);
      json.put("savingbytes", savingBytes);
      json.put("path", srcPath.toString());
      json.put("type", type.name());
      json.put("cluster", fs.getUri().getAuthority());
      ENCODER_METRICS_LOG.info(json.toString());
    } catch (JSONException e) {
      LOG.warn("Exception when logging the Raid metrics: " + e.getMessage(), e);
    }
  }
}
