package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.DataChecksum;

public class CloudFsDatasetImpl extends FsDatasetImpl {
  /**
   * An FSDataset has a directory where it loads its data files.
   *
   * @param datanode
   * @param storage
   * @param conf
   */
  public static final String GEN_STAMP = "GEN_STAMP";
  public static final String OBJECT_SIZE = "OBJECT_SIZE";

  static final Log LOG = LogFactory.getLog(CloudFsDatasetImpl.class);
  private CloudPersistenceProvider cloud;
  private final boolean bypassCache;

  CloudFsDatasetImpl(DataNode datanode, DataStorage storage,
                     Configuration conf) throws IOException {
    super(datanode, storage, conf);
    bypassCache = conf.getBoolean(DFSConfigKeys.DFS_DN_CLOUD_BYPASS_CACHE_KEY,
            DFSConfigKeys.DFS_DN_CLOUD_BYPASS_CACHE_DEFAULT);
    cloud = CloudPersistenceProviderFactory.getCloudClient(conf);
    cloud.checkAllBuckets();
  }

  @Override
  public void preFinalize(ExtendedBlock b) throws IOException {
    // upload to cloud
    if (!b.isProvidedBlock()) {
      super.preFinalize(b);
    } else {
      preFinalizeInternal(b);
    }
  }

  public void preFinalizeInternal(ExtendedBlock b) throws IOException {
    LOG.info("HopsFS-Cloud. Prefinalize Stage. Uploading... Block: " + b.getLocalBlock());

    ReplicaInfo replicaInfo = getReplicaInfo(b);
    File blockFile = replicaInfo.getBlockFile();
    File metaFile = replicaInfo.getMetaFile();
    String blockFileKey = getBlockKey(b.getLocalBlock());
    String metaFileKey = getMetaFileKey(b.getLocalBlock());

    if (!cloud.objectExists(b.getCloudBucketID(), blockFileKey)
            && !cloud.objectExists(b.getCloudBucketID(), metaFileKey)) {
      cloud.uploadObject(b.getCloudBucketID(), blockFileKey, blockFile,
              getBlockFileMetadata(b.getLocalBlock()));
      cloud.uploadObject(b.getCloudBucketID(), metaFileKey, metaFile,
              getMetaMetadata(b.getLocalBlock()));
    } else {
      LOG.error("HopsFS-Cloud. Block: " + b + " alreay exists.");
      throw new IOException("Block: " + b + " alreay exists.");
    }
  }

  @Override
  public synchronized void finalizeBlock(ExtendedBlock b) throws IOException {
    if (!b.isProvidedBlock()) {
      super.finalizeBlock(b);
    } else {
      finalizeBlockInternal(b);
    }
  }

  private synchronized void finalizeBlockInternal(ExtendedBlock b) throws IOException {
    LOG.info("HopsFS-Cloud. Finalizing bloclk. Block: " + b.getLocalBlock());
    if (Thread.interrupted()) {
      // Don't allow data modifications from interrupted threads
      throw new IOException("Cannot finalize block from Interrupted Thread");
    }

    ReplicaInfo replicaInfo = getReplicaInfo(b);
    File blockFile = replicaInfo.getBlockFile();
    File metaFile = replicaInfo.getMetaFile();
    long dfsBytes = blockFile.length() + metaFile.length();

    // release rbw space
    FsVolumeImpl v = (FsVolumeImpl) replicaInfo.getVolume();
    v.releaseReservedSpace(replicaInfo.getBytesReserved());
    v.decDfsUsed(b.getBlockPoolId(), dfsBytes);

    // remove from volumeMap, so we can get it from s3 instead
    volumeMap.remove(b.getBlockPoolId(), replicaInfo.getBlockId());

    if (bypassCache) {
      blockFile.delete();
      metaFile.delete();
    } else {
      //move the blocks to the cache
      FsVolumeImpl cloudVol = getCloudVolume();
      File cDir = cloudVol.getCacheDir(b.getBlockPoolId());

      File movedBlock = new File(cDir, getBlockKey(b.getLocalBlock()));
      if (!blockFile.renameTo(movedBlock)) {
        LOG.warn("HopsFS-Cloud. Unable to move finalized block to cache");
        blockFile.delete();
      } else {
        providedBlocksCacheUpdateTS(b.getBlockPoolId(), movedBlock);
        LOG.info("HopsFS-Cloud. Moved " + movedBlock.getName() + " to cache. path " + movedBlock);
      }

      File movedMetaFile = new File(cDir, getMetaFileKey(b.getLocalBlock()));
      if (!metaFile.renameTo(movedMetaFile)) {
        LOG.warn("HopsFS-Cloud. Unable to move finalized block meta file to cache");
        metaFile.delete();
      } else {
        providedBlocksCacheUpdateTS(b.getBlockPoolId(), movedMetaFile);
        LOG.info("HopsFS-Cloud. Moved " + movedMetaFile.getName() + " to cache. path : " + movedMetaFile);
      }
    }
  }

  @Override
  public void postFinalize(ExtendedBlock b) throws IOException {
  }

  @Override // FsDatasetSpi
  public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
          throws IOException {

    LOG.info("HopsFS-Cloud. Get block inputstream "+b.getLocalBlock());
    if (!b.isProvidedBlock()
            //case of reading a provided block that is being written to disk
            || (b.isProvidedBlock() && volumeMap.get(b.getBlockPoolId(), b.getBlockId()) != null)
    ) {
      return super.getBlockInputStream(b, seekOffset);
    } else {
      FsVolumeImpl cloudVolume = getCloudVolume();
      File localBlkCopy = new File(cloudVolume.getCacheDir(b.getBlockPoolId()),
              getBlockKey(b.getLocalBlock()));
      String blockFileKey = getBlockKey(b.getLocalBlock());

      return getInputStreamInternal(b.getCloudBucketID(), blockFileKey,
              localBlkCopy, b.getBlockPoolId(), seekOffset);
    }
  }

  @Override // FsDatasetSpi
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
          throws IOException {
    if (!b.isProvidedBlock()) {
      return super.getMetaDataInputStream(b);
    } else {
      FsVolumeImpl cloudVolume = getCloudVolume();
      String metaFileKey = getMetaFileKey(b.getLocalBlock());
      File localMetaFileCopy = new File(cloudVolume.getCacheDir(b.getBlockPoolId()),
              getMetaFileKey(b.getLocalBlock()));

      InputStream is = getInputStreamInternal(b.getCloudBucketID(), metaFileKey,
              localMetaFileCopy, b.getBlockPoolId(), 0);
      LengthInputStream lis = new LengthInputStream(is, localMetaFileCopy.length());

      return lis;
    }
  }

  private InputStream getInputStreamInternal(short cloudBucketID, String objectKey,
                                             File localCopy, String bpid,
                                             long seekOffset) throws IOException {
    try {
      //check if object exists
      long startTime = System.currentTimeMillis();

//      if (!cloud.objectExists(cloudBucketID, objectKey)) {
//        throw new IOException("Expected block " + objectKey + " does not exist.");
//      }

      boolean download = bypassCache;
      if (!bypassCache) {
        if (!(localCopy.exists() && localCopy.length() > 0)) {
          localCopy.delete();
          download = true;
        } else {
          LOG.info("HopsFS-Cloud. Reading provided block from cache. Block: " + objectKey);
        }
      }
      if (download) {
        cloud.downloadObject(cloudBucketID, objectKey, localCopy);
      }

      InputStream ioStream = new FileInputStream(localCopy);
      ioStream.skip(seekOffset);

      providedBlocksCacheUpdateTS(bpid, localCopy);  //after opening the file put it in the cache

      LOG.info("HopsFS-Cloud. " + objectKey + " GetInputStream Fn took :" + (System.currentTimeMillis() - startTime));
      return ioStream;
    } catch (IOException e) {
      throw new IOException("Could not read " + objectKey + ". ", e);
    }
  }

  @Override // FsDatasetSpi
  public ReplicaInfo getReplica(ExtendedBlock b) {
    if (!b.isProvidedBlock()) {
      return super.getReplica(b);
    } else {
      return getReplicaInternal(b);
    }
  }

  public ReplicaInfo getReplicaInternal(ExtendedBlock b) {
    ReplicaInfo replicaInfo = super.getReplica(b);
    if (replicaInfo != null) {
      return replicaInfo;
    }

    try {
      String blockFileKey = getBlockKey(b.getLocalBlock());
      if (cloud.objectExists(b.getCloudBucketID(), blockFileKey)) {
        Map<String, String> metadata = cloud.getUserMetaData(b.getCloudBucketID(), blockFileKey);

        long genStamp = Long.parseLong((String) metadata.get(GEN_STAMP));
        long size = Long.parseLong((String) metadata.get(OBJECT_SIZE));

        FinalizedReplica info = new FinalizedReplica(b.getBlockId(), size, genStamp,
                b.getCloudBucketID(), null, null);
        return info;
      }

    } catch (IOException up) {
      LOG.info(up, up);
    }
    return null;
  }

  // Finalized provided blocks are removed from the replica map
  public boolean isProvideBlockFinalized(ExtendedBlock b) {
    return super.getReplica(b) == null ? true : false;
  }


  private String getCloudProviderName() {
    return conf.get(DFSConfigKeys.DFS_CLOUD_PROVIDER,
            DFSConfigKeys.DFS_CLOUD_PROVIDER_DEFAULT);
  }

  @Override
  FsVolumeImpl getNewFsVolumeImpl(FsDatasetImpl dataset, String storageID,
                                  File currentDir, Configuration conf,
                                  StorageType storageType) throws IOException {

    if (storageType == StorageType.CLOUD) {
      if (getCloudProviderName().compareToIgnoreCase(CloudProvider.AWS.name()) == 0) {
        return new CloudFsVolumeImpl(this, storageID, currentDir, conf, storageType);
      } else {
        throw new UnsupportedOperationException("Cloud provider '" +
                getCloudProviderName() + "' is not supported");
      }
    } else {
      return new FsVolumeImpl(this, storageID, currentDir, conf, storageType);
    }
  }

  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
  @Override // FsDatasetSpi
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {

    final List<String> errors = new ArrayList<String>();

    for (Block invalidBlk : invalidBlks) {
      if (invalidBlk.isProvidedBlock()) {
        invalidateProvidedBlock(bpid, invalidBlk, errors);
      } else {
        super.invalidateBlock(bpid, invalidBlk, errors);
      }
    }

    printInvalidationErrors(errors, invalidBlks.length);
  }

  private void invalidateProvidedBlock(String bpid, Block invalidBlk, List<String> errors) throws IOException {
    final File f;
    final FsVolumeImpl v;
    ReplicaInfo info;
    synchronized (this) {
      info = volumeMap.get(bpid, invalidBlk);
    }

    // case when the block is not yet uploaded to the cloud
    if (info != null) {
      super.invalidateBlock(bpid, invalidBlk, errors);
    } else {
      // block is in the cloud.
      // Edge cases such as deletion of be blocks in flight
      // should be taekn care of by the block reporting system

      FsVolumeImpl cloudVolume = getCloudVolume();

      if (cloudVolume == null) {
        errors.add("HopsFS-Cloud. Failed to delete replica " + invalidBlk);
      }

      File localBlkCopy = new File(cloudVolume.getCacheDir(bpid),
              getBlockKey(invalidBlk));
      File localMetaFileCopy = new File(cloudVolume.getCacheDir(bpid),
              getMetaFileKey(invalidBlk));

      LOG.info("HopsFS-Cloud. Scheduling async deletion of block: " + invalidBlk);
      File volumeDir = cloudVolume.getCurrentDir();
      asyncDiskService.deleteAsyncProvidedBlock(new ExtendedBlock(bpid, invalidBlk),
              cloud, localBlkCopy, localMetaFileCopy, volumeDir);
    }
  }

  @Override
  FinalizedReplica updateReplicaUnderRecovery(
          String bpid,
          ReplicaUnderRecovery rur,
          long recoveryId,
          long newBlockId,
          long newlength,
          short cloudBlockID) throws IOException {

    if (!rur.isProvidedBlock()) {
      return super.updateReplicaUnderRecovery(bpid, rur, recoveryId,
              newBlockId, newlength, cloudBlockID);
    } else {
      return updateReplicaUnderRecoveryInternal(bpid, rur, recoveryId,
              newBlockId, newlength, cloudBlockID);
    }
  }

  FinalizedReplica updateReplicaUnderRecoveryInternal(
          String bpid,
          ReplicaUnderRecovery rur,
          long recoveryId,
          long newBlockId,
          long newlength,
          short cloudBlockID) throws IOException {
    //check recovery id
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException("rur.getRecoveryID() != recoveryId = " +
              recoveryId + ", rur=" + rur);
    }

    boolean copyOnTruncate = newBlockId > 0L && rur.getBlockId() != newBlockId;
    if (copyOnTruncate == true) {
      throw new UnsupportedOperationException("Truncate using copy is not supported");
    }

    // Create new truncated block with truncated data and bump up GS
    //update length
    if (rur.getNumBytes() < newlength) {
      throw new IOException(
              "rur.getNumBytes() < newlength = " + newlength + ", rur=" + rur);
    }

    if (rur.getNumBytes() >= newlength) { // Create a new block even if zero bytes are truncated,
      // because GS needs to be increased.
      truncateProvidedBlock(bpid, rur, rur.getNumBytes(), newlength, recoveryId);
      // update RUR with the new length
      rur.setNumBytesNoPersistance(newlength);
      rur.setGenerationStampNoPersistance(recoveryId);
    }

    return new FinalizedReplica(rur, null, null);
  }

  private void truncateProvidedBlock(String bpid, ReplicaInfo rur, long oldlen,
                                     long newlen, long newGS) throws IOException {
    LOG.info("HopsFS-Cloud. Truncating a block: " + rur.getBlockId() + "_" + rur.getGenerationStamp());

    Block bOld = new Block(rur.getBlockId(), rur.getNumBytes(), rur.getGenerationStamp(),
            rur.getCloudBucketID());
    String oldBlkKey = getBlockKey(bOld);
    String oldBlkMetaKey = getMetaFileKey(bOld);

    if (newlen > oldlen) {
      throw new IOException("Cannot truncate block to from oldlen (=" + oldlen +
              ") to newlen (=" + newlen + ")");
    }

    //download the block
    FsVolumeImpl vol = getCloudVolume();
    File blockFile = new File(vol.getCacheDir(bpid), oldBlkKey);
    File metaFile = new File(vol.getCacheDir(bpid), oldBlkMetaKey);

    if (!(blockFile.exists() && blockFile.length() == bOld.getNumBytes())) {
      blockFile.delete(); //delete old files if any
      cloud.downloadObject(rur.getCloudBucketID(), oldBlkKey, blockFile);
      providedBlocksCacheUpdateTS(bpid, blockFile);
    }

    if (!(metaFile.exists() && metaFile.length() > 0)) {
      metaFile.delete(); //delete old files if any
      cloud.downloadObject(rur.getCloudBucketID(), oldBlkMetaKey, metaFile);
      providedBlocksCacheUpdateTS(bpid, metaFile);
    }

    //truncate the disk block and update the metafile
    DataChecksum dcs = BlockMetadataHeader.readHeader(metaFile).getChecksum();
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long n = (newlen - 1) / bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + n * checksumsize;
    long lastchunkoffset = (n - 1) * bpc;
    int lastchunksize = (int) (newlen - lastchunkoffset);
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)];

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      //truncate blockFile
      blockRAF.setLength(newlen);

      //read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
    }

    //compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    //update metaFile
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }

    //update the blocks
    LOG.info("HopsFS-Cloud. Truncated on disk copy of the block: " + bOld);

    Block bNew = new Block(rur.getBlockId(), newlen, newGS, rur.getCloudBucketID());
    String newBlkKey = getBlockKey(bNew);
    String newBlkMetaKey = getMetaFileKey(bNew);

    if (!cloud.objectExists(rur.getCloudBucketID(), newBlkKey)
            && !cloud.objectExists(rur.getCloudBucketID(), newBlkMetaKey)) {
      LOG.info("HopsFS-Cloud. Uploading Truncated Block: " + bNew);
      cloud.uploadObject(rur.getCloudBucketID(), newBlkKey, blockFile,
              getBlockFileMetadata(bNew));
      cloud.uploadObject(rur.getCloudBucketID(), newBlkMetaKey, metaFile,
              getMetaMetadata(bNew));
    } else {
      LOG.error("HopsFS-Cloud. Block: " + b + " alreay exists.");
      throw new IOException("Block: " + b + " alreay exists.");
    }

    LOG.info("HopsFS-Cloud. Deleting old block from cloud. Block: " + bOld);
    cloud.deleteObject(rur.getCloudBucketID(), oldBlkKey);
    cloud.deleteObject(rur.getCloudBucketID(), oldBlkMetaKey);

    LOG.info("HopsFS-Cloud. Deleting disk tmp copy: " + bOld);
    blockFile.delete();
    metaFile.delete();

    //remove the entry from replica map
    volumeMap.remove(bpid, bNew.getBlockId());
  }

  @Override
  public void checkReplicaFiles(final ReplicaInfo r) throws IOException {
    if (!r.isProvidedBlock()) {
      super.checkReplicaFiles(r);
    } else {
      checkReplicaFilesInternal(r);
    }
  }

  public void checkReplicaFilesInternal(final ReplicaInfo r) throws IOException {
    //check replica's file
    // make sure that the block and the meta objects exist in S3.
    Block b = new Block(r.getBlockId(), r.getNumBytes(),
            r.getGenerationStamp(), r.getCloudBucketID());
    String blockKey = getBlockKey(b);
    String metaKey = getMetaFileKey(b);

    if (!cloud.objectExists(r.getCloudBucketID(), blockKey)) {
      throw new FileNotFoundException("Block: " + b + " not found in the cloud storage");
    }

    long blockSize = cloud.getObjectSize(r.getCloudBucketID(), blockKey);
    if (blockSize != r.getNumBytes()) {
      throw new IOException(
              "File length mismatched. Expected: " + r.getNumBytes() + " Got: " + blockSize);
    }

    if (!cloud.objectExists(r.getCloudBucketID(), metaKey)) {
      throw new FileNotFoundException("Meta Object for Block: " + b + " not found in the cloud " +
              "storage");
    }

    long metaFileSize = cloud.getObjectSize(r.getCloudBucketID(), metaKey);
    if (metaFileSize == 0) {
      throw new IOException("Metafile is empty. Block: " + b);
    }
  }

  @Override
  public synchronized FsVolumeImpl getVolume(final ExtendedBlock b) {
    if (!b.isProvidedBlock()) {
      return super.getVolume(b);
    } else {
      return getVolumeInternal(b);
    }
  }

  @Override // FsDatasetSpi
  public Map<DatanodeStorage, BlockReport> getBlockReports(String bpid) {
    Map<DatanodeStorage, BlockReport> blockReportsMap =
            new HashMap<>();
    LOG.error("Block reportnig for cloud storage is not yet implemented");
    return blockReportsMap;
  }

  public synchronized FsVolumeImpl getVolumeInternal(final ExtendedBlock b) {
    return getCloudVolume();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    cloud.shutdown();
  }

  @VisibleForTesting
  public FsVolumeImpl getCloudVolume() {
    for (FsVolumeImpl vol : getVolumes()) {
      if (vol.getStorageType() == StorageType.CLOUD) {
        return vol;
      }
    }
    return null;
  }

  private Map<String, String> getBlockFileMetadata(Block b) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(GEN_STAMP, Long.toString(b.getGenerationStamp()));
    metadata.put(OBJECT_SIZE, Long.toString(b.getNumBytes()));
    return metadata;
  }

  private Map<String, String> getMetaMetadata(Block b) {
    Map<String, String> metadata = new HashMap<>();
    return metadata;
  }

  public static String getBlockKey(Block b) {
    return b.getBlockName() + "_" + b.getGenerationStamp();
  }

  public static String getMetaFileKey(Block b) {
    String metaFileID = DatanodeUtil.getMetaName(b.getBlockName(), b.getGenerationStamp());
    return metaFileID;
  }

  public void providedBlocksCacheUpdateTS(String bpid, File f) throws IOException {
    FsVolumeImpl cloudVolume = getCloudVolume();
    cloudVolume.getBlockPoolSlice(bpid).fileAccessed(f);
  }

  public void providedBlocksCacheDelete(String bpid, File f) throws IOException {
    FsVolumeImpl cloudVolume = getCloudVolume();
    cloudVolume.getBlockPoolSlice(bpid).fileDeleted(f);
  }

  @VisibleForTesting
  public CloudPersistenceProvider getCloudConnector(){
    return cloud;
  }

  @VisibleForTesting
  public void installMockCloudConnector(CloudPersistenceProvider mock){
    cloud = mock;
  }
}

