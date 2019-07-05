package org.apache.hadoop.hdfs.server.blockmanagement;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.ProvidedBlockCacheLocDataAccess;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.metadata.hdfs.entity.ProvidedBlockCacheLoc;
import io.hops.metadata.hdfs.entity.ReplicaBase;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.Block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ProvidedBlocksCacheHelper {
  public static final Log LOG = LogFactory.getLog(ProvidedBlocksCacheHelper.class);

  public static ProvidedBlockCacheLoc getProvidedBlockCacheLocation(final long blkID)
          throws StorageException {
    try {
      LightWeightRequestHandler h =
              new LightWeightRequestHandler(HDFSOperationType.GET_CLOUD_BLKS_CACHE_LOC) {
                @Override
                public Object performTask() throws IOException {
                  ProvidedBlockCacheLocDataAccess da = (ProvidedBlockCacheLocDataAccess) HdfsStorageFactory
                          .getDataAccess(ProvidedBlockCacheLocDataAccess.class);
                  return da.findByBlockID(blkID);
                }
              };
      return (ProvidedBlockCacheLoc) h.handle();
    } catch (IOException e) {
      LOG.error(e, e);
      StorageException up = new StorageException(e);
      throw up;
    }
  }

  public static void updateProvidedBlockCacheLocation(final Block newBlock,
                                                      final DatanodeStorageInfo[] targets) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.UPDATE_CLOUD_BLKS_CACHE_LOC) {
      @Override
      public Object performTask() throws IOException {
        ProvidedBlockCacheLocDataAccess da = (ProvidedBlockCacheLocDataAccess) HdfsStorageFactory
                .getDataAccess(ProvidedBlockCacheLocDataAccess.class);
        LOG.info("HopsFS-Cloud. Added  provided block cache entry for Block ID: " + newBlock.getBlockId());
        assert targets.length == 1;
        List<ProvidedBlockCacheLoc> locs = new ArrayList<>();
        locs.add(new ProvidedBlockCacheLoc(newBlock.getBlockId(), targets[0].getSid()));
        da.update(locs);
        return null;
      }
    }.handle();
  }

  public static void deleteProvidedBlockCacheLocation(final List<Block> deletedBlocks) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DELETE_CLOUD_BLKS_CACHE_LOC) {
      @Override
      public Object performTask() throws IOException {
        ProvidedBlockCacheLocDataAccess da = (ProvidedBlockCacheLocDataAccess) HdfsStorageFactory
                .getDataAccess(ProvidedBlockCacheLocDataAccess.class);
        List<Long> locs = new ArrayList<>();
        for (Block block : deletedBlocks) {
          locs.add(block.getBlockId());
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("HopsFS-Cloud. Deleting cache entry for block ID: " +
                  Arrays.toString(locs.toArray()));
        }
        da.delete(locs);
        return null;
      }
    }.handle();
  }

  public static Map<Long, ProvidedBlockCacheLoc> batchReadCacheLocs(final List<Block> blocks)
          throws IOException {
    HopsTransactionalRequestHandler h =
            new HopsTransactionalRequestHandler(
                    HDFSOperationType.BATCH_READ_CLOUD_BLKS_CACHE_LOCS) {
              @Override
              public Object performTask() throws IOException {
                ProvidedBlockCacheLocDataAccess da = (ProvidedBlockCacheLocDataAccess) HdfsStorageFactory
                        .getDataAccess(ProvidedBlockCacheLocDataAccess.class);
                long locs[] = new long[blocks.size()];
                for (int i = 0; i < blocks.size(); i++) {
                  locs[i] = blocks.get(i).getBlockId();
                }
                if (LOG.isDebugEnabled()) {
                  LOG.debug("HopsFS-Cloud. Batch read cache entries for block IDs: " +
                          Arrays.toString(locs));
                }
                return da.findByBlockIDs(locs);
              }

              @Override
              public void acquireLock(TransactionLocks locks) throws IOException {
              }
            };
    return (Map<Long, ProvidedBlockCacheLoc>) h.handle();
  }
}
