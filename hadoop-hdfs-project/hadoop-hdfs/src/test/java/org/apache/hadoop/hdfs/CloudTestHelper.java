package org.apache.hadoop.hdfs;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.hdfs.entity.*;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudFsDatasetImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudPersistenceProviderFactory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

public class CloudTestHelper {
  static final Log LOG = LogFactory.getLog(CloudTestHelper.class);

  private static List<INode> findAllINodes() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory
                        .getDataAccess(INodeDataAccess.class);
                return da.allINodes();
              }
            };
    return (List<INode>) handler.handle();
  }


  public static Map<Long, BlockInfoContiguous> findAllBlocks() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                Map<Long, BlockInfoContiguous> blkMap = new HashMap<>();
                BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory
                        .getDataAccess(BlockInfoDataAccess.class);

                List<BlockInfoContiguous> blocks = da.findAllBlocks();
                for (BlockInfoContiguous blk : blocks) {
                  blkMap.put(blk.getBlockId(), blk);
                }
                return blkMap;
              }
            };
    return (Map<Long, BlockInfoContiguous>) handler.handle();
  }

  public static Map<Long, ProvidedBlockCacheLoc> findCacheLocations() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                ProvidedBlockCacheLocDataAccess da = (ProvidedBlockCacheLocDataAccess) HdfsStorageFactory
                        .getDataAccess(ProvidedBlockCacheLocDataAccess.class);

                Map<Long, ProvidedBlockCacheLoc> blkMap = da.findAll();
                return blkMap;
              }
            };
    return (Map<Long, ProvidedBlockCacheLoc>) handler.handle();
  }


  private static List<Replica> findAllReplicas() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
                        .getDataAccess(ReplicaDataAccess.class);
                return da.findAll();
              }
            };
    return (List<Replica>) handler.handle();
  }


  private static List<ReplicaUnderConstruction> findAllReplicasUC() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                ReplicaUnderConstructionDataAccess da =
                        (ReplicaUnderConstructionDataAccess) HdfsStorageFactory
                                .getDataAccess(ReplicaUnderConstructionDataAccess.class);
                return da.findAll();
              }
            };
    return (List<ReplicaUnderConstruction>) handler.handle();
  }


  private static List<PendingBlockInfo> findAllPendingBlocks() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                PendingBlockDataAccess da = (PendingBlockDataAccess) HdfsStorageFactory
                        .getDataAccess(PendingBlockDataAccess.class);
                return da.findAll();
              }
            };
    return (List<PendingBlockInfo>) handler.handle();
  }

  private static List<CorruptReplica> findAllCorruptReplicas() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                CorruptReplicaDataAccess da = (CorruptReplicaDataAccess) HdfsStorageFactory
                        .getDataAccess(CorruptReplicaDataAccess.class);
                return da.findAll();
              }
            };
    return (List<CorruptReplica>) handler.handle();
  }

  private static List<ExcessReplica> findAllExcessReplicas() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                ExcessReplicaDataAccess da = (ExcessReplicaDataAccess) HdfsStorageFactory
                        .getDataAccess(ExcessReplicaDataAccess.class);
                return da.findAll();
              }
            };
    return (List<ExcessReplica>) handler.handle();
  }

  private static List<InvalidatedBlock> findAllInvalidatedBlocks() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) HdfsStorageFactory
                        .getDataAccess(InvalidateBlockDataAccess.class);
                return da.findAll();
              }
            };
    return (List<InvalidatedBlock>) handler.handle();
  }

  private static List<UnderReplicatedBlock> findAllUnderReplicatedBlocks() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                UnderReplicatedBlockDataAccess da = (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                        .getDataAccess(UnderReplicatedBlockDataAccess.class);
                return da.findAll();
              }
            };
    return (List<UnderReplicatedBlock>) handler.handle();
  }

  private static boolean match(Map<Long, Block> cloudView, Map<Long, BlockInfoContiguous> dbView) {
    if (cloudView.size() != dbView.size()) {
      List cv = new ArrayList(cloudView.values());
      Collections.sort(cv);
      List dbv = new ArrayList(dbView.values());
      Collections.sort(cv);
      LOG.info("HopsFS-Cloud Cloud Blocks " + Arrays.toString(cv.toArray()));
      LOG.info("HopsFS-Cloud DB Blocks " + Arrays.toString(dbv.toArray()));
    }

    assert cloudView.size() == dbView.size();

    for (long blkID : dbView.keySet()) {
      Block cloudBlock = cloudView.get(blkID);
      BlockInfoContiguous dbBlock = dbView.get(blkID);

      assert cloudBlock != null && dbBlock != null;

      assert cloudBlock.getCloudBucketID() == dbBlock.getCloudBucketID();
      assert cloudBlock.getGenerationStamp() == dbBlock.getGenerationStamp();
      assert cloudBlock.getNumBytes() == dbBlock.getNumBytes();

      assert dbBlock.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE;
    }

    return true;
  }

  public static boolean checkMetaDataMatch(Configuration conf) throws IOException {
    LOG.info("HopsFS-Cloud. CloudTestHelper. Checking Metadata");
    CloudPersistenceProvider cloud = null;

    try {
      cloud = CloudPersistenceProviderFactory.getCloudClient(conf);
      Map<Long, Block> cloudView = getAllCloudBlocks(cloud);
      Map<Long, BlockInfoContiguous> dbView = findAllBlocks();

      List sortDBBlocks = new ArrayList();
      sortDBBlocks.add(dbView.values());
      Collections.sort(sortDBBlocks);
      List sortCloudBlocks = new ArrayList();
      sortCloudBlocks.add(cloudView.values());
      Collections.sort(sortCloudBlocks);

      LOG.info("HopsFS-Cloud. DB View: " + sortDBBlocks);
      LOG.info("HopsFS-Cloud. Cloud View: " + sortCloudBlocks);


      String cloudProvider = conf.get(DFSConfigKeys.DFS_CLOUD_PROVIDER);
      if (!cloudProvider.equals(CloudProvider.AWS.name())) {
        match(cloudView, dbView); //fails becase of S3 eventual consistent LS, GCE is consistent
      }

      //block cache mapping
      Map<Long, ProvidedBlockCacheLoc> cacheLoc = findCacheLocations();
      assert cacheLoc.size() == dbView.size();

      for (Block blk : dbView.values()) {
        LOG.info("HopsFS-Cloud. Checking Block: "+blk);
        short bucketID = blk.getCloudBucketID();
        String blockKey = CloudFsDatasetImpl.getBlockKey(blk);
        String metaKey = CloudFsDatasetImpl.getMetaFileKey(blk);

        assert cloud.objectExists(bucketID, blockKey) == true;
        assert cloud.objectExists(bucketID, metaKey) == true;
        assert cloud.getObjectSize(bucketID, blockKey) == blk.getNumBytes();

        Map<String, String> metadata = cloud.getUserMetaData(bucketID, blockKey);
        assert Long.parseLong(metadata.get(CloudFsDatasetImpl.OBJECT_SIZE)) == blk.getNumBytes();
        assert Long.parseLong(metadata.get(CloudFsDatasetImpl.GEN_STAMP)) == blk.getGenerationStamp();

        metadata = cloud.getUserMetaData(bucketID, metaKey);
        assert metadata.size() == 0;
        assert cacheLoc.get(blk.getBlockId()) != null;
      }

      assert findAllReplicas().size() == 0;
      assert findAllReplicasUC().size() == 0;
      assert findAllExcessReplicas().size() == 0;
      assert findAllPendingBlocks().size() == 0;
      assert findAllCorruptReplicas().size() == 0;
      assert findAllInvalidatedBlocks().size() == 0;
      assert findAllUnderReplicatedBlocks().size() == 0;

    } finally {
      if (cloud != null) {
        cloud.shutdown();
      }
    }
    return true;
  }

  public static Map<Long, Block> getAllCloudBlocks(CloudPersistenceProvider cloud) throws IOException {
    return cloud.getAll();
  }
}
