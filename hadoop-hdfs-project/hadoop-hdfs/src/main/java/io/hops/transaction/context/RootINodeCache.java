package io.hops.transaction.context;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

import java.io.IOException;
import org.apache.hadoop.hdfs.protocol.HdfsConstantsClient;

/**
 * Created by salman on 2016-08-21.
 */
public class RootINodeCache {

  protected final static Log LOG = LogFactory.getLog(RootINodeCache.class);
  private static final int UPDATE_TIMER = 200; //ms
  private static RootINodeCacheUpdaterThread rootCacheUpdater;
  private static INode rootINode = null;
  private static boolean running = false;
  private static RootINodeCache instance;
  private static final boolean ENABLE_CACHE = true;

  static {
    instance = new RootINodeCache();
  }

  private RootINodeCache() {
  }

  public static RootINodeCache getInstance() {
    return instance;
  }

  public static void start() {
    if (!running && ENABLE_CACHE) {
      rootCacheUpdater = new RootINodeCacheUpdaterThread();
      rootCacheUpdater.start();
    }
  }

  public static void stop() {
    try {
      if (running) {
        running = false;
        rootCacheUpdater.join();
        rootCacheUpdater = null;
      }
    } catch (InterruptedException e) {
      LOG.error(e);
      Thread.currentThread().interrupt();
    }
  }

  public static INode getRootINode() {
    if(rootCacheUpdater!=null) {
      synchronized (rootCacheUpdater) {
        return rootINode;
      }
    }else{
      return null;
    }
  }
  
  public static void forceRefresh(){
    if(rootCacheUpdater != null){
      synchronized (rootCacheUpdater) {
        rootINode = null;
      }
    }
  }
  
  public static boolean isRootInCache() {
    return rootINode != null;
  }

  private static class RootINodeCacheUpdaterThread extends Thread {

    @Override
    public void run() {
      running = true;
      LOG.debug("RootCache Started");
      final long rootPartitionId = INode.calculatePartitionId(HdfsConstantsClient.GRANDFATHER_INODE_ID,
          INodeDirectory.ROOT_NAME, INodeDirectory.ROOT_DIR_DEPTH);

      LightWeightRequestHandler getRootINode =
              new LightWeightRequestHandler(HDFSOperationType.GET_ROOT) {
                @Override
                public Object performTask() throws IOException {
                  INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory
                          .getDataAccess(INodeDataAccess.class);
                  INode rootINode = (INode) da.findInodeByNameParentIdAndPartitionIdPK(INodeDirectory.ROOT_NAME,
                      HdfsConstantsClient.GRANDFATHER_INODE_ID, rootPartitionId);
                  return rootINode;
                }
              };

      while (running) {
        try {
          Thread.sleep(UPDATE_TIMER);
          INode rootInodeRet = (INode) getRootINode.handle();
          if (rootInodeRet != null) {
            synchronized (rootCacheUpdater) {
              rootINode = rootInodeRet;
            }
          } else {
            LOG.debug("RootCache: root does not exist.");
          }
        } catch (InterruptedException e) {
          LOG.warn(e);
          Thread.currentThread().interrupt();
        } catch (IOException e) {
          LOG.warn(e);
        }
      } // end while
    }
  }
}
