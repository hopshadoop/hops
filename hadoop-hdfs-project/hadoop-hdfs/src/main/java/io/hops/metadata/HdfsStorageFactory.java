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
package io.hops.metadata;

import com.google.common.annotations.VisibleForTesting;
import io.hops.DalDriver;
import io.hops.DalStorageFactory;
import io.hops.StorageConnector;
import io.hops.common.IDsMonitor;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.log.NDCWrapper;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.hdfs.entity.*;
import io.hops.resolvingcache.Cache;
import io.hops.metadata.adaptor.BlockInfoDALAdaptor;
import io.hops.metadata.adaptor.CacheDirectiveDALAdaptor;
import io.hops.metadata.adaptor.CachePoolDALAdaptor;
import io.hops.metadata.adaptor.DirectoryWithQuotaFeatureDALAdaptor;
import io.hops.metadata.adaptor.INodeDALAdaptor;
import io.hops.metadata.adaptor.LeaseDALAdaptor;
import io.hops.metadata.adaptor.PendingBlockInfoDALAdaptor;
import io.hops.metadata.adaptor.ReplicaUnderConstructionDALAdaptor;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.common.entity.ArrayVariable;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.IntVariable;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.StringVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.LeDescriptorDataAccess;
import io.hops.metadata.election.entity.LeDescriptor.HdfsLeDescriptor;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.*;
import io.hops.transaction.lock.LockFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.Lease;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature;

public class HdfsStorageFactory {

  private static boolean isDALInitialized = false;
  private static DalStorageFactory dStorageFactory;
  private static Map<Class, EntityDataAccess> dataAccessAdaptors =
      new HashMap<>();
  
  public static StorageConnector getConnector() {
    return dStorageFactory.getConnector();
  }

  @VisibleForTesting
  public static void reset() {
    isDALInitialized = false;
    IDsMonitor.reset();
  }

  public static boolean isInitialized(){
    return isDALInitialized;
  }

  public static void setConfiguration(Configuration conf) throws IOException {
    IDsMonitor.getInstance().setConfiguration(conf);
    Cache.getInstance(conf);
    LockFactory.getInstance().setConfiguration(conf);
    NDCWrapper.enableNDC(conf.getBoolean(DFSConfigKeys.DFS_NDC_ENABLED_KEY,
        DFSConfigKeys.DFS_NDC_ENABLED_DEFAULT));
    TransactionsStats.getInstance().setConfiguration(
        conf.getBoolean(DFSConfigKeys.DFS_TRANSACTION_STATS_ENABLED,
            DFSConfigKeys.DFS_TRANSACTION_STATS_ENABLED_DEFAULT),
        conf.get(DFSConfigKeys.DFS_TRANSACTION_STATS_DIR,
            DFSConfigKeys.DFS_TRANSACTION_STATS_DIR_DEFAULT), conf.getInt
            (DFSConfigKeys.DFS_TRANSACTION_STATS_WRITER_ROUND, DFSConfigKeys
                .DFS_TRANSACTION_STATS_WRITER_ROUND_DEFAULT), conf
            .getBoolean(DFSConfigKeys.DFS_TRANSACTION_STATS_DETAILED_ENABLED,
                DFSConfigKeys.DFS_TRANSACTION_STATS_DETAILED_ENABLED_DEFAULT));
    if (!isDALInitialized) {
      HdfsVariables.registerDefaultValues(conf);
      addToClassPath(conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_JAR_FILE,
          DFSConfigKeys.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT));
      dStorageFactory = DalDriver.load(
          conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_CLASS,
              DFSConfigKeys.DFS_STORAGE_DRIVER_CLASS_DEFAULT));
      dStorageFactory.setConfiguration(getMetadataClusterConfiguration(conf));
      initDataAccessWrappers();
      EntityManager.addContextInitializer(getContextInitializer());
  
      UsersGroups.init((UserDataAccess) getDataAccess
          (UserDataAccess.class), (UserGroupDataAccess) getDataAccess
          (UserGroupDataAccess.class), (GroupDataAccess) getDataAccess
          (GroupDataAccess.class), conf.getInt(CommonConfigurationKeys
          .HOPS_UG_CACHE_SECS, CommonConfigurationKeys
          .HOPS_UG_CACHE_SECS_DEFAULT), conf.getInt(CommonConfigurationKeys
          .HOPS_UG_CACHE_SIZE, CommonConfigurationKeys
          .HOPS_UG_CACHE_SIZE_DEFUALT));
      
      isDALInitialized = true;
    }
  }

  public static Properties getMetadataClusterConfiguration(Configuration conf)
      throws IOException {
    String configFile = conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_CONFIG_FILE,
        DFSConfigKeys.DFS_STORAGE_DRIVER_CONFIG_FILE_DEFAULT);
    Properties clusterConf = new Properties();
    InputStream inStream =
        StorageConnector.class.getClassLoader().getResourceAsStream(configFile);
    clusterConf.load(inStream);
    if(inStream == null){
      throw new FileNotFoundException("Unable to load database configuration file");
    }
    return clusterConf;
  }
  
  //[M]: just for testing purposes
  private static void addToClassPath(String s)
      throws StorageInitializtionException {
    try {
      File f = new File(s);
      URL u = f.toURI().toURL();
      URLClassLoader urlClassLoader =
          (URLClassLoader) ClassLoader.getSystemClassLoader();
      Class urlClass = URLClassLoader.class;
      Method method =
          urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
      method.setAccessible(true);
      method.invoke(urlClassLoader, new Object[]{u});
    } catch (MalformedURLException | SecurityException | NoSuchMethodException | InvocationTargetException | IllegalArgumentException | IllegalAccessException ex) {
      throw new StorageInitializtionException(ex);
    }
  }
  
  private static void initDataAccessWrappers() {
    dataAccessAdaptors.clear();
    dataAccessAdaptors.put(BlockInfoDataAccess.class, new BlockInfoDALAdaptor(
        (BlockInfoDataAccess) getDataAccess(BlockInfoDataAccess.class)));
    dataAccessAdaptors.put(ReplicaUnderConstructionDataAccess.class,
        new ReplicaUnderConstructionDALAdaptor(
            (ReplicaUnderConstructionDataAccess) getDataAccess(
                ReplicaUnderConstructionDataAccess.class)));
    dataAccessAdaptors.put(LeaseDataAccess.class, new LeaseDALAdaptor(
        (LeaseDataAccess) getDataAccess(LeaseDataAccess.class)));
    dataAccessAdaptors.put(PendingBlockDataAccess.class,
        new PendingBlockInfoDALAdaptor((PendingBlockDataAccess) getDataAccess(
            PendingBlockDataAccess.class)));
    dataAccessAdaptors.put(INodeDataAccess.class, new INodeDALAdaptor(
        (INodeDataAccess) getDataAccess(INodeDataAccess.class)));
    dataAccessAdaptors.put(DirectoryWithQuotaFeatureDataAccess.class,
        new DirectoryWithQuotaFeatureDALAdaptor((DirectoryWithQuotaFeatureDataAccess) getDataAccess(
            DirectoryWithQuotaFeatureDataAccess.class)));
    dataAccessAdaptors.put(CacheDirectiveDataAccess.class, new CacheDirectiveDALAdaptor(
        (CacheDirectiveDataAccess) getDataAccess(CacheDirectiveDataAccess.class)));
    dataAccessAdaptors.put(CachePoolDataAccess.class, new CachePoolDALAdaptor(
        (CachePoolDataAccess) getDataAccess(CachePoolDataAccess.class)));
  }

  private static ContextInitializer getContextInitializer() {
    return new ContextInitializer() {
      @Override
      public Map<Class, EntityContext> createEntityContexts() {
        Map<Class, EntityContext> entityContexts =
            new HashMap<>();

        BlockInfoContext bic = new BlockInfoContext(
            (BlockInfoDataAccess) getDataAccess(BlockInfoDataAccess.class));
        entityContexts.put(BlockInfoContiguous.class, bic);
        entityContexts.put(BlockInfoContiguousUnderConstruction.class, bic);
        entityContexts.put(ReplicaUnderConstruction.class,
            new ReplicaUnderConstructionContext(
                (ReplicaUnderConstructionDataAccess) getDataAccess(
                    ReplicaUnderConstructionDataAccess.class)));
        entityContexts.put(Replica.class, new ReplicaContext(
            (ReplicaDataAccess) getDataAccess(ReplicaDataAccess.class)));
        entityContexts.put(ExcessReplica.class, new ExcessReplicaContext(
            (ExcessReplicaDataAccess) getDataAccess(
                ExcessReplicaDataAccess.class)));
        entityContexts.put(InvalidatedBlock.class, new InvalidatedBlockContext(
            (InvalidateBlockDataAccess) getDataAccess(
                InvalidateBlockDataAccess.class)));
        entityContexts.put(Lease.class, new LeaseContext(
            (LeaseDataAccess) getDataAccess(LeaseDataAccess.class)));
        entityContexts.put(LeasePath.class, new LeasePathContext(
            (LeasePathDataAccess) getDataAccess(LeasePathDataAccess.class)));
        entityContexts.put(LeaseCreationLock.class, new LeaseCreationLocksContext(
                (LeaseCreationLocksDataAccess) getDataAccess(LeaseCreationLocksDataAccess.class)));
        entityContexts.put(PendingBlockInfo.class, new PendingBlockContext(
            (PendingBlockDataAccess) getDataAccess(
                PendingBlockDataAccess.class)));

        INodeContext inodeContext = new INodeContext(
            (INodeDataAccess) getDataAccess(INodeDataAccess.class));
        entityContexts.put(INode.class, inodeContext);
        entityContexts.put(INodeDirectory.class, inodeContext);
        entityContexts.put(INodeFile.class, inodeContext);
        entityContexts.put(INodeSymlink.class, inodeContext);

        entityContexts.put(CorruptReplica.class, new CorruptReplicaContext(
            (CorruptReplicaDataAccess) getDataAccess(
                CorruptReplicaDataAccess.class)));
        entityContexts.put(UnderReplicatedBlock.class,
            new UnderReplicatedBlockContext(
                (UnderReplicatedBlockDataAccess) getDataAccess(
                    UnderReplicatedBlockDataAccess.class)));
        VariableContext variableContext = new VariableContext(
            (VariableDataAccess) getDataAccess(VariableDataAccess.class));
        entityContexts.put(Variable.class, variableContext);
        entityContexts.put(IntVariable.class, variableContext);
        entityContexts.put(LongVariable.class, variableContext);
        entityContexts.put(ByteArrayVariable.class, variableContext);
        entityContexts.put(StringVariable.class, variableContext);
        entityContexts.put(ArrayVariable.class, variableContext);
        entityContexts.put(HdfsLeDescriptor.class,
            new LeSnapshot.HdfsLESnapshot(
                (LeDescriptorDataAccess) getDataAccess(
                    HdfsLeDescriptorDataAccess.class)));
        entityContexts.put(DirectoryWithQuotaFeature.class, new DirectoryWithQuotaFeatureContext(
            (DirectoryWithQuotaFeatureDataAccess) getDataAccess(
                DirectoryWithQuotaFeatureDataAccess.class)));

        entityContexts.put(EncodingStatus.class, new EncodingStatusContext(
            (EncodingStatusDataAccess) getDataAccess(
                EncodingStatusDataAccess.class)));
        entityContexts.put(BlockChecksum.class, new BlockChecksumContext(
            (BlockChecksumDataAccess) getDataAccess(
                BlockChecksumDataAccess.class)));
        entityContexts.put(QuotaUpdate.class, new QuotaUpdateContext(
            (QuotaUpdateDataAccess) getDataAccess(
                QuotaUpdateDataAccess.class)));
        
        MetadataLogContext metadataLogContext = new MetadataLogContext(
            (MetadataLogDataAccess) getDataAccess(MetadataLogDataAccess.class));
            
        entityContexts.put(MetadataLogEntry.class, metadataLogContext);
        entityContexts.put(INodeMetadataLogEntry.class, metadataLogContext);
        entityContexts.put(XAttrMetadataLogEntry.class, metadataLogContext);
        
        entityContexts.put(SubTreeOperation.class, new SubTreeOperationsContext(
            (OngoingSubTreeOpsDataAccess) getDataAccess(OngoingSubTreeOpsDataAccess.class)));
        entityContexts.put(HashBucket.class, new HashBucketContext(
            (HashBucketDataAccess) getDataAccess(HashBucketDataAccess.class)));
        entityContexts.put(Ace.class, new AcesContext((AceDataAccess) getDataAccess(AceDataAccess.class)));

        entityContexts.put(RetryCacheEntry.class, new RetryCacheEntryContext(
            (RetryCacheEntryDataAccess) getDataAccess(RetryCacheEntryDataAccess.class)));
        
        entityContexts.put(CacheDirective.class, new CacheDirectiveContext(
            (CacheDirectiveDataAccess) getDataAccess(CacheDirectiveDataAccess.class)));
        entityContexts.put(CachePool.class, new CachePoolContext(
            (CachePoolDataAccess) getDataAccess(CachePoolDataAccess.class)));
        entityContexts.put(CachedBlock.class, new CachedBlockContext(
            (CachedBlockDataAccess) getDataAccess(CachedBlockDataAccess.class)));
        entityContexts.put(StoredXAttr.class,
            new XAttrContext((XAttrDataAccess)getDataAccess(XAttrDataAccess.class)));
        entityContexts.put(EncryptionZone.class,
            new EncryptionZoneContext((EncryptionZoneDataAccess)getDataAccess(EncryptionZoneDataAccess.class)));
        return entityContexts;
      }

      @Override
      public StorageConnector getConnector() {
        return dStorageFactory.getConnector();
      }
    };
  }

  public static EntityDataAccess getDataAccess(Class type) {
    if (dataAccessAdaptors.containsKey(type)) {
      return dataAccessAdaptors.get(type);
    }
    return dStorageFactory.getDataAccess(type);
  }
  
  public static boolean formatStorage() throws StorageException {
    Cache.getInstance().flush();
    UsersGroups.clearCache();
    return dStorageFactory.getConnector().formatStorage();
  }

  public static boolean formatHdfsStorage() throws StorageException {
    Cache.getInstance().flush();
    return dStorageFactory.getConnector().formatHDFSStorage();
  }

  public static boolean formatHdfsStorageNonTransactional() throws StorageException {
    Cache.getInstance().flush();
    return dStorageFactory.getConnector().formatHDFSStorageNonTransactional();
  }

  public static boolean formatAllStorageNonTransactional()
      throws StorageException {
    Cache.getInstance().flush();
    return dStorageFactory.getConnector().formatAllStorageNonTransactional();
  }

  public static boolean formatStorage(Class<? extends EntityDataAccess>... das)
      throws StorageException {
    Cache.getInstance().flush();
    return dStorageFactory.getConnector().formatStorage(das);
  }
  
  public static boolean hasResources(double threshold) throws StorageException {
    return dStorageFactory.hasResources(threshold);
  }

  public static float getResourceMemUtilization() throws StorageException {
    return dStorageFactory.getResourceMemUtilization();
  }
}
