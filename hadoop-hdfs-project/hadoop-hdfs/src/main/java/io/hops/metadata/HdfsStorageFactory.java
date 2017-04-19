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
import io.hops.MultiZoneStorageConnector;
import io.hops.StorageConnector;
import io.hops.common.IDsMonitor;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.log.NDCWrapper;
import io.hops.metadata.adaptor.*;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.common.entity.*;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.LeDescriptorDataAccess;
import io.hops.metadata.election.entity.LeDescriptor.HdfsLeDescriptor;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.hdfs.entity.*;
import io.hops.resolvingcache.Cache;
import io.hops.security.Users;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import io.hops.transaction.TransactionCluster;
import io.hops.transaction.context.*;
import io.hops.transaction.lock.LockFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.Lease;

import java.io.File;
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

public class HdfsStorageFactory {
  private interface WrapperDataAccessBuilder {
    EntityDataAccess build(StorageConnector connector, DalStorageFactory factory);
  }

  private static boolean isDALInitialized = false;
  private static DalStorageFactory dStorageFactory;
  private static Map<Class, WrapperDataAccessBuilder> dataAccessAdaptors = new HashMap<>();
  
  public static MultiZoneStorageConnector getConnector() {
    return dStorageFactory.getMultiZoneConnector();
  }

  @VisibleForTesting
  public static void resetDALInitialized() {
    isDALInitialized = false;
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
      if(conf.getBoolean(
          CommonConfigurationKeys.HOPS_GROUPS_ENABLE,
          CommonConfigurationKeys.HOPS_GROUPS_ENABLE_DEFAULT)) {
        StorageConnector connector = dStorageFactory.getMultiZoneConnector().connectorFor(TransactionCluster.PRIMARY);
        UsersGroups.init(
            (UserDataAccess) getDataAccess(connector, UserDataAccess.class),
            (UserGroupDataAccess) getDataAccess(connector, UserGroupDataAccess.class),
            (GroupDataAccess) getDataAccess(connector, GroupDataAccess.class),
            conf.getInt(
                CommonConfigurationKeys.HOPS_GROUPS_UPDATER_ROUND,
                CommonConfigurationKeys.HOPS_GROUPS_UPDATER_ROUND_DEFAULT),
            conf.getInt(
                CommonConfigurationKeys.HOPS_USERS_LRU_THRESHOLD,
                CommonConfigurationKeys .HOPS_USERS_LRU_THRESHOLD_DEFAULT)
        );
      }
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
    } catch (MalformedURLException ex) {
      throw new StorageInitializtionException(ex);
    } catch (IllegalAccessException ex) {
      throw new StorageInitializtionException(ex);
    } catch (IllegalArgumentException ex) {
      throw new StorageInitializtionException(ex);
    } catch (InvocationTargetException ex) {
      throw new StorageInitializtionException(ex);
    } catch (NoSuchMethodException ex) {
      throw new StorageInitializtionException(ex);
    } catch (SecurityException ex) {
      throw new StorageInitializtionException(ex);
    }
  }
  
  private static void initDataAccessWrappers() {
    dataAccessAdaptors.clear();
    dataAccessAdaptors.put(BlockInfoDataAccess.class, new WrapperDataAccessBuilder() {
      @Override
      public EntityDataAccess build(StorageConnector connector, DalStorageFactory factory) {
        return new BlockInfoDALAdaptor(
            (BlockInfoDataAccess) factory.getDataAccess(connector, BlockInfoDataAccess.class));
      }
    });
    dataAccessAdaptors.put(ReplicaUnderConstructionDataAccess.class, new WrapperDataAccessBuilder() {
      @Override
      public EntityDataAccess build(StorageConnector connector, DalStorageFactory factory) {
        return new ReplicaUnderConstructionDALAdaptor(
            (ReplicaUnderConstructionDataAccess) factory.getDataAccess(connector, ReplicaUnderConstructionDataAccess.class));
      }
    });
    dataAccessAdaptors.put(LeaseDataAccess.class, new WrapperDataAccessBuilder() {
      @Override
      public EntityDataAccess build(StorageConnector connector, DalStorageFactory factory) {
        return new LeaseDALAdaptor(
            (LeaseDataAccess) factory.getDataAccess(connector, LeaseDataAccess.class));
      }
    });
    dataAccessAdaptors.put(PendingBlockDataAccess.class, new WrapperDataAccessBuilder() {
      @Override
      public EntityDataAccess build(StorageConnector connector, DalStorageFactory factory) {
        return new PendingBlockInfoDALAdaptor(
            (PendingBlockDataAccess) factory.getDataAccess(connector, PendingBlockDataAccess.class));
      }
    });
    dataAccessAdaptors.put(INodeDataAccess.class, new WrapperDataAccessBuilder() {
      @Override
      public EntityDataAccess build(StorageConnector connector, DalStorageFactory factory) {
        return new INodeDALAdaptor(
            (INodeDataAccess) factory.getDataAccess(connector, INodeDataAccess.class));
      }
    });

    dataAccessAdaptors.put(INodeAttributesDataAccess.class, new WrapperDataAccessBuilder() {
      @Override
      public EntityDataAccess build(StorageConnector connector, DalStorageFactory factory) {
        return new INodeAttributeDALAdaptor(
            (INodeAttributesDataAccess) factory.getDataAccess(connector, INodeAttributesDataAccess.class));
      }
    });
  }

  private static ContextInitializer getContextInitializer() {
    return new ContextInitializer() {
      @Override
      public Map<Class, EntityContext> createEntityContexts(StorageConnector connector) {
        Map<Class, EntityContext> entityContexts = new HashMap<>();

        BlockInfoContext bic = new BlockInfoContext(
            (BlockInfoDataAccess) getDataAccess(connector, BlockInfoDataAccess.class));
        entityContexts.put(BlockInfo.class, bic);
        entityContexts.put(BlockInfoUnderConstruction.class, bic);

        entityContexts.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionContext(
            (ReplicaUnderConstructionDataAccess) getDataAccess(connector, ReplicaUnderConstructionDataAccess.class)));

        entityContexts.put(Replica.class, new ReplicaContext(
            (ReplicaDataAccess) getDataAccess(connector, ReplicaDataAccess.class)));

        entityContexts.put(ExcessReplica.class,
            new ExcessReplicaContext((ExcessReplicaDataAccess) getDataAccess(connector, ExcessReplicaDataAccess.class)));

        entityContexts.put(InvalidatedBlock.class,
            new InvalidatedBlockContext((InvalidateBlockDataAccess) getDataAccess(connector, InvalidateBlockDataAccess.class)));

        entityContexts.put(Lease.class,
            new LeaseContext((LeaseDataAccess) getDataAccess(connector, LeaseDataAccess.class)));

        entityContexts.put(LeasePath.class, new LeasePathContext(
            (LeasePathDataAccess) getDataAccess(connector, LeasePathDataAccess.class)));

        entityContexts.put(PendingBlockInfo.class, new PendingBlockContext(
            (PendingBlockDataAccess) getDataAccess(connector, PendingBlockDataAccess.class)));

        INodeContext inodeContext = new INodeContext(
            (INodeDataAccess) getDataAccess(connector, INodeDataAccess.class));
        entityContexts.put(INode.class, inodeContext);
        entityContexts.put(INodeDirectory.class, inodeContext);
        entityContexts.put(INodeFile.class, inodeContext);
        entityContexts.put(INodeDirectoryWithQuota.class, inodeContext);
        entityContexts.put(INodeSymlink.class, inodeContext);
        entityContexts.put(INodeFileUnderConstruction.class, inodeContext);

        entityContexts.put(CorruptReplica.class, new CorruptReplicaContext(
            (CorruptReplicaDataAccess) getDataAccess(connector, CorruptReplicaDataAccess.class)));

        entityContexts.put(UnderReplicatedBlock.class, new UnderReplicatedBlockContext(
                (UnderReplicatedBlockDataAccess) getDataAccess(connector, UnderReplicatedBlockDataAccess.class)));

        VariableContext variableContext = new VariableContext(
            (VariableDataAccess) getDataAccess(connector, VariableDataAccess.class));
        entityContexts.put(Variable.class, variableContext);
        entityContexts.put(IntVariable.class, variableContext);
        entityContexts.put(LongVariable.class, variableContext);
        entityContexts.put(ByteArrayVariable.class, variableContext);
        entityContexts.put(StringVariable.class, variableContext);
        entityContexts.put(ArrayVariable.class, variableContext);

        entityContexts.put(HdfsLeDescriptor.class, new LeSnapshot.HdfsLESnapshot(
            (LeDescriptorDataAccess) getDataAccess(connector, HdfsLeDescriptorDataAccess.class)));

        entityContexts.put(INodeAttributes.class, new INodeAttributesContext(
            (INodeAttributesDataAccess) getDataAccess(connector, INodeAttributesDataAccess.class)));

        entityContexts.put(EncodingStatus.class, new EncodingStatusContext(
            (EncodingStatusDataAccess) getDataAccess(connector, EncodingStatusDataAccess.class)));

        entityContexts.put(BlockChecksum.class, new BlockChecksumContext(
            (BlockChecksumDataAccess) getDataAccess(connector, BlockChecksumDataAccess.class)));

        entityContexts.put(QuotaUpdate.class, new QuotaUpdateContext(
            (QuotaUpdateDataAccess) getDataAccess(connector, QuotaUpdateDataAccess.class)));

        entityContexts.put(MetadataLogEntry.class, new MetadataLogContext(
            (MetadataLogDataAccess) getDataAccess(connector, MetadataLogDataAccess.class)));

        entityContexts.put(SubTreeOperation.class, new SubTreeOperationsContext(
            (OngoingSubTreeOpsDataAccess) getDataAccess(connector, OngoingSubTreeOpsDataAccess.class)));

        return entityContexts;
      }

      @Override
      public MultiZoneStorageConnector getMultiZoneConnector() {
        return HdfsStorageFactory.getConnector();
      }
    };
  }

  public static EntityDataAccess getDataAccess(StorageConnector connector, Class type) {
    if (dataAccessAdaptors.containsKey(type)) {
      return dataAccessAdaptors.get(type).build(connector, dStorageFactory);
    }
    return dStorageFactory.getDataAccess(connector, type);
  }
  
  public static boolean formatStorage(StorageConnector connector) throws StorageException {
    Cache.getInstance().flush();
    Users.flushCache();
    return connector.formatStorage();
  }

  public static boolean formatHdfsStorage(StorageConnector connector) throws StorageException {
    Cache.getInstance().flush();
    return connector.formatHDFSStorage();
  }

  public static boolean formatHdfsStorageNonTransactional(StorageConnector connector) throws StorageException {
    Cache.getInstance().flush();
    return connector.formatHDFSStorageNonTransactional();
  }

  public static boolean formatAllStorageNonTransactional(StorageConnector connector)
      throws StorageException {
    Cache.getInstance().flush();
    return connector.formatAllStorageNonTransactional();
  }

  public static boolean formatStorage(StorageConnector connector, Class<? extends EntityDataAccess>... das)
      throws StorageException {
    Cache.getInstance().flush();
    return connector.formatStorage(das);
  }
}
