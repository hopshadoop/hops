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
package io.hops.util;

import io.hops.*;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.common.entity.ArrayVariable;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.IntVariable;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.StringVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.election.dal.LeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.dal.UserGroupDataAccess;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import io.hops.transaction.TransactionCluster;
import io.hops.transaction.context.ContextInitializer;
import io.hops.transaction.context.EntityContext;
import io.hops.transaction.context.LeSnapshot;
import io.hops.transaction.context.VariableContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

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
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class RMStorageFactory {

  private static boolean isInitialized = false;
  private static DalStorageFactory dStorageFactory;

  private static DalNdbEventStreaming dNdbEventStreaming;
  private static boolean ndbStreaingRunning = false;

  public static MultiZoneStorageConnector getConnector() {
    return dStorageFactory.getMultiZoneConnector();
  }

  public static synchronized void kickTheNdbEventStreamingAPI(boolean isLeader,
          Configuration conf) throws
          StorageInitializtionException {
    dNdbEventStreaming = DalDriver.loadHopsNdbEventStreamingLib(
            YarnAPIStorageFactory.NDB_EVENT_STREAMING_FOR_DISTRIBUTED_SERVICE);

    // TODO[rob]: it should be ok to use the primary connection settings here.
    StorageConnector connector = dStorageFactory.getMultiZoneConnector().connectorFor(TransactionCluster.PRIMARY);
    
    String connectionString = connector.getClusterConnectString() + ":" +
            conf.getInt(YarnConfiguration.HOPS_NDB_EVENT_STREAMING_DB_PORT, 
                    YarnConfiguration.DEFAULT_HOPS_NDB_EVENT_STREAMING_DB_PORT);
    
    dNdbEventStreaming.init(conf.get(
            YarnConfiguration.EVENT_SHEDULER_CONFIG_PATH,
            YarnConfiguration.DEFAULT_EVENT_SHEDULER_CONFIG_PATH), conf.get(
                    YarnConfiguration.EVENT_RT_CONFIG_PATH,
                    YarnConfiguration.DEFAULT_EVENT_RT_CONFIG_PATH),
            connectionString, connector.getDatabaseName()
            );
    dNdbEventStreaming.startHopsNdbEvetAPISession(isLeader);
    ndbStreaingRunning = true;
  }
  
  public static synchronized void stopTheNdbEventStreamingAPI() {
    if(ndbStreaingRunning && dNdbEventStreaming!=null){
      ndbStreaingRunning = false;
      dNdbEventStreaming.closeHopsNdbEventAPISession();
    }
  }
  
  public static void setConfiguration(Configuration conf)
      throws StorageInitializtionException, IOException {
    if (isInitialized) {
      return;
    }
    addToClassPath(conf.get(YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE,
        YarnAPIStorageFactory.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT));
    dStorageFactory = DalDriver.load(
        conf.get(YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS,
            YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CLASS_DEFAULT));
    dStorageFactory.setConfiguration(getMetadataClusterConfiguration(conf));
    EntityManager.addContextInitializer(getContextInitializer());
    // connector going to the primary cluster
    // TODO[rob]: should be ok in YARN
    StorageConnector connector = dStorageFactory.getMultiZoneConnector().connectorFor(TransactionCluster.PRIMARY);
    if(conf.getBoolean(CommonConfigurationKeys.HOPS_GROUPS_ENABLE, CommonConfigurationKeys.HOPS_GROUPS_ENABLE_DEFAULT)) {
      UsersGroups.init(
          (UserDataAccess) getDataAccess(connector, UserDataAccess.class),
          (UserGroupDataAccess) getDataAccess(connector, UserGroupDataAccess.class),
          (GroupDataAccess) getDataAccess(connector, GroupDataAccess.class),
          conf.getInt(CommonConfigurationKeys.HOPS_GROUPS_UPDATER_ROUND, CommonConfigurationKeys.HOPS_GROUPS_UPDATER_ROUND_DEFAULT),
          conf.getInt(CommonConfigurationKeys.HOPS_USERS_LRU_THRESHOLD, CommonConfigurationKeys.HOPS_USERS_LRU_THRESHOLD_DEFAULT));
    }
    isInitialized = true;
  }

  public static Properties getMetadataClusterConfiguration(Configuration conf)
      throws IOException {
    String configFile =
        conf.get(YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CONFIG_FILE,
            YarnAPIStorageFactory.DFS_STORAGE_DRIVER_CONFIG_FILE_DEFAULT);
    Properties clusterConf = new Properties();
    InputStream inStream = StorageConnector.class.getClassLoader().
        getResourceAsStream(configFile);
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

  private static ContextInitializer getContextInitializer() {
    return new ContextInitializer() {
      @Override
      public Map<Class, EntityContext> createEntityContexts(StorageConnector connector) {
        Map<Class, EntityContext> entityContexts = new HashMap<>();
        VariableContext variableContext = new VariableContext((VariableDataAccess) getDataAccess(connector, VariableDataAccess.class));
        entityContexts.put(IntVariable.class, variableContext);
        entityContexts.put(LongVariable.class, variableContext);
        entityContexts.put(ByteArrayVariable.class, variableContext);
        entityContexts.put(StringVariable.class, variableContext);
        entityContexts.put(ArrayVariable.class, variableContext);
        entityContexts.put(Variable.class, variableContext);
        entityContexts.put(LeDescriptor.YarnLeDescriptor.class,
            new LeSnapshot.YarnLESnapshot((LeDescriptorDataAccess)
                getDataAccess(connector, YarnLeDescriptorDataAccess.class)));
        return entityContexts;
      }

      @Override
      public MultiZoneStorageConnector getMultiZoneConnector() {
        return dStorageFactory.getMultiZoneConnector();
      }
    };
  }

  public static EntityDataAccess getDataAccess(StorageConnector connector, Class type) {
    return dStorageFactory.getDataAccess(connector, type);
  }
}
