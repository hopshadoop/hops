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

import io.hops.DalDriver;
import io.hops.DalStorageFactory;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.leaderElection.VarsRegister;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.dal.UserGroupDataAccess;
import io.hops.security.UsersGroups;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.ContextInitializer;
import io.hops.transaction.context.EntityContext;
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

public class YarnAPIStorageFactory {

  private static boolean isInitialized = false;
  private static DalStorageFactory dStorageFactory;
  private static Map<Class, EntityDataAccess> dataAccessAdaptors =
      new HashMap<Class, EntityDataAccess>();
  public static final String DFS_STORAGE_DRIVER_JAR_FILE =
      "dfs.storage.driver.jarFile";
  public static final String DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT = "";
  public static final String DFS_STORAGE_DRIVER_CLASS =
      "dfs.storage.driver.class";
  public static final String DFS_STORAGE_DRIVER_CLASS_DEFAULT =
      "io.hops.metadata.ndb.NdbStorageFactory";
  public static final String DFS_STORAGE_DRIVER_CONFIG_FILE =
      "dfs.storage.driver.configfile";
  public static final String DFS_STORAGE_DRIVER_CONFIG_FILE_DEFAULT =
      "ndb-config.properties";
  public static final String EVENT_STREAMING_FOR_DISTRIBUTED_SERVICE
          = "io.hops.metadata.ndb.JniNdbEventStreaming";

  public static StorageConnector getConnector() {
    return dStorageFactory.getConnector();
  }

  public static void setConfiguration(Configuration conf)
          throws StorageInitializtionException, IOException {
    if (isInitialized) {
      return;
    }
    addToClassPath(conf.get(DFS_STORAGE_DRIVER_JAR_FILE,
            DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT));
    dStorageFactory = DalDriver.load(
            conf.get(DFS_STORAGE_DRIVER_CLASS, DFS_STORAGE_DRIVER_CLASS_DEFAULT));
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
    VarsRegister.registerYarnDefaultValues();
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

  private static void initDataAccessWrappers() {
    dataAccessAdaptors.clear();
  }

  private static ContextInitializer getContextInitializer() {
    return new ContextInitializer() {
      @Override
      public Map<Class, EntityContext> createEntityContexts() {
        Map<Class, EntityContext> entityContexts = new HashMap<Class, EntityContext>();
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
  
  public static boolean formatYarnStorageNonTransactional()
      throws StorageException {
    return dStorageFactory.getConnector().formatYarnStorageNonTransactional();
  }
  
  public static boolean formatYarnStorage() throws StorageException {
    return dStorageFactory.getConnector().formatYarnStorage();
  }
}
