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

import io.hops.DalDriver;
import io.hops.DalStorageFactory;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.leaderElection.VarsRegister;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.common.entity.ArrayVariable;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.IntVariable;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.StringVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.LeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.ContextInitializer;
import io.hops.transaction.context.EntityContext;
import io.hops.transaction.context.LeSnapshot;
import io.hops.transaction.context.VariableContext;

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

public class LEStorageFactory {

  private static boolean isDALInitialized = false;
  private static DalStorageFactory dStorageFactory;
  private static Map<Class, EntityDataAccess> dataAccessAdaptors =
      new HashMap<Class, EntityDataAccess>();
  
  public static StorageConnector getConnector() {
    return dStorageFactory.getConnector();
  }

  public static void setConfiguration(final String driver,
      final String driverClass, final String driverConfigFile)
      throws IOException {
    if (!isDALInitialized) {
      VarsRegister.registerHdfsDefaultValues();
      addToClassPath(driver);
      dStorageFactory = DalDriver.load(driverClass);
      dStorageFactory
          .setConfiguration(getMetadataClusterConfiguration(driverConfigFile));
      initDataAccessWrappers();
      EntityManager.addContextInitializer(getContextInitializer());
      isDALInitialized = true;
    }
  }

  public static Properties getMetadataClusterConfiguration(final String driver)
      throws IOException {
    Properties clusterConf = new Properties();
    InputStream inStream =
        StorageConnector.class.getClassLoader().getResourceAsStream(driver);
    clusterConf.load(inStream);
    return clusterConf;
  }

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
        Map<Class, EntityContext> entityContexts =
            new HashMap<Class, EntityContext>();
        VariableContext variableContext = new VariableContext(
            (VariableDataAccess) getDataAccess(VariableDataAccess.class));
        entityContexts.put(Variable.class, variableContext);
        entityContexts.put(IntVariable.class, variableContext);
        entityContexts.put(LongVariable.class, variableContext);
        entityContexts.put(ByteArrayVariable.class, variableContext);
        entityContexts.put(StringVariable.class, variableContext);
        entityContexts.put(ArrayVariable.class, variableContext);
        entityContexts.put(LeDescriptor.HdfsLeDescriptor.class,
            new LeSnapshot.HdfsLESnapshot(
                (LeDescriptorDataAccess) getDataAccess(
                    HdfsLeDescriptorDataAccess.class)));
        entityContexts.put(LeDescriptor.YarnLeDescriptor.class,
            new LeSnapshot.YarnLESnapshot(
                (LeDescriptorDataAccess) getDataAccess(
                    YarnLeDescriptorDataAccess.class)));
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
    return dStorageFactory.getConnector().formatStorage();
  }
  
  public static boolean formatAllStorageNonTransactional()
      throws StorageException {
    return dStorageFactory.getConnector().formatAllStorageNonTransactional();
  }

  public static boolean formatStorage(Class<? extends EntityDataAccess>... das)
      throws StorageException {
    return dStorageFactory.getConnector().formatStorage(das);
  }
}
