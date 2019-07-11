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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import com.google.common.annotations.VisibleForTesting;
import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.ConfDataAccess;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.hdfs.dal.ConfMutationDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.RMStorageFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

public class DBConfigurationStore extends YarnConfigurationStore {

  private long maxLogs;

  private LogMutation pendingMutation;

  @VisibleForTesting
  protected static final Version CURRENT_VERSION_INFO = Version
      .newInstance(0, 1);

  /**
   * Initialize the configuration store, with schedConf as the initial
   * scheduler configuration. If a persisted store already exists, use the
   * scheduler configuration stored there, and ignore schedConf.
   *
   * @param conf configuration to initialize store with
   * @param schedConf Initial key-value scheduler configuration to persist.
   * @param rmContext RMContext for this configuration store
   * @throws IOException if initialization fails
   */
  public void initialize(Configuration conf, Configuration schedConf,
      RMContext rmContext) throws Exception {
    this.maxLogs = conf.getLong(YarnConfiguration.RM_SCHEDCONF_MAX_LOGS,
        YarnConfiguration.DEFAULT_RM_SCHEDCONF_DB_MAX_LOGS);
    
    if (retrieve() == null) {
      HashMap<String, String> mapConf = new HashMap<>();
      for (Map.Entry<String, String> entry : schedConf) {
        mapConf.put(entry.getKey(), entry.getValue());
      }
      persistConf(mapConf);
    }
  }

  ;

  /**
   * Closes the configuration store, releasing any required resources.
   * @throws IOException on failure to close
   */
  public void close() throws IOException {
  }

  /**
   * Logs the configuration change to backing store.
   *
   * @param logMutation configuration change to be persisted in write ahead log
   * @throws IOException if logging fails
   */
  public void logMutation(final LogMutation logMutation) throws Exception {
    LightWeightRequestHandler logMutationHandler
        = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ConfMutationDataAccess DA
            = (ConfMutationDataAccess) RMStorageFactory
                .getDataAccess(ConfMutationDataAccess.class);
        List<byte[]> storedLogs = DA.get();
        LinkedList<LogMutation> logs = new LinkedList<>();
        if (storedLogs != null) {
          for (byte[] storedLog : storedLogs) {
            try {
              logs.add((LogMutation) deserializeObject(storedLog));
            } catch (Exception ex) {
              throw new StorageException(ex);
            }
          }
        }
        logs.add(logMutation);
        if (logs.size() > maxLogs) {
          logs.remove(logs.removeFirst());
        }
        storedLogs = new ArrayList<>();
        for (LogMutation log : logs) {
          try {
            storedLogs.add(serializeObject(log));
          } catch (Exception ex) {
            throw new StorageException(ex);
          }
        }
        DA.set(storedLogs);
        connector.commit();
        return null;
      }
    };
    logMutationHandler.handle();
    pendingMutation = logMutation;
  }

  @VisibleForTesting
  protected LinkedList<LogMutation> getLogs() throws Exception {
    LightWeightRequestHandler getLogHandler
        = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ConfMutationDataAccess DA
            = (ConfMutationDataAccess) RMStorageFactory
                .getDataAccess(ConfMutationDataAccess.class);
        List<byte[]> storedLogs = DA.get();
        LinkedList<LogMutation> logs = new LinkedList<>();
        if (storedLogs != null) {
          for (byte[] storedLog : storedLogs) {
            try {
              logs.add((LogMutation) deserializeObject(storedLog));
            } catch (Exception ex) {
              throw new StorageException(ex);
            }
          }
        }
        connector.commit();
        return logs;
      }
    };
    return (LinkedList<LogMutation>) getLogHandler.handle();
  }

  /**
   * Should be called after {@code logMutation}. Gets the pending mutation
   * last logged by {@code logMutation} and marks the mutation as persisted (no
   * longer pending). If isValid is true, merge the mutation with the persisted
   * configuration.
   *
   * @param isValid if true, update persisted configuration with pending
   * mutation.
   * @throws Exception if mutation confirmation fails
   */
  public void confirmMutation(boolean isValid) throws Exception {
    if (isValid) {
      Configuration storedConfigs = retrieve();
      Map<String, String> mapConf = new HashMap<>();
      for (Map.Entry<String, String> storedConf : storedConfigs) {
        mapConf.put(storedConf.getKey(), storedConf.getValue());
      }
      for (Map.Entry<String, String> confChange : pendingMutation.getUpdates().entrySet()) {
        if (confChange.getValue() == null || confChange.getValue().isEmpty()) {
          mapConf.remove(confChange.getKey());
        } else {
          mapConf.put(confChange.getKey(), confChange.getValue());
        }
      }
      persistConf(mapConf);
    }
    pendingMutation = null;
  }

  /**
   * Retrieve the persisted configuration.
   *
   * @return configuration as key-value
   */
  public Configuration retrieve() throws IOException {
    return getConf();
  }

  /**
   * Get a list of confirmed configuration mutations starting from a given id.
   *
   * @param fromId id from which to start getting mutations, inclusive
   * @return list of configuration mutations
   */
  public List<LogMutation> getConfirmedConfHistory(long fromId) {
    return null; // unimplemented
  }

  /**
   * Get schema version of persisted conf store, for detecting compatibility
   * issues when changing conf store schema.
   *
   * @return Schema version currently used by the persisted configuration store.
   * @throws Exception On version fetch failure
   */
  protected Version getConfStoreVersion() throws Exception {
    ByteArrayVariable var = (ByteArrayVariable) getVariable(Variable.Finder.ConfigurationStoreVersion);
    byte[] data = var == null ? null : (byte[]) var.getValue();
    if (data != null) {
      return new VersionPBImpl(YarnServerCommonProtos.VersionProto
          .parseFrom(data));
    }

    return null;
  }

  /**
   * Persist the hard-coded schema version to the conf store.
   *
   * @throws Exception On storage failure
   */
  protected void storeVersion() throws Exception {
    byte[] data = ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();

    setVariable(new ByteArrayVariable(Variable.Finder.ConfigurationStoreVersion,
        data));
  }

  /**
   * Get the hard-coded schema version, for comparison against the schema
   * version currently persisted.
   *
   * @return Current hard-coded schema version
   */
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  private void setVariable(final Variable var) throws IOException {
    LightWeightRequestHandler setVersionHandler = new LightWeightRequestHandler(
        YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        VariableDataAccess vDA = (VariableDataAccess) RMStorageFactory
            .getDataAccess(VariableDataAccess.class);
        vDA.setVariable(var);
        connector.commit();
        return null;
      }
    };
    setVersionHandler.handle();
  }

  private Variable getVariable(final Variable.Finder finder) throws IOException {
    LightWeightRequestHandler getVersionHandler = new LightWeightRequestHandler(
        YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.readCommitted();
        VariableDataAccess DA
            = (VariableDataAccess) RMStorageFactory
                .getDataAccess(VariableDataAccess.class);
        Variable var = (Variable) DA.getVariable(finder);

        connector.commit();
        return var;
      }
    };
    return (Variable) getVersionHandler.handle();
  }

  private static byte[] serializeObject(Object o) throws Exception {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);) {
      oos.writeObject(o);
      oos.flush();
      baos.flush();
      return baos.toByteArray();
    }
  }

  private static Object deserializeObject(byte[] bytes) throws Exception {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);) {
      return ois.readObject();
    }
  }
  
  private void persistConf(Map<String, String> mapConf) throws Exception{
    final byte[] confBytes = serializeObject(mapConf);
    LightWeightRequestHandler confHandler = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ConfDataAccess DA = (ConfDataAccess) RMStorageFactory.getDataAccess(ConfDataAccess.class);
        
        DA.set(confBytes);
        connector.commit();
        return null;
      }
    };
    confHandler.handle();
  }
  
  private Configuration getConf() throws IOException {
    LightWeightRequestHandler confHandler = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ConfDataAccess DA = (ConfDataAccess) RMStorageFactory.getDataAccess(ConfDataAccess.class);
        
        byte[] result = (byte[]) DA.get();
        connector.commit();
        return result;
      }
    };
    byte[] confBytes = (byte[]) confHandler.handle();
    
    try {
      Map<String, String> map = (HashMap<String, String>) deserializeObject(confBytes);
      Configuration c = new Configuration();
      for (Map.Entry<String, String> e : map.entrySet()) {
        c.set(e.getKey(), e.getValue());
      }
      return c;
    } catch (Exception e) {
      LOG.error("Exception while deserializing scheduler configuration " + "from store", e);
    }
    return null;
  }
}
