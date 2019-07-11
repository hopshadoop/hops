/*
 * Copyright 2016 Apache Software Foundation.
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

import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.*;
import io.hops.security.UsersGroups;
import io.hops.transaction.handler.AsyncLightWeightRequestHandler;
import java.io.IOException;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;

public class DBUtility {

  private static final Log LOG = LogFactory.getLog(DBUtility.class);

  public static void removeContainersToClean(final Set<ContainerId> containers,
          final org.apache.hadoop.yarn.api.records.NodeId nodeId) throws IOException {
    long start = System.currentTimeMillis();
    AsyncLightWeightRequestHandler removeContainerToClean
            = new AsyncLightWeightRequestHandler(
            YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ContainerIdToCleanDataAccess ctcDA
                = (ContainerIdToCleanDataAccess) RMStorageFactory
                .getDataAccess(ContainerIdToCleanDataAccess.class);
        List<io.hops.metadata.yarn.entity.ContainerId> containersToClean
                = new ArrayList<io.hops.metadata.yarn.entity.ContainerId>();
        for (ContainerId cid : containers) {
          containersToClean.add(new io.hops.metadata.yarn.entity.ContainerId(
                  nodeId.toString(), cid.toString()));
        }
        ctcDA.removeAll(containersToClean);
        connector.commit();
        return null;
      }
    };

    removeContainerToClean.handle();
    long duration = System.currentTimeMillis() - start;
    if(duration>10){
      LOG.error("too long " + duration);
    }
  }

  public static void removeContainersToSignal(final Set<SignalContainerRequest> containerRequests, final NodeId nodeId)
      throws IOException {
    long start = System.currentTimeMillis();
    AsyncLightWeightRequestHandler removeContainerToSignal
        = new AsyncLightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ContainerToSignalDataAccess ctsDA = (ContainerToSignalDataAccess) RMStorageFactory.getDataAccess(
            ContainerToSignalDataAccess.class);
        List<ContainerToSignal> containersToSignal = new ArrayList<ContainerToSignal>();
        for (SignalContainerRequest cr : containerRequests) {
          containersToSignal.add(new ContainerToSignal(nodeId.toString(), cr.getContainerId().toString(), cr.
              getCommand().toString()));
        }
        ctsDA.removeAll(containersToSignal);
        connector.commit();
        return null;
      }
    };

    removeContainerToSignal.handle();
    long duration = System.currentTimeMillis() - start;
    if (duration > 10) {
      LOG.error("too long " + duration);
    }
  }

  public static void addContainerToSignal(final SignalContainerRequest containerRequest, final NodeId nodeId)
      throws IOException {
    long start = System.currentTimeMillis();
    AsyncLightWeightRequestHandler addContainerToSignal = new AsyncLightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ContainerToSignalDataAccess ctsDA = (ContainerToSignalDataAccess) RMStorageFactory.getDataAccess(
            ContainerToSignalDataAccess.class);
        ContainerToSignal containerToSignal = new ContainerToSignal(nodeId.toString(),
            containerRequest.getContainerId().toString(), containerRequest.getCommand().toString());

        ctsDA.add(containerToSignal);
        connector.commit();
        return null;
      }
    };

    addContainerToSignal.handle();
    long duration = System.currentTimeMillis() - start;
    if (duration > 10) {
      LOG.error("too long " + duration);
    }
  }

  public static void removeRMNodeApplications(
      final List<ApplicationId> applications, final org.apache.hadoop.yarn.api.records.NodeId nodeId,
      final RMNodeApplication.RMNodeApplicationStatus status)
      throws IOException {
    long start = System.currentTimeMillis();
    AsyncLightWeightRequestHandler removeRMNodeApplication
        = new AsyncLightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        RMNodeApplicationsDataAccess faDA = (RMNodeApplicationsDataAccess) RMStorageFactory.getDataAccess(
            RMNodeApplicationsDataAccess.class);
        List<RMNodeApplication> rmNodeApps = new ArrayList<RMNodeApplication>();
        for (ApplicationId appId : applications) {
          rmNodeApps.add(new RMNodeApplication(nodeId.toString(), appId.toString(), status));
        }
        faDA.removeAll(rmNodeApps);
        connector.commit();

        return null;
      }
    };

    removeRMNodeApplication.handle();
    long duration = System.currentTimeMillis() - start;
    if (duration > 10) {
      LOG.error("too long " + duration);
    }
  }

  public static void removeRMNodeApplication(final ApplicationId appId,
      final org.apache.hadoop.yarn.api.records.NodeId nodeId, final RMNodeApplication.RMNodeApplicationStatus status)
      throws IOException {
    long start = System.currentTimeMillis();
    AsyncLightWeightRequestHandler removeRMNodeApplication
        = new AsyncLightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        RMNodeApplicationsDataAccess faDA = (RMNodeApplicationsDataAccess) RMStorageFactory.getDataAccess(
            RMNodeApplicationsDataAccess.class);
        RMNodeApplication rmNodeApp = new RMNodeApplication(nodeId.toString(), appId.toString(), status);

        faDA.remove(rmNodeApp);
        connector.commit();

        return null;
      }
    };

    removeRMNodeApplication.handle();
    long duration = System.currentTimeMillis() - start;
    if (duration > 10) {
      LOG.error("too long " + duration);
    }
  }
    
  public static void addRMNodeApplication(final ApplicationId appId,
      final org.apache.hadoop.yarn.api.records.NodeId nodeId, final RMNodeApplication.RMNodeApplicationStatus status)
      throws IOException {
    long start = System.currentTimeMillis();
    AsyncLightWeightRequestHandler addRMNodeApplication = new AsyncLightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        RMNodeApplicationsDataAccess faDA = (RMNodeApplicationsDataAccess) RMStorageFactory.getDataAccess(
            RMNodeApplicationsDataAccess.class);
        faDA.add(new RMNodeApplication(nodeId.toString(), appId.toString(), status));
        connector.commit();

        return null;
      }
    };

    addRMNodeApplication.handle();
    long duration = System.currentTimeMillis() - start;
    if (duration > 10) {
      LOG.error("too long " + duration);
    }
  }

  public static void addContainerToClean(final ContainerId containerId,
          final org.apache.hadoop.yarn.api.records.NodeId nodeId) throws
          IOException {
    long start = System.currentTimeMillis();
    AsyncLightWeightRequestHandler addContainerToClean
            = new AsyncLightWeightRequestHandler(
                    YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ContainerIdToCleanDataAccess ctcDA
                = (ContainerIdToCleanDataAccess) RMStorageFactory
                .getDataAccess(ContainerIdToCleanDataAccess.class);
        ctcDA.add(
                new io.hops.metadata.yarn.entity.ContainerId(nodeId.toString(),
                        containerId.toString()));
        connector.commit();
        return null;
      }
    };
    addContainerToClean.handle();
    long duration = System.currentTimeMillis() - start;
    if(duration>10){
      LOG.error("too long " + duration);
    }
  }
  
  public static void addNextHB(final boolean nextHB, final String nodeId) throws IOException {
           long start = System.currentTimeMillis();
    AsyncLightWeightRequestHandler addNextHB
            = new AsyncLightWeightRequestHandler(
                    YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        NextHeartbeatDataAccess nhbDA
                = (NextHeartbeatDataAccess) RMStorageFactory
                .getDataAccess(NextHeartbeatDataAccess.class);
        nhbDA.update(new NextHeartbeat(nodeId, nextHB));
        connector.commit();
        return null;
              }
            };
    addNextHB.handle();
    long duration = System.currentTimeMillis() - start;
    if(duration>10){
      LOG.error("too long " + duration);
    }
  }

  public static Map<String, Load> getAllLoads() throws IOException {
    LightWeightRequestHandler getLoadHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        RMLoadDataAccess DA = (RMLoadDataAccess) YarnAPIStorageFactory.
                getDataAccess(RMLoadDataAccess.class);
        Map<String, Load> res = DA.getAll();
        connector.commit();
        return res;
      }
    };
    return (Map<String, Load>) getLoadHandler.handle();
  }

  public static void updateLoad(final Load load) throws IOException {
    AsyncLightWeightRequestHandler updateLoadHandler = new AsyncLightWeightRequestHandler(
            YARNOperationType.OTHER) {

      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.readCommitted();
        RMLoadDataAccess DA = (RMLoadDataAccess) YarnAPIStorageFactory.
                getDataAccess(RMLoadDataAccess.class);
        DA.update(load);
        connector.commit();
        return null;
      }
    };
    updateLoadHandler.handle();
  }

  public static void removePendingEvent(String rmNodeId, PendingEvent.Type type,
          PendingEvent.Status status, int id, int contains) throws IOException {
long start = System.currentTimeMillis();
    final PendingEvent pendingEvent = new PendingEvent(rmNodeId, type, status,
            id, contains);

    AsyncLightWeightRequestHandler removePendingEvents
            = new AsyncLightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        PendingEventDataAccess pendingEventDAO
                = (PendingEventDataAccess) YarnAPIStorageFactory
                .getDataAccess(PendingEventDataAccess.class);
        pendingEventDAO.removePendingEvent(pendingEvent);
        connector.commit();

        return null;
      }
    };
    removePendingEvents.handle();
    long duration = System.currentTimeMillis() - start;
    if (duration > 10) {
      LOG.error("too long " + duration);
    }
  }

  public static boolean InitializeDB() throws IOException {
    LightWeightRequestHandler setRMDTMasterKeyHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        boolean success = connector.formatStorage();
        UsersGroups.createSyncRow();
        LOG.debug("HOP :: Format storage has been completed: " + success);
        return success;
      }
    };
    return (boolean) setRMDTMasterKeyHandler.handle();
  }

  public static byte[] verifySalt(final byte[] salt) throws IOException {

    AsyncLightWeightRequestHandler verifySalt
        = new AsyncLightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        VariableDataAccess<Variable, Variable.Finder> variableDAO = (VariableDataAccess) YarnAPIStorageFactory.
            getDataAccess(VariableDataAccess.class);
        Variable var = null;
        try {
          var = variableDAO.getVariable(Variable.Finder.Seed);
        } catch (StorageException ex) {
          LOG.warn(ex);
        }
        if (var == null) {
          var = new ByteArrayVariable(Variable.Finder.Seed, salt);
          variableDAO.setVariable(var);
        }

        connector.commit();

        return var.getBytes();
      }
    };
    byte[] s = (byte[]) verifySalt.handle();
    return s;
  }
}
