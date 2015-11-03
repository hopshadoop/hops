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
package io.hops.metadata.util;

import com.google.protobuf.InvalidProtocolBufferException;
import io.hops.exception.StorageException;
import io.hops.ha.common.TransactionStateImpl;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.FullRMNodeDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.AppSchedulingInfo;
import io.hops.metadata.yarn.entity.AppSchedulingInfoBlacklist;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLiveContainers;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppNewlyAllocatedContainers;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.LaunchedContainers;
import io.hops.metadata.yarn.entity.Load;
import io.hops.metadata.yarn.entity.Node;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.QueueMetrics;
import io.hops.metadata.yarn.entity.RMContainer;
import io.hops.metadata.yarn.entity.RMContextActiveNodes;
import io.hops.metadata.yarn.entity.RMContextInactiveNodes;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.ResourceRequest;
import io.hops.metadata.yarn.entity.SchedulerApplication;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import io.hops.metadata.yarn.entity.YarnVariables;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.metadata.yarn.entity.rmstatestore.DelegationKey;
import io.hops.metadata.yarn.entity.rmstatestore.DelegationToken;
import io.hops.metadata.yarn.entity.rmstatestore.RMStateVersion;
import io.hops.metadata.yarn.entity.rmstatestore.SecretMamagerKey;
import io.hops.metadata.yarn.entity.rmstatestore.SequenceNumber;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NDBRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RMUtilities {

  private static final Log LOG = LogFactory.getLog(RMUtilities.class);
  public static final String CONTAINERS_TO_CLEAN = "containersToClean";
  public static final String FINISHED_APPLICATIONS = "finishedApplications";

  /**
   * Set version table info at NDB.
   *
   * @param version
   */
  public static void setRMStateVersionLightweight(final byte[] version)
      throws IOException {
    LightWeightRequestHandler setVersionHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            RMStateVersionDataAccess vDA =
                (RMStateVersionDataAccess) RMStorageFactory
                    .getDataAccess(RMStateVersionDataAccess.class);
            RMStateVersion newVersion = new RMStateVersion(0, version);
            vDA.add(newVersion);
            connector.commit();
            return null;
          }
        };
    setVersionHandler.handle();
  }


  /**
   * Get version table info at NDB.
   *
   * @param id
   * @return
   */
  public static byte[] getRMStateVersionBinaryLightweight(final int id)
      throws IOException {
    LightWeightRequestHandler getVersionHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.readCommitted();
            RMStateVersionDataAccess DA =
                (RMStateVersionDataAccess) RMStorageFactory
                    .getDataAccess(RMStateVersionDataAccess.class);
            RMStateVersion hopRMStateVersion = (RMStateVersion) DA.findById(id);
            byte[] version = null;
            if (hopRMStateVersion != null) {
              version = hopRMStateVersion.getVersion();
            }
            connector.commit();
            return version;
          }
        };
    return (byte[]) getVersionHandler.handle();
  }

  /**
   * Retrieve all ApplicationState rows from NDB.
   *
   * @return
   * @throws IOException
   */
  public static List<ApplicationState> getApplicationStates()
      throws IOException {
    LightWeightRequestHandler getApplicationStateHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ApplicationStateDataAccess DA =
                (ApplicationStateDataAccess) RMStorageFactory
                    .getDataAccess(ApplicationStateDataAccess.class);
            List<ApplicationState> appStates = DA.getAll();
            connector.commit();
            return appStates;
          }
        };
    return (List<ApplicationState>) getApplicationStateHandler.handle();
  }

  /**
   * Retrieve all ApplicationState rows from NDB.
   *
   * @param applicationId
   * @return
   * @throws IOException
   */
  public static ApplicationState getApplicationState(final String applicationId)
      throws IOException {
    LightWeightRequestHandler getApplicationStateHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ApplicationStateDataAccess DA =
                (ApplicationStateDataAccess) RMStorageFactory
                    .getDataAccess(ApplicationStateDataAccess.class);
            ApplicationState appState =
                (ApplicationState) DA.findByApplicationId(applicationId);
            connector.commit();
            return appState;
          }
        };
    return (ApplicationState) getApplicationStateHandler.handle();
  }

  /**
   * Retrieve applications (RMApp, RMAppAttempt) from NDB. MUST be used only
   * by ResourceTrackerService as some fields of the objects are not set.
   *
   * @param rmContext
   * @param conf
   * @param applicationId
   * @return
   * @throws java.io.IOException
   */
  public static RMApp getRMApp(RMContext rmContext, Configuration conf,
      String applicationId) throws IOException {
    //Retrieve all applicationIds from NDB
    ApplicationState hopAppState =
        RMUtilities.getApplicationState(applicationId);
    
    if (hopAppState != null) {
      //Create ApplicationState for every application
      
      ApplicationId appId = ConverterUtils.toApplicationId(hopAppState.
          getApplicationid());
      ApplicationStateDataPBImpl appStateData =
          new ApplicationStateDataPBImpl(ApplicationStateDataProto.
              parseFrom(hopAppState.getAppstate()));
      RMStateStore.ApplicationState appState =
          new RMStateStore.ApplicationState(appStateData.
              getSubmitTime(), appStateData.getStartTime(),
              appStateData.getApplicationSubmissionContext(),
              appStateData.getUser(), appStateData.getState(),
              appStateData.getDiagnostics(), appStateData.getFinishTime(),
              appStateData.getStateBeforeKilling(),
              appStateData.getUpdatedNodesId());
      LOG.debug("loadRMAppState for app " + appState.getAppId() + " state " +
          appState.getState());

      //Create RMApp
      //Null fields are not required by ResourceTrackerService
      RMAppImpl application = new RMAppImpl(appId, rmContext, conf,
          appState.getApplicationSubmissionContext().getApplicationName(),
          appState.getUser(),
          appState.getApplicationSubmissionContext().getQueue(),
          appState.getApplicationSubmissionContext(), null, null,
          appState.getSubmitTime(), appState.
          getApplicationSubmissionContext().getApplicationType(),
          appState.getApplicationSubmissionContext().getApplicationTags(),
          null);
      ApplicationAttemptId appAttemptId = ApplicationAttemptId
          .newInstance(appId, appState.getAttemptCount() + 1);
      ApplicationAttemptState hopAppAttemptState = RMUtilities
          .getApplicationAttemptState(applicationId, appAttemptId.toString());
      if (hopAppAttemptState != null) {
        ApplicationAttemptStateDataPBImpl attemptStateData =
            new ApplicationAttemptStateDataPBImpl(
                YarnServerResourceManagerServiceProtos.ApplicationAttemptStateDataProto
                    .
                        parseFrom(
                            hopAppAttemptState.getApplicationattemptstate()));

        RMAppAttempt attempt =
            new RMAppAttemptImpl(appAttemptId, rmContext, null, null,
                appState.getApplicationSubmissionContext(), conf,
                application.getMaxAppAttempts() ==
                    application.getAppAttempts().size());

        ((RMAppAttemptImpl) attempt)
            .setMasterContainer(attemptStateData.getMasterContainer());

        application.addRMAppAttempt(appAttemptId, attempt);
        return application;
      }
    }
    return null;
  }

  public static Map<RMStateStore.KeyType, MasterKey> getSecretMamagerKeys()
      throws IOException {
    LightWeightRequestHandler getNMTokenSecretMamagerCurrentKeyHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask()
              throws StorageException, InvalidProtocolBufferException {
            connector.beginTransaction();
            connector.writeLock();
            SecretMamagerKeysDataAccess DA =
                (SecretMamagerKeysDataAccess) RMStorageFactory
                    .getDataAccess(SecretMamagerKeysDataAccess.class);
            List<SecretMamagerKey> hopKeys =
                (List<SecretMamagerKey>) DA.getAll();
            connector.commit();
            Map<RMStateStore.KeyType, MasterKey> keys =
                new EnumMap<RMStateStore.KeyType, MasterKey>(
                    RMStateStore.KeyType.class);
            MasterKey key;
            if (hopKeys != null) {
              for (SecretMamagerKey hopKey : hopKeys) {
                key = new MasterKeyPBImpl(YarnServerCommonProtos.MasterKeyProto
                    .parseFrom(hopKey.getKey()));
                keys.put(RMStateStore.KeyType.valueOf(hopKey.getKeyType()),
                    key);
              }
            }
            return keys;
          }
        };
    return (Map<RMStateStore.KeyType, MasterKey>) getNMTokenSecretMamagerCurrentKeyHandler
        .handle();
  }
  
  public static Map<ApplicationAttemptId, org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse> getAllocateResponses()
      throws IOException {
    LightWeightRequestHandler allocateResponsesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            AllocateResponseDataAccess da =
                (AllocateResponseDataAccess) RMStorageFactory
                    .getDataAccess(AllocateResponseDataAccess.class);
            List<AllocateResponse> hopAllocateResponses =
                (List<AllocateResponse>) da.getAll();
            connector.commit();
            Map<ApplicationAttemptId, org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse>
                allocateResponses =
                new HashMap<ApplicationAttemptId, org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse>();
            if (hopAllocateResponses != null) {
              for (AllocateResponse hopAllocateResponse : hopAllocateResponses) {
                org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse
                    allocateResponse = new AllocateResponsePBImpl(
                    YarnServiceProtos.AllocateResponseProto
                        .parseFrom(hopAllocateResponse.getAllocateResponse()));
                allocateResponses.put(ConverterUtils.toApplicationAttemptId(
                    hopAllocateResponse.getApplicationattemptid()),
                    allocateResponse);
              }
            }
            return allocateResponses;
          }
        };
    return (Map<ApplicationAttemptId, org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse>) allocateResponsesHandler
        .handle();
  }

  /**
   * Retrieve all RPC rows from database.
   *
   * @return
   * @throws IOException
   */
  public static List<RPC> getAppMasterRPCs() throws IOException {
    LightWeightRequestHandler getAppMasterRPCHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            RPCDataAccess DA = (RPCDataAccess) RMStorageFactory
                .getDataAccess(RPCDataAccess.class);
            List<RPC> appMasterRPCs = DA.getAll();
            connector.commit();
            return appMasterRPCs;
          }
        };
    return (List<RPC>) getAppMasterRPCHandler.handle();
  }

  public static List<AppSchedulingInfo> getAppSchedulingInfos()
      throws IOException {
    LightWeightRequestHandler handler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            //        connector.readCommitted();
            connector.beginTransaction();
            connector.writeLock();
            AppSchedulingInfoDataAccess dA =
                (AppSchedulingInfoDataAccess) RMStorageFactory
                    .getDataAccess(AppSchedulingInfoDataAccess.class);
            List<AppSchedulingInfo> result = dA.findAll();
            connector.commit();
            return result;
          }
        };
    return (List<AppSchedulingInfo>) handler.handle();
  }
  
  public static Map<String, SchedulerApplication> getSchedulerApplications()
      throws IOException {
    LightWeightRequestHandler getSchedulerApplicationHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            SchedulerApplicationDataAccess DA =
                (SchedulerApplicationDataAccess) RMStorageFactory
                    .getDataAccess(SchedulerApplicationDataAccess.class);
            Map<String, SchedulerApplication> schedulerApplications =
                DA.getAll();
            connector.commit();
            return schedulerApplications;
          }
        };
    return (Map<String, SchedulerApplication>) getSchedulerApplicationHandler
        .handle();
  }

  public static List<FiCaSchedulerNode> getAllFiCaSchedulerNodes()
      throws IOException {
    LightWeightRequestHandler getFiCaSchedulerNodesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            FiCaSchedulerNodeDataAccess DA =
                (FiCaSchedulerNodeDataAccess) RMStorageFactory
                    .getDataAccess(FiCaSchedulerNodeDataAccess.class);
            List<FiCaSchedulerNode> fiCaSchedulerNodes = DA.getAll();
            connector.commit();
            return fiCaSchedulerNodes;
          }
        };
    return (List<FiCaSchedulerNode>) getFiCaSchedulerNodesHandler.handle();
  }

  
  public static Map<String, List<LaunchedContainers>> getAllLaunchedContainers()
      throws IOException {
    LightWeightRequestHandler getLaunchedContainersHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            LaunchedContainersDataAccess DA =
                (LaunchedContainersDataAccess) RMStorageFactory
                    .getDataAccess(LaunchedContainersDataAccess.class);
            Map<String, List<LaunchedContainers>> hopLaunchedContainers =
                DA.getAll();
            connector.commit();
            return hopLaunchedContainers;
          }
        };
    return (Map<String, List<LaunchedContainers>>) getLaunchedContainersHandler.
        handle();
  }

  //For testing TODO move to test
  public static List<FiCaSchedulerAppNewlyAllocatedContainers> getNewlyAllocatedContainers(
      final String ficaId) throws IOException {
    LightWeightRequestHandler getNewlyAllocatedContainersHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            FiCaSchedulerAppNewlyAllocatedContainersDataAccess DA =
                (FiCaSchedulerAppNewlyAllocatedContainersDataAccess) RMStorageFactory
                    .getDataAccess(
                        FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class);
            List<FiCaSchedulerAppNewlyAllocatedContainers>
                hopNewlyAllocatedContainers = DA.findById(ficaId);
            connector.commit();
            return hopNewlyAllocatedContainers;
          }
        };
    return (List<FiCaSchedulerAppNewlyAllocatedContainers>) getNewlyAllocatedContainersHandler
        .handle();
  }

  public static Map<String, List<FiCaSchedulerAppNewlyAllocatedContainers>> getAllNewlyAllocatedContainers()
      throws IOException {
    LightWeightRequestHandler getNewlyAllocatedContainersHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            FiCaSchedulerAppNewlyAllocatedContainersDataAccess DA =
                (FiCaSchedulerAppNewlyAllocatedContainersDataAccess) RMStorageFactory
                    .
                        getDataAccess(
                            FiCaSchedulerAppNewlyAllocatedContainersDataAccess.class);
            Map<String, List<FiCaSchedulerAppNewlyAllocatedContainers>>
                hopNewlyAllocatedContainers = DA.getAll();
            connector.commit();
            return hopNewlyAllocatedContainers;
          }
        };
    return (Map<String, List<FiCaSchedulerAppNewlyAllocatedContainers>>) getNewlyAllocatedContainersHandler
        .
            handle();
  }
  

  public static Map<String, List<FiCaSchedulerAppLiveContainers>> getAllLiveContainers()
      throws IOException {
    LightWeightRequestHandler getLiveContainersHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            FiCaSchedulerAppLiveContainersDataAccess DA =
                (FiCaSchedulerAppLiveContainersDataAccess) RMStorageFactory.
                    getDataAccess(
                        FiCaSchedulerAppLiveContainersDataAccess.class);
            Map<String, List<FiCaSchedulerAppLiveContainers>>
                hopLiveContainers = DA.getAll();
            connector.commit();
            return hopLiveContainers;
          }
        };
    return (Map<String, List<FiCaSchedulerAppLiveContainers>>) getLiveContainersHandler
        .
            handle();
  }
  

  public static Map<String, List<ResourceRequest>> getAllResourceRequests()
      throws IOException {
    LightWeightRequestHandler getResourceRequestsHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ResourceRequestDataAccess DA =
                (ResourceRequestDataAccess) RMStorageFactory
                    .getDataAccess(ResourceRequestDataAccess.class);
            Map<String, List<ResourceRequest>> hopResourceRequests =
                DA.getAll();
            connector.commit();
            return hopResourceRequests;
          }
        };
    return (Map<String, List<ResourceRequest>>) getResourceRequestsHandler
        .handle();
  }
  

  public static Map<String, List<AppSchedulingInfoBlacklist>> getAllBlackLists()
      throws IOException {
    LightWeightRequestHandler getBlackListHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            AppSchedulingInfoBlacklistDataAccess DA =
                (AppSchedulingInfoBlacklistDataAccess) RMStorageFactory
                    .getDataAccess(AppSchedulingInfoBlacklistDataAccess.class);
            Map<String, List<AppSchedulingInfoBlacklist>> hopBlackList =
                DA.getAll();
            connector.commit();
            return hopBlackList;
          }
        };
    return (Map<String, List<AppSchedulingInfoBlacklist>>) getBlackListHandler
        .handle();
  }

  /**
   * Get Sequential Number from NDB.
   *
   * @param id
   * @return
   * @throws IOException
   */
  public static Integer getRMSequentialNumber(final int id) throws IOException {
    LightWeightRequestHandler getSequentialNumberHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            SequenceNumberDataAccess DA =
                (SequenceNumberDataAccess) RMStorageFactory
                    .getDataAccess(SequenceNumberDataAccess.class);
            SequenceNumber hseqNumber = (SequenceNumber) DA.findById(id);
            Integer seqNumber = null;
            if (hseqNumber != null) {
              seqNumber = hseqNumber.getSequencenumber();
            }
            connector.commit();
            return seqNumber;
          }
        };
    return (Integer) getSequentialNumberHandler.handle();
  }

  /**
   * Retrieve all DelegationToken rows from NDB
   *
   * @return
   * @throws IOException
   */
  public static List<DelegationToken> getDelegationTokens() throws IOException {
    LightWeightRequestHandler getDelegationTokenHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            DelegationTokenDataAccess DA =
                (DelegationTokenDataAccess) RMStorageFactory
                    .getDataAccess(DelegationTokenDataAccess.class);
            List<DelegationToken> delTokens = DA.getAll();
            connector.commit();
            return delTokens;
          }
        };
    return (List<DelegationToken>) getDelegationTokenHandler.handle();
  }

  /**
   * Retrieve all DelegationKey rows from NDB.
   *
   * @return
   * @throws IOException
   */
  public static List<DelegationKey> getDelegationKeys() throws IOException {
    LightWeightRequestHandler getDelegationKeyHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            DelegationKeyDataAccess DA =
                (DelegationKeyDataAccess) RMStorageFactory
                    .getDataAccess(DelegationKeyDataAccess.class);
            List<DelegationKey> delKeys = DA.getAll();
            connector.commit();
            return delKeys;
          }
        };
    return (List<DelegationKey>) getDelegationKeyHandler.handle();
  }

  /**
   * Store/Delete applicationState information from NDB.
   *
   * @param appId
   * @param appState
   * @throws IOException
   */
  public static void setApplicationState(final String appId,
      final byte[] appState) throws IOException {
    LightWeightRequestHandler setApplicationStateHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ApplicationStateDataAccess DA =
                (ApplicationStateDataAccess) RMStorageFactory
                    .getDataAccess(ApplicationStateDataAccess.class);
            ApplicationState hop =
                new ApplicationState(appId, appState, null, null, null);
            DA.add(hop);
            connector.commit();
            return null;
          }
        };
    setApplicationStateHandler.handle();
  }

  /**
   * Store/Delete ApplicationMasterRPC into/from NDB.
   *
   * @param rpcID
   * @param type
   * @param rpc
   * @throws IOException
   */
  public static void persistAppMasterRPC(final int rpcID, final RPC.Type type,
      final byte[] rpc) throws IOException {
    persistAppMasterRPC(rpcID, type, rpc, null);
  }
  
  public static void persistAppMasterRPC(final int rpcID, final RPC.Type type,
      final byte[] rpc, final String userId) throws IOException {
    LightWeightRequestHandler setAppMasterRPCHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            RPCDataAccess DA = (RPCDataAccess) RMStorageFactory
                .getDataAccess(RPCDataAccess.class);
            RPC hop = new RPC(rpcID, type, rpc, userId);

            DA.add(hop);
            LOG.debug("HOP :: persistAppMasterRPC() - persistRPC");

            connector.commit();
            LOG.debug("HOP :: persistAppMasterRPC() - persistRPC");
            return null;
          }
        };
    setAppMasterRPCHandler.handle();
  }

  public static void removeAppMasterRPC(final int rpcID) throws IOException {
    LightWeightRequestHandler setAppMasterRPCHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            RPCDataAccess DA = (RPCDataAccess) RMStorageFactory
                .getDataAccess(RPCDataAccess.class);
            RPC hop = new RPC(rpcID);
            DA.remove(hop);
            LOG.debug("HOP :: removeAppMasterRPC() - persistRPC");
            connector.commit();
            return null;
          }
        };
    setAppMasterRPCHandler.handle();
  }

  /**
   * Checks for pending RMNode RPCs and if the RMNode is active.
   * If an rpc for a particular RMNode and with the same type already exists
   * the method returns false. If the RPC does not exist it checks if the
   * RMNode is in the ActiveNodes table. If so, it returns the retrieved
   * RMNode.
   * <p/>
   *
   * @param type
   * @param rpc
   * @param userId
   * @return
   * @throws IOException
   */
  public static Map<Integer, Object> registerNMRPCValidation(
      final RPC.Type type, final byte[] rpc, final String userId)
      throws IOException {
    LightWeightRequestHandler registerNMRPCValidationHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            Map<Integer, Object> toReturn;
            //Check if an rpc of the same type for this RMNode already exists
            RPCDataAccess rpcDA = (RPCDataAccess) RMStorageFactory
                .getDataAccess(RPCDataAccess.class);
            //If it exists, return false
            if (rpcDA.findByTypeAndUserId(type.toString(), userId) /*||
                        rpcDA.findByTypeAndUserId(HopRPC.Type.NodeHeartbeat.toString(), userId)*/) {
              LOG.debug("HOP :: rmnodeRPCValidation() - RPC already exists");
              connector.commit();
              return null;
            } else {
              LOG.debug("HOP :: rmnodeRPCValidation() rpcIdFound was null");
              //Get new rpcId and persist it
              YarnVariablesDataAccess yDA =
                  (YarnVariablesDataAccess) RMStorageFactory
                      .getDataAccess(YarnVariablesDataAccess.class);
              YarnVariables found =
                  (YarnVariables) yDA.findById(HopYarnAPIUtilities.RPC);
              int rpcId = Integer.MIN_VALUE;
              if (found != null) {
                rpcId = found.getValue();
                found.setValue(found.getValue() + 1);
                yDA.add(found);
              }
              RPC rpcToPersist = new RPC(rpcId, type, rpc, userId);
              rpcDA.add(rpcToPersist);
              toReturn = new HashMap<Integer, Object>();
              toReturn.put(0, rpcId);
            }
            //Check if RMNode(userId) is in ActiveRMNodesMap
            RMContextActiveNodesDataAccess rmDA =
                (RMContextActiveNodesDataAccess) RMStorageFactory.
                    getDataAccess(RMContextActiveNodesDataAccess.class);

            RMContextActiveNodes hopActiveRMNode =
                (RMContextActiveNodes) rmDA.findEntry(userId);
            if (hopActiveRMNode != null) {
              toReturn.put(1, true);
            } else {
              toReturn.put(1, false);
            }
            connector.commit();
            return toReturn;
          }
        };
    return (Map<Integer, Object>) registerNMRPCValidationHandler.handle();
  }

  /**
   * Checks for pending heartbeats.
   *
   * @param type
   * @param rpc
   * @param id
   * @param rmContext
   * @return
   * @throws IOException
   */
  public static int heartbeatNMRPCValidation(final RPC.Type type,
      final byte[] rpc, final String id, final RMContext rmContext)
      throws IOException {
    LightWeightRequestHandler heartbeatNMRPCValidationHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            LOG.debug("HOP :: heartbeatNMRPCValidation - START");
            connector.beginTransaction();
            connector.writeLock();
            //Check if an rpc of the same type for this RMNode already exists
            RPCDataAccess rpcDA = (RPCDataAccess) RMStorageFactory
                .getDataAccess(RPCDataAccess.class);
            int rpcId = Integer.MIN_VALUE;
            if (rpcDA.findByTypeAndUserId(type.toString(), id)) {
              LOG.debug(
                  "HOP :: heartbeatNMRPCValidation() - RPC already exists");
              connector.commit();
              return Integer.MIN_VALUE;
            } else {
              //Get new rpcId and persist it
              YarnVariablesDataAccess yDA =
                  (YarnVariablesDataAccess) RMStorageFactory
                      .getDataAccess(YarnVariablesDataAccess.class);
              YarnVariables found =
                  (YarnVariables) yDA.findById(HopYarnAPIUtilities.RPC);
              if (found != null) {
                rpcId = found.getValue();
                found.setValue(found.getValue() + 1);

                yDA.add(found);
              }
              RPC rpcToPersist = new RPC(rpcId, type, rpc, id);
              rpcDA.add(rpcToPersist);
            }
            connector.commit();
            LOG.debug("HOP :: heartbeatNMRPCValidation - FINISH");
            return rpcId;
          }
        };
    return (Integer) heartbeatNMRPCValidationHandler.handle();
  }

  public static void updatePendingEvents(final PendingEvent persistedEvent,
      final int action) throws IOException {
    LightWeightRequestHandler updatePendingEventsHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            PendingEventDataAccess DA =
                (PendingEventDataAccess) RMStorageFactory
                    .getDataAccess(PendingEventDataAccess.class);
            if (action == TablesDef.PendingEventTableDef.PERSISTEDEVENT_ADD) {
              DA.createPendingEvent(persistedEvent);
            } else if (action == TablesDef.PendingEventTableDef.PERSISTEDEVENT_REMOVE) {
              DA.removePendingEvent(persistedEvent);
            }

            connector.commit();
            return null;
          }
        };
    updatePendingEventsHandler.handle();
  }
  
  public static RMNodeComps getRMNodeBatch(final String id) throws IOException {
    //get the RMNode
    LightWeightRequestHandler getRMNodeBatchHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask()
              throws StorageException, InvalidProtocolBufferException {
            connector.beginTransaction();
            connector.readLock();
            //Get HopRMNode
            FullRMNodeDataAccess fullRMNodeDA =
                (FullRMNodeDataAccess) RMStorageFactory.
                    getDataAccess(FullRMNodeDataAccess.class);
            RMNodeComps hopRMNodeFull =
                (RMNodeComps) fullRMNodeDA.findByNodeId(id);

            connector.commit();
            return hopRMNodeFull;
          }
        };
    return (RMNodeComps) getRMNodeBatchHandler.handle();
  }
  
  public static org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode getRMNode(
      final String id, final RMContext context, final Configuration conf)
      throws IOException {
    LightWeightRequestHandler getRMNodeHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();
            org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode rmNode =
                null;
            RMNodeDataAccess rmnodeDA = (RMNodeDataAccess) RMStorageFactory.
                getDataAccess(RMNodeDataAccess.class);
            RMNode hopRMNode = (RMNode) rmnodeDA.findByNodeId(id);
            if (hopRMNode != null) {
              ResourceDataAccess resDA = (ResourceDataAccess) RMStorageFactory
                  .getDataAccess(ResourceDataAccess.class);
              NodeDataAccess nodeDA = (NodeDataAccess) RMStorageFactory.
                  getDataAccess(NodeDataAccess.class);
              //Retrieve resource of RMNode
              Resource res = (Resource) resDA.findEntry(hopRMNode.
                      getNodeId(), Resource.TOTAL_CAPABILITY, Resource.RMNODE);

              NodeId nodeId = ConverterUtils.toNodeId(id);
              //Retrieve and Initialize NodeBase for RMNode
              org.apache.hadoop.net.Node node = null;
              if (hopRMNode.getNodeId() != null) {
                Node hopNode = (Node) nodeDA.findById(hopRMNode.
                    getNodeId());
                node = new NodeBase(hopNode.getName(), hopNode.getLocation());
                if (hopNode.getParent() != null) {
                  node.setParent(new NodeBase(hopNode.getParent()));
                }
                node.setLevel(hopNode.getLevel());
              }
              //Retrieve nextHeartbeat
              NextHeartbeatDataAccess nextHBDA =
                  (NextHeartbeatDataAccess) RMStorageFactory.
                      getDataAccess(NextHeartbeatDataAccess.class);
              boolean nextHeartbeat = nextHBDA.findEntry(id);
              //Create Resource
              ResourceOption resourceOption = null;
              if (res != null) {
                resourceOption = ResourceOption.newInstance(
                    org.apache.hadoop.yarn.api.records.Resource
                        .newInstance(res.getMemory(), res.getVirtualCores()),
                    hopRMNode.getOvercommittimeout());
              }
              rmNode = new RMNodeImpl(nodeId, context, hopRMNode.getHostName(),
                  hopRMNode.getCommandPort(), hopRMNode.getHttpPort(), node,
                  resourceOption, hopRMNode.getNodemanagerVersion(),
                  hopRMNode.getHealthReport(),
                  hopRMNode.getLastHealthReportTime(), nextHeartbeat,
                  conf.getBoolean(YarnConfiguration.HOPS_DISTRIBUTED_RT_ENABLED,
                      YarnConfiguration.DEFAULT_HOPS_DISTRIBUTED_RT_ENABLED));

              ((RMNodeImpl) rmNode).setState(hopRMNode.getCurrentState());
              // *** Recover maps/lists of RMNode ***
              //Use a cache for retrieved ContainerStatus
              Map<String, ContainerStatus> hopContainerStatuses =
                  new HashMap<String, ContainerStatus>();
              //1. Recover JustLaunchedContainers
              JustLaunchedContainersDataAccess jlcDA =
                  (JustLaunchedContainersDataAccess) RMStorageFactory
                      .getDataAccess(JustLaunchedContainersDataAccess.class);
              ContainerStatusDataAccess containerStatusDA =
                  (ContainerStatusDataAccess) RMStorageFactory
                      .getDataAccess(ContainerStatusDataAccess.class);
              List<JustLaunchedContainers> hopJlcList = jlcDA.findByRMNode(id);
              if (hopJlcList != null && !hopJlcList.isEmpty()) {
                Map<org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>
                    justLaunchedContainers =
                    new HashMap<org.apache.hadoop.yarn.api.records.ContainerId, org.apache.hadoop.yarn.api.records.ContainerStatus>();
                for (JustLaunchedContainers hop : hopJlcList) {
                  //Create ContainerId
                  org.apache.hadoop.yarn.api.records.ContainerId cid =
                      ConverterUtils.toContainerId(hop.getContainerId());
                  //Find and create ContainerStatus
                  if (!hopContainerStatuses.containsKey(hop.getContainerId())) {
                    hopContainerStatuses.put(hop.getContainerId(),
                        (ContainerStatus) containerStatusDA
                            .findEntry(hop.getContainerId(), id));
                  }
                  org.apache.hadoop.yarn.api.records.ContainerStatus conStatus =
                      org.apache.hadoop.yarn.api.records.ContainerStatus
                          .newInstance(cid, ContainerState.valueOf(
                                  hopContainerStatuses.get(hop.getContainerId())
                                      .getState()),
                              hopContainerStatuses.get(hop.getContainerId())
                                  .getDiagnostics(),
                              hopContainerStatuses.get(hop.getContainerId())
                                  .getExitstatus());
                  justLaunchedContainers.put(cid, conStatus);
                }
                ((RMNodeImpl) rmNode)
                    .setJustLaunchedContainers(justLaunchedContainers);
              }
              //2. Return ContainerIdToClean
              ContainerIdToCleanDataAccess cidToCleanDA =
                  (ContainerIdToCleanDataAccess) RMStorageFactory
                      .getDataAccess(ContainerIdToCleanDataAccess.class);
              List<ContainerId> cidToCleanList = cidToCleanDA.findByRMNode(id);
              if (cidToCleanList != null && !cidToCleanList.isEmpty()) {
                Set<org.apache.hadoop.yarn.api.records.ContainerId>
                    containersToClean =
                    new TreeSet<org.apache.hadoop.yarn.api.records.ContainerId>();
                for (ContainerId hop : cidToCleanList) {
                  //Create ContainerId
                  containersToClean
                      .add(ConverterUtils.toContainerId(hop.getContainerId()));
                }
                ((RMNodeImpl) rmNode).setContainersToClean(containersToClean);
              }
              //3. Finished Applications
              FinishedApplicationsDataAccess finishedAppsDA =
                  (FinishedApplicationsDataAccess) RMStorageFactory
                      .getDataAccess(FinishedApplicationsDataAccess.class);
              List<FinishedApplications> hopFinishedAppsList =
                  finishedAppsDA.findByRMNode(id);
              if (hopFinishedAppsList != null &&
                  !hopFinishedAppsList.isEmpty()) {
                List<ApplicationId> finishedApps =
                    new ArrayList<ApplicationId>();
                for (FinishedApplications hop : hopFinishedAppsList) {
                  finishedApps.add(
                      ConverterUtils.toApplicationId(hop.getApplicationId()));
                }
                ((RMNodeImpl) rmNode).setFinishedApplications(finishedApps);
              }

              //4. UpdadedContainerInfo
              UpdatedContainerInfoDataAccess uciDA =
                  (UpdatedContainerInfoDataAccess) RMStorageFactory
                      .getDataAccess(UpdatedContainerInfoDataAccess.class);
              //Retrieve all UpdatedContainerInfo entries for this particular RMNode
              Map<Integer, List<UpdatedContainerInfo>>
                  hopUpdatedContainerInfoMap = uciDA.findByRMNode(id);
              if (hopUpdatedContainerInfoMap != null &&
                  !hopUpdatedContainerInfoMap.isEmpty()) {
                ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>
                    updatedContainerInfoQueue =
                    new ConcurrentLinkedQueue<org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo>();
                for (int uciId : hopUpdatedContainerInfoMap.keySet()) {
                  for (UpdatedContainerInfo hopUCI : hopUpdatedContainerInfoMap
                      .get(uciId)) {
                    List<org.apache.hadoop.yarn.api.records.ContainerStatus>
                        newlyAllocated =
                        new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>();
                    List<org.apache.hadoop.yarn.api.records.ContainerStatus>
                        completed =
                        new ArrayList<org.apache.hadoop.yarn.api.records.ContainerStatus>();
                    //Retrieve containerstatus entries for the particular updatedcontainerinfo
                    org.apache.hadoop.yarn.api.records.ContainerId cid =
                        ConverterUtils.toContainerId(hopUCI.getContainerId());
                    if (!hopContainerStatuses
                        .containsKey(hopUCI.getContainerId())) {
                      hopContainerStatuses.put(hopUCI.getContainerId(),
                          (ContainerStatus) containerStatusDA
                              .findEntry(hopUCI.getContainerId(), id));
                    }
                    org.apache.hadoop.yarn.api.records.ContainerStatus
                        conStatus =
                        org.apache.hadoop.yarn.api.records.ContainerStatus
                            .newInstance(cid, ContainerState.valueOf(
                                    hopContainerStatuses
                                        .get(hopUCI.getContainerId())
                                        .getState()), hopContainerStatuses
                                    .get(hopUCI.getContainerId())
                                    .getDiagnostics(), hopContainerStatuses
                                    .get(hopUCI.getContainerId())
                                    .getExitstatus());
                    //Check ContainerStatus state to add it to appropriate list
                    if (conStatus != null) {
                      if (conStatus.getState().toString()
                          .equals(TablesDef.ContainerStatusTableDef.STATE_RUNNING)) {
                        newlyAllocated.add(conStatus);
                      } else if (conStatus.getState().toString()
                          .equals(TablesDef.ContainerStatusTableDef.STATE_COMPLETED)) {
                        completed.add(conStatus);
                      }
                    }
                    org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo
                        uci =
                        new org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo(
                            newlyAllocated, completed,
                            hopUCI.getUpdatedContainerInfoId());
                    updatedContainerInfoQueue.add(uci);
                    ((RMNodeImpl) rmNode)
                        .setUpdatedContainerInfo(updatedContainerInfoQueue);
                    //Update uci counter
                    ((RMNodeImpl) rmNode)
                        .setUpdatedContainerInfoId(hopRMNode.getUciId());
                  }
                }
              }

              //5. Retrieve latestNodeHeartBeatResponse
              NodeHBResponseDataAccess hbDA =
                  (NodeHBResponseDataAccess) RMStorageFactory
                      .getDataAccess(NodeHBResponseDataAccess.class);
              NodeHBResponse hopHB = (NodeHBResponse) hbDA.findById(id);
              if (hopHB != null) {
                NodeHeartbeatResponse hb = new NodeHeartbeatResponsePBImpl(
                    YarnServerCommonServiceProtos.NodeHeartbeatResponseProto
                        .parseFrom(hopHB.getResponse()));
                ((RMNodeImpl) rmNode).setLatestNodeHBResponse(hb);
              }
            }
            connector.commit();
            return rmNode;
          }
        };
    return (org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode) getRMNodeHandler
        .handle();
  }

  /**
   * Retrieves the nextheartbeat variable of RMNode from NDB.
   *
   * @return
   * @throws IOException
   */
  public static Map<String, Boolean> getAllNextHeartbeats() throws IOException {
    LightWeightRequestHandler getAllNextHeartbeatsHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            NextHeartbeatDataAccess nextHBDA =
                (NextHeartbeatDataAccess) RMStorageFactory.
                    getDataAccess(NextHeartbeatDataAccess.class);
            Map<String, Boolean> allNextHeartbeats = nextHBDA.getAll();
            connector.commit();
            return allNextHeartbeats;
          }
        };
    return (Map<String, Boolean>) getAllNextHeartbeatsHandler.handle();
  }
  
  /**
   * Retrieves the nextheartbeat variable of RMNode from NDB.
   *
   * @param id
   * @return
   * @throws IOException
   */
  public static boolean getNextHeartbeat(final String id) throws IOException {
    LightWeightRequestHandler getNextHeartbeatHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            NextHeartbeatDataAccess nextHBDA =
                (NextHeartbeatDataAccess) RMStorageFactory.
                    getDataAccess(NextHeartbeatDataAccess.class);
            boolean hopNextHB = nextHBDA.findEntry(id);
            connector.commit();
            return hopNextHB;
          }
        };
    return (Boolean) getNextHeartbeatHandler.handle();
  }
  

  /**
   * Persists pending events to NDB. Used for testing.
   *
   * @param pendingEvents
   * @throws IOException
   */
  public static void setPendingEvents(final List<PendingEvent> pendingEvents)
      throws IOException {
    LightWeightRequestHandler setPendingEventsHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            PendingEventDataAccess DA =
                (PendingEventDataAccess) RMStorageFactory
                    .getDataAccess(PendingEventDataAccess.class);
            DA.prepare(pendingEvents, null);
            connector.commit();
            return null;
          }
        };
    setPendingEventsHandler.handle();
  }

  /**
   * Retrieves pending events from NDB. If numberOfEvents is zero, it
   * retrieves all the events.
   *
   * @param numberOfEvents
   * @param status
   * @return
   * @throws IOException
   */
  public static Map<String, ConcurrentSkipListSet<PendingEvent>> getPendingEvents(
      final int numberOfEvents, final byte status) throws IOException {
    LightWeightRequestHandler getPendingEventsHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            LOG.debug("HOP :: getPendingEvents - START");
            connector.beginTransaction();
            connector.writeLock();
            List<PendingEvent> pendingEvents;
            //Map groups events by RMNode to assign them to ThreadPool
            Map<String, ConcurrentSkipListSet<PendingEvent>>
                pendingEventsByRMNode =
                new HashMap<String, ConcurrentSkipListSet<PendingEvent>>();
            PendingEventDataAccess DA =
                (PendingEventDataAccess) RMStorageFactory
                    .getDataAccess(PendingEventDataAccess.class);
            //Check the number of events to retrieve
            if (numberOfEvents == 0) {
              pendingEvents = DA.getAll(status);
              if (pendingEvents != null && !pendingEvents.isEmpty()) {
                for (PendingEvent hop : pendingEvents) {
                  if (!pendingEventsByRMNode.containsKey(hop.getRmnodeId())) {
                    pendingEventsByRMNode.put(hop.getRmnodeId(),
                        new ConcurrentSkipListSet<PendingEvent>());
                  }
                  pendingEventsByRMNode.get(hop.getRmnodeId()).add(hop);
                }
              }
            }
            connector.commit();
            LOG.debug("HOP :: getPendingEvents - FINISH");
            return pendingEventsByRMNode;
          }
        };
    return (Map<String, ConcurrentSkipListSet<PendingEvent>>) getPendingEventsHandler
        .handle();
  }

  /**
   * Retrieves pending events from NDB and updates their status.
   * If numberOfEvents is zero, it retrieves all the events.
   * <p/>
   *
   * @param numberOfEvents
   * @param status
   * @return
   * @throws IOException
   */
  public static Map<String, ConcurrentSkipListSet<PendingEvent>> getAndUpdatePendingEvents(
      final int numberOfEvents, final byte status) throws IOException {
    LightWeightRequestHandler getAndUpdatePendingEventsHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            List<PendingEvent> pendingEvents;
            //Map groups events by RMNode to assign them to ThreadPool
            Map<String, ConcurrentSkipListSet<PendingEvent>>
                pendingEventsByRMNode =
                new HashMap<String, ConcurrentSkipListSet<PendingEvent>>();
            PendingEventDataAccess DA =
                (PendingEventDataAccess) RMStorageFactory
                    .getDataAccess(PendingEventDataAccess.class);
            //Check the number of events to retrieve
            if (numberOfEvents == 0) {
              pendingEvents = DA.getAll(status);
              if (pendingEvents != null && !pendingEvents.isEmpty()) {
                List<PendingEvent> pendingEventsModified =
                    new ArrayList<PendingEvent>();

                for (PendingEvent hop : pendingEvents) {
                  if (!pendingEventsByRMNode.containsKey(hop.getRmnodeId())) {
                    pendingEventsByRMNode.put(hop.getRmnodeId(),
                        new ConcurrentSkipListSet<PendingEvent>());
                  }
                  pendingEventsByRMNode.get(hop.getRmnodeId()).add(hop);
                  PendingEvent pending =
                      new PendingEvent(hop.getRmnodeId(), hop.getType(),
                          TablesDef.PendingEventTableDef.PENDING, hop.getId());
                  pendingEventsModified.add(pending);
                }
                DA.prepare(pendingEventsModified, null);
              }
            }
            connector.commit();
            return pendingEventsByRMNode;
          }
        };
    return (Map<String, ConcurrentSkipListSet<PendingEvent>>) getAndUpdatePendingEventsHandler
        .handle();
  }

  public static void removeApplicationStateAndAttempts(final String appId,
      final Collection<String> attemptsToRemove) throws IOException {
    LightWeightRequestHandler setApplicationStateHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            if (appId != null) {
              connector.beginTransaction();
              connector.writeLock();
              ApplicationStateDataAccess DA =
                  (ApplicationStateDataAccess) RMStorageFactory
                      .getDataAccess(ApplicationStateDataAccess.class);
              //Remove this particular appState from NDB
              ApplicationState hop = new ApplicationState(appId);
              DA.remove(hop);

              //Remove attempts of this app
              ApplicationAttemptStateDataAccess attemptDA =
                  (ApplicationAttemptStateDataAccess) RMStorageFactory
                      .getDataAccess(ApplicationAttemptStateDataAccess.class);
              List<ApplicationAttemptState> attemptIdsToRemove =
                  new ArrayList<ApplicationAttemptState>();
              for (String attemptId : attemptsToRemove) {
                attemptIdsToRemove
                    .add(new ApplicationAttemptState(appId, attemptId));
              }
              attemptDA.removeAll(attemptIdsToRemove);
              connector.commit();
            }
            return null;
          }
        };
    setApplicationStateHandler.handle();
  }

  public static void removeDelegationToken(final int seqNumber)
      throws IOException {
    LightWeightRequestHandler setDelegationTokenHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            if (seqNumber != Integer.MIN_VALUE) {
              connector.beginTransaction();
              connector.writeLock();
              DelegationTokenDataAccess DA =
                  (DelegationTokenDataAccess) RMStorageFactory
                      .getDataAccess(DelegationTokenDataAccess.class);
              //Remove this particular DT from NDB
              DelegationToken dtToRemove = new DelegationToken(seqNumber, null);
              DA.remove(dtToRemove);
              connector.commit();
            }
            return null;
          }
        };
    setDelegationTokenHandler.handle();
  }

  public static void removeRMDTMasterKey(final int key) throws IOException {
    LightWeightRequestHandler setRMDTMasterKeyHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            LOG.debug("HOP :: key=" + key);
            if (key != Integer.MIN_VALUE) {
              connector.beginTransaction();
              connector.writeLock();
              DelegationKeyDataAccess DA =
                  (DelegationKeyDataAccess) RMStorageFactory
                      .getDataAccess(DelegationKeyDataAccess.class);
              //Remove this particular DK from NDB
              DelegationKey dkeyToremove = new DelegationKey(key, null);
              DA.remove(dkeyToremove);
              connector.commit();
              LOG.debug("HOP :: committed");
            }
            return null;
          }
        };
    setRMDTMasterKeyHandler.handle();
  }

  public static void setRMTokenSecretManagerMasterKeyState(final MasterKey key,
      final RMStateStore.KeyType keyType) throws IOException {
    LightWeightRequestHandler setRMTokenSecretManagerMasterKeyStateHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            SecretMamagerKeysDataAccess DA =
                (SecretMamagerKeysDataAccess) RMStorageFactory
                    .getDataAccess(SecretMamagerKeysDataAccess.class);
            SecretMamagerKey hop = new SecretMamagerKey(keyType.toString(),
                ((MasterKeyPBImpl) key).getProto().toByteArray());
            DA.add(hop);
            connector.commit();
            return null;
          }
        };
    setRMTokenSecretManagerMasterKeyStateHandler.handle();
  }

  public static void removeRMTokenSecretManagerMasterKeyState(
      final RMStateStore.KeyType keyType) throws IOException {
    LightWeightRequestHandler RMTokenSecretManagerMasterKeyStateHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            SecretMamagerKeysDataAccess DA =
                (SecretMamagerKeysDataAccess) RMStorageFactory
                    .getDataAccess(SecretMamagerKeysDataAccess.class);
            //Remove this particular DK from NDB
            SecretMamagerKey hop =
                new SecretMamagerKey(keyType.toString(), null);
            DA.remove(hop);
            connector.commit();
            return null;
          }
        };
    RMTokenSecretManagerMasterKeyStateHandler.handle();
  }

  /**
   * Retrieve all applicationAttemptId rows for particular ApplicationState.
   *
   * @return
   * @throws IOException
   */
  public static Map<String, List<ApplicationAttemptState>> getAllApplicationAttemptStates()
      throws IOException {
    LightWeightRequestHandler getApplicationAttemptIdsByApplicationIdHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ApplicationAttemptStateDataAccess DA =
                (ApplicationAttemptStateDataAccess) RMStorageFactory
                    .getDataAccess(ApplicationAttemptStateDataAccess.class);
            Map<String, List<ApplicationAttemptState>> attempts = DA.getAll();
            connector.commit();
            return attempts;
          }
        };
    return (Map<String, List<ApplicationAttemptState>>) getApplicationAttemptIdsByApplicationIdHandler
        .handle();
  }

  /**
   * Retrieve HopApplicationAttemptState particular appId and attemptId.
   *
   * @param appId
   * @param attemptId
   * @return
   * @throws IOException
   */
  public static ApplicationAttemptState getApplicationAttemptState(
      final String appId, final String attemptId) throws IOException {
    LightWeightRequestHandler getApplicationAttemptIdsByApplicationIdHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ApplicationAttemptStateDataAccess DA =
                (ApplicationAttemptStateDataAccess) RMStorageFactory
                    .getDataAccess(ApplicationAttemptStateDataAccess.class);
            ApplicationAttemptState attempt =
                (ApplicationAttemptState) DA.findEntry(appId, attemptId);
            connector.commit();
            return attempt;
          }
        };
    return (ApplicationAttemptState) getApplicationAttemptIdsByApplicationIdHandler
        .handle();
  }

  /**
   * Create an ApplicationAttemptId at NDB.
   *
   * @param appId
   * @param appAttemptId
   * @param attemptIdData
   * @throws IOException
   */
  public static void setApplicationAttemptId(final String appId,
      final String appAttemptId, final byte[] attemptIdData)
      throws IOException {
    LightWeightRequestHandler setApplicationAttemptIdHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ApplicationAttemptStateDataAccess DA =
                (ApplicationAttemptStateDataAccess) RMStorageFactory.
                    getDataAccess(ApplicationAttemptStateDataAccess.class);
            DA.createApplicationAttemptStateEntry(
                new ApplicationAttemptState(appId, appAttemptId, attemptIdData,
                    null, 0, null, null));
            connector.commit();
            return null;
          }
        };
    setApplicationAttemptIdHandler.handle();
  }

  /**
   * Create a DelegationToken and the respective sequence number for that token
   * at NDB These operations take place in the same transaction.
   *
   * @param rmDTIdentifier
   * @param renewDate
   * @param latestSequenceNumber
   * @throws IOException
   */
  public static void setTokenAndSequenceNumber(
      final RMDelegationTokenIdentifier rmDTIdentifier, final Long renewDate,
      final int latestSequenceNumber) throws IOException {
    LightWeightRequestHandler setTokenAndSequenceNumberHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            DelegationTokenDataAccess DA =
                (DelegationTokenDataAccess) RMStorageFactory
                    .getDataAccess(DelegationTokenDataAccess.class);
            //Create byte array for RMDelegationTokenIdentifier and renewdate
            ByteArrayOutputStream tokenOs = new ByteArrayOutputStream();
            DataOutputStream tokenOut = new DataOutputStream(tokenOs);
            byte[] identifierBytes;
            try {
              rmDTIdentifier.write(tokenOut);
              tokenOut.writeLong(renewDate);
              identifierBytes = tokenOs.toByteArray();
            } finally {
              tokenOs.close();
            }
            DA.createDelegationTokenEntry(
                new DelegationToken(rmDTIdentifier.getSequenceNumber(),
                    identifierBytes));

            //Persist sequence number
            SequenceNumberDataAccess SDA =
                (SequenceNumberDataAccess) RMStorageFactory
                    .getDataAccess(SequenceNumberDataAccess.class);
            SequenceNumber sn = new SequenceNumber(NDBRMStateStore.SEQNUMBER_ID,
                latestSequenceNumber);
            SDA.add(sn);
            connector.commit();
            return null;
          }
        };
    setTokenAndSequenceNumberHandler.handle();
  }

  public static void setRMDTMasterKeyState(
      final org.apache.hadoop.security.token.delegation.DelegationKey delegationKey)
      throws IOException {
    LightWeightRequestHandler setRMDTMasterKeyHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            DelegationKeyDataAccess DA =
                (DelegationKeyDataAccess) RMStorageFactory
                    .getDataAccess(DelegationKeyDataAccess.class);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try {
              DataOutputStream fsOut = new DataOutputStream(os);
              delegationKey.write(fsOut);
            } finally {
              os.close();
            }
            DA.createDTMasterKeyEntry(
                new DelegationKey(delegationKey.getKeyId(), os.toByteArray()));

            connector.commit();
            return null;
          }
        };
    setRMDTMasterKeyHandler.handle();
  }

  
  public static List<RMContextActiveNodes> getAllRMContextActiveNodes()
      throws IOException {
    LightWeightRequestHandler getAllRMContextActiveNodesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            RMContextActiveNodesDataAccess rmctxnodesDA =
                (RMContextActiveNodesDataAccess) RMStorageFactory.
                    getDataAccess(RMContextActiveNodesDataAccess.class);
            List<RMContextActiveNodes> hopRMContextNodes = rmctxnodesDA.
                findAll();
            connector.commit();
            return hopRMContextNodes;
          }
        };
    return (List<RMContextActiveNodes>) getAllRMContextActiveNodesHandler.
        handle();
  }
  
  public static Map<String, RMNode> getAllRMNodes() throws IOException {
    LightWeightRequestHandler getAllRMNodesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            RMNodeDataAccess rmDA = (RMNodeDataAccess) RMStorageFactory.
                getDataAccess(RMNodeDataAccess.class);
            Map<String, RMNode> rmNodes = rmDA.
                getAll();
            connector.commit();
            return rmNodes;
          }
        };
    return (Map<String, RMNode>) getAllRMNodesHandler.handle();
  }
  
  public static Map<String, Node> getAllNodes() throws IOException {
    LightWeightRequestHandler getAllNodesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            NodeDataAccess nodeDA = (NodeDataAccess) RMStorageFactory
                .getDataAccess(NodeDataAccess.class);
            Map<String, Node> rmNodes = nodeDA.
                getAll();
            connector.commit();
            return rmNodes;
          }
        };
    return (Map<String, Node>) getAllNodesHandler.handle();
  }

  
  public static List<RMContextInactiveNodes> getAllRMContextInactiveNodes()
      throws IOException {
    LightWeightRequestHandler getAllRMContextInactiveNodesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            RMContextInactiveNodesDataAccess rmctxInactiveNodesDA =
                (RMContextInactiveNodesDataAccess) RMStorageFactory.
                    getDataAccess(RMContextInactiveNodesDataAccess.class);
            List<RMContextInactiveNodes> hopRMContextInactiveNodes =
                rmctxInactiveNodesDA.findAll();
            connector.commit();
            return hopRMContextInactiveNodes;
          }
        };
    return (List<RMContextInactiveNodes>) getAllRMContextInactiveNodesHandler.
        handle();
  }

  
  private static Map<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>
      alreadyRecoveredRMContextInactiveNodes =
      new HashMap<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>();

  /**
   * Recover inactive nodes map of RMContextImpl.
   *
   * @param rmContext
   * @param state
   * @return
   * @throws java.lang.Exception
   */
  //For testing TODO move to test
  public static Map<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode> getRMContextInactiveNodes(
      final RMContext rmContext, final RMState state, final Configuration conf)
      throws Exception {
    LightWeightRequestHandler getRMContextInactiveNodesHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ConcurrentMap<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>
                inactiveNodes =
                new ConcurrentHashMap<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>();
            //Retrieve rmctxnodes table entries
            RMContextInactiveNodesDataAccess rmctxInactiveNodesDA =
                (RMContextInactiveNodesDataAccess) RMStorageFactory
                    .getDataAccess(RMContextInactiveNodesDataAccess.class);
            ResourceDataAccess DA = (ResourceDataAccess) YarnAPIStorageFactory
                .getDataAccess(ResourceDataAccess.class);
            RMNodeDataAccess rmDA = (RMNodeDataAccess) RMStorageFactory
                .getDataAccess(RMNodeDataAccess.class);
            List<RMContextInactiveNodes> hopRMContextInactiveNodes =
                rmctxInactiveNodesDA.findAll();
            if (hopRMContextInactiveNodes != null &&
                !hopRMContextInactiveNodes.isEmpty()) {
              for (RMContextInactiveNodes key : hopRMContextInactiveNodes) {

                NodeId nodeId = ConverterUtils.toNodeId(key.getRmnodeid());
                //retrieve RMNode in order to create a new FiCaSchedulerNode
                RMNode hopRMNode =
                    (RMNode) rmDA.findByNodeId(key.getRmnodeid());
                //Retrieve resource of RMNode
                Resource res = (Resource) DA
                    .findEntry(hopRMNode.getNodeId(), Resource.TOTAL_CAPABILITY,
                        Resource.RMNODE);
                //Retrieve and Initialize NodeBase for RMNode
                NodeDataAccess nodeDA = (NodeDataAccess) RMStorageFactory
                    .getDataAccess(NodeDataAccess.class);
                //Retrieve and Initialize NodeBase for RMNode
                org.apache.hadoop.net.Node node = null;
                if (hopRMNode.getNodeId() != null) {
                  Node hopNode = (Node) nodeDA.findById(hopRMNode.getNodeId());
                  node = new NodeBase(hopNode.getName(), hopNode.getLocation());
                  if (hopNode.getParent() != null) {
                    node.setParent(new NodeBase(hopNode.getParent()));
                  }
                  node.setLevel(hopNode.getLevel());
                }
                //Retrieve nextHeartbeat
                NextHeartbeatDataAccess nextHBDA =
                    (NextHeartbeatDataAccess) RMStorageFactory.
                        getDataAccess(NextHeartbeatDataAccess.class);
                boolean nextHeartbeat = nextHBDA.findEntry(key.getRmnodeid());
                org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode
                    rmNode =
                    new RMNodeImpl(nodeId, rmContext, hopRMNode.getHostName(),
                        hopRMNode.getCommandPort(), hopRMNode.getHttpPort(),
                        node, ResourceOption.newInstance(
                        org.apache.hadoop.yarn.api.records.Resource
                            .newInstance(res.getMemory(),
                                res.getVirtualCores()),
                        hopRMNode.getOvercommittimeout()),
                        hopRMNode.getNodemanagerVersion(),
                        hopRMNode.getHealthReport(),
                        hopRMNode.getLastHealthReportTime(), nextHeartbeat,
                        conf.getBoolean(
                            YarnConfiguration.HOPS_DISTRIBUTED_RT_ENABLED,
                            YarnConfiguration.DEFAULT_HOPS_DISTRIBUTED_RT_ENABLED));
                ((RMNodeImpl) rmNode).setState(hopRMNode.getCurrentState());
                alreadyRecoveredRMContextInactiveNodes
                    .put(rmNode.getNodeID().getHost(), rmNode);
                inactiveNodes.put(rmNode.getNodeID().getHost(), rmNode);

              }
            }
            connector.commit();
            return inactiveNodes;
          }
        };
    try {
      if (alreadyRecoveredRMContextInactiveNodes.isEmpty()) {
        Map<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>
            result =
            (Map<String, org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode>) getRMContextInactiveNodesHandler
                .handle();
        for (org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode node : result
            .values()) {
          node.recover(state);
        }
        return result;
      } else {
        return alreadyRecoveredRMContextInactiveNodes;
      }
    } catch (IOException ex) {
      LOG.error("HOP", ex);
    }
    return null;
  }

  public static Map<String, Map<Integer, List<UpdatedContainerInfo>>> getAllUpdatedContainerInfos()
      throws IOException {
    LightWeightRequestHandler getAllUpdatedContainerInfosHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            UpdatedContainerInfoDataAccess uciDA =
                (UpdatedContainerInfoDataAccess) RMStorageFactory.
                    getDataAccess(UpdatedContainerInfoDataAccess.class);
            Map<String, Map<Integer, List<UpdatedContainerInfo>>>
                allUpdatedContainerInfos = uciDA.getAll();
            connector.commit();
            return allUpdatedContainerInfos;
          }
        };
    return (Map<String, Map<Integer, List<UpdatedContainerInfo>>>) getAllUpdatedContainerInfosHandler
        .
            handle();
  }
  
  public static Map<String, ContainerStatus> getAllContainerStatus()
      throws IOException {
    LightWeightRequestHandler getAllContainerStatusHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            ContainerStatusDataAccess csDA =
                (ContainerStatusDataAccess) YarnAPIStorageFactory.
                    getDataAccess(ContainerStatusDataAccess.class);
            Map<String, ContainerStatus> allContainerStatus = csDA.getAll();
            connector.commit();
            return allContainerStatus;
          }
        };
    return (Map<String, ContainerStatus>) getAllContainerStatusHandler.
        handle();
  }
  

  public static Map<String, NodeHBResponse> getAllNodeHeartBeatResponse()
      throws IOException {
    LightWeightRequestHandler getNodeHeartBeatResponseHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();

            NodeHBResponseDataAccess DA =
                (NodeHBResponseDataAccess) RMStorageFactory
                    .getDataAccess(NodeHBResponseDataAccess.class);
            Map<String, NodeHBResponse> hop = DA.getAll();
            connector.commit();
            return hop;
          }
        };
    
    return (Map<String, NodeHBResponse>) getNodeHeartBeatResponseHandler
        .handle();
  }


  public static Map<String, Set<ContainerId>> getAllContainersToClean()
      throws IOException {
    LightWeightRequestHandler getContainersToCleanHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            ContainerIdToCleanDataAccess tocleanDA =
                (ContainerIdToCleanDataAccess) YarnAPIStorageFactory.
                    getDataAccess(ContainerIdToCleanDataAccess.class);
            Map<String, Set<ContainerId>> hopContainerIdToClean =
                tocleanDA.getAll();
            connector.commit();
            return hopContainerIdToClean;
          }
        };
    return (Map<String, Set<ContainerId>>) getContainersToCleanHandler.handle();
  }

  /**
   * Retrieves and sets RMNode containersToClean and FinishedApplications.
   *
   * @param rmnodeId
   * @param containersToClean
   * @param finishedApplications
   * @throws IOException
   */
  public static void setContainersToCleanAndFinishedApplications(
      final String rmnodeId,
      final Set<org.apache.hadoop.yarn.api.records.ContainerId> containersToClean,
      final List<ApplicationId> finishedApplications) throws IOException {
    LightWeightRequestHandler setContainersToCleanAndFinishedAppsHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.readLock();
            //1. Retrieve ContainerIdToClean
            ContainerIdToCleanDataAccess tocleanDA =
                (ContainerIdToCleanDataAccess) YarnAPIStorageFactory.
                    getDataAccess(ContainerIdToCleanDataAccess.class);
            List<ContainerId> hopContainersToClean =
                tocleanDA.findByRMNode(rmnodeId);
            if (hopContainersToClean != null &&
                !hopContainersToClean.isEmpty()) {
              Set<org.apache.hadoop.yarn.api.records.ContainerId>
                  containersToCleanNDB =
                  new TreeSet<org.apache.hadoop.yarn.api.records.ContainerId>();
              for (ContainerId hop : hopContainersToClean) {
                containersToCleanNDB
                    .add(ConverterUtils.toContainerId(hop.getContainerId()));
              }
              containersToClean.clear();
              containersToClean.addAll(containersToCleanNDB);
            }

            //2. Retrieve finishedApplications
            FinishedApplicationsDataAccess finishedAppsDA =
                (FinishedApplicationsDataAccess) YarnAPIStorageFactory.
                    getDataAccess(FinishedApplicationsDataAccess.class);
            List<FinishedApplications> hopFinishedApps =
                finishedAppsDA.findByRMNode(rmnodeId);
            if (hopFinishedApps != null && !hopFinishedApps.isEmpty()) {
              List<ApplicationId> finishedApplicationsNDB =
                  new ArrayList<ApplicationId>();
              for (FinishedApplications hopFinishedApp : hopFinishedApps) {
                finishedApplicationsNDB.add(ConverterUtils
                    .toApplicationId(hopFinishedApp.getApplicationId()));
              }
              finishedApplications.clear();
              finishedApplications.addAll(finishedApplicationsNDB);
            }

            connector.commit();
            return null;
          }
        };
    setContainersToCleanAndFinishedAppsHandler.handle();
  }
  
  
  public static Map<String, List<FinishedApplications>> getAllFinishedApplications()
      throws IOException {
    LightWeightRequestHandler getFinishedApplicationsHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.writeLock();
            FinishedApplicationsDataAccess finishedAppsDA =
                (FinishedApplicationsDataAccess) RMStorageFactory.
                    getDataAccess(FinishedApplicationsDataAccess.class);
            Map<String, List<FinishedApplications>> hopFinishedApps =
                finishedAppsDA.getAll();

            connector.commit();
            return hopFinishedApps;
          }
        };
    return (Map<String, List<FinishedApplications>>) getFinishedApplicationsHandler
        .
            handle();
  }
  
  public static Map<String, List<JustLaunchedContainers>> getAllJustLaunchedContainers()
      throws IOException {
    LightWeightRequestHandler getAllJustLaunchedContainersHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            JustLaunchedContainersDataAccess jlcDA =
                (JustLaunchedContainersDataAccess) YarnAPIStorageFactory.
                    getDataAccess(JustLaunchedContainersDataAccess.class);
            Map<String, List<JustLaunchedContainers>> justLaunchedContainers =
                jlcDA.getAll();
            connector.commit();
            return justLaunchedContainers;
          }
        };

    return (Map<String, List<JustLaunchedContainers>>) getAllJustLaunchedContainersHandler
        .
            handle();
  }
  

  public static Map<String, RMContainer> getAllRMContainers()
      throws IOException {

    LightWeightRequestHandler getRMContainerHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            RMContainerDataAccess DA = (RMContainerDataAccess) RMStorageFactory
                .getDataAccess(RMContainerDataAccess.class);
            Map<String, RMContainer> found = DA.getAll();
            connector.commit();
            return found;
          }
        };

    return (Map<String, RMContainer>) getRMContainerHandler.handle();
  }
  

  public static Map<String, Container> getAllContainers() throws IOException {
    LightWeightRequestHandler getContainerHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            ContainerDataAccess DA = (ContainerDataAccess) RMStorageFactory.
                getDataAccess(ContainerDataAccess.class);
            Map<String, Container> found = DA.getAll();
            connector.commit();
            return found;
          }
        };
    return (Map<String, Container>) getContainerHandler.handle();
  }
  
  
  public static List<QueueMetrics> getAllQueueMetrics() throws IOException {
    LightWeightRequestHandler handler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.readCommitted();
            QueueMetricsDataAccess qDA =
                (QueueMetricsDataAccess) RMStorageFactory
                    .getDataAccess(QueueMetricsDataAccess.class);
            return qDA.findAll();
          }
        };
    return (List<QueueMetrics>) handler.handle();
  }

  static int nextRPC = 0;
  static Map<Integer, TransactionStateImpl> finishedRPCs =
      new HashMap<Integer, TransactionStateImpl>();
  static Lock nextRPCLock = new ReentrantLock();
  
  public static void finishRPCs(TransactionStateImpl ts, int rpcID) throws IOException {
    nextRPCLock.lock();
    LOG.debug("finishing rpc " + rpcID + " " + nextRPC);
    if (rpcID > nextRPC) {
      finishedRPCs.put(rpcID, ts);
      nextRPCLock.unlock();
    } else {
      nextRPCLock.unlock();
      do {
        finishRPC(ts, rpcID);
        nextRPCLock.lock();
        try {
          nextRPC++;
          rpcID = nextRPC;
          ts = finishedRPCs.remove(rpcID);
        } finally {
          nextRPCLock.unlock();
        }
      } while (ts != null);
    }
  }
  
  public static void finishRPC(final TransactionStateImpl ts, final int rpcID) throws IOException {

    LOG.debug("HOP :: finishRPC - START:" + rpcID);

    LightWeightRequestHandler setfinishRPCHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();
            LOG.debug("HOP :: finishRPC() - handler for rpc: " + rpcID);

            RPCDataAccess DA = (RPCDataAccess) RMStorageFactory
                .getDataAccess(RPCDataAccess.class);
            RMNodeDataAccess rmnodeDA = (RMNodeDataAccess) RMStorageFactory
                .getDataAccess(RMNodeDataAccess.class);
            ResourceDataAccess resourceDA =
                (ResourceDataAccess) YarnAPIStorageFactory
                    .getDataAccess(ResourceDataAccess.class);
            NodeDataAccess nodeDA = (NodeDataAccess) YarnAPIStorageFactory
                .getDataAccess(NodeDataAccess.class);
            RMContextInactiveNodesDataAccess rmctxInactiveNodesDA =
                (RMContextInactiveNodesDataAccess) RMStorageFactory
                    .getDataAccess(RMContextInactiveNodesDataAccess.class);
            FiCaSchedulerNodeDataAccess ficaNodeDA =
                (FiCaSchedulerNodeDataAccess) RMStorageFactory
                    .getDataAccess(FiCaSchedulerNodeDataAccess.class);
            NodeHBResponseDataAccess hbDA =
                (NodeHBResponseDataAccess) YarnAPIStorageFactory
                    .getDataAccess(NodeHBResponseDataAccess.class);
            ContainerStatusDataAccess csDA =
                (ContainerStatusDataAccess) YarnAPIStorageFactory
                    .getDataAccess(ContainerStatusDataAccess.class);
            ContainerIdToCleanDataAccess cidToCleanDA =
                (ContainerIdToCleanDataAccess) YarnAPIStorageFactory
                    .getDataAccess(ContainerIdToCleanDataAccess.class);
            JustLaunchedContainersDataAccess justLaunchedContainersDA =
                (JustLaunchedContainersDataAccess) YarnAPIStorageFactory
                    .getDataAccess(JustLaunchedContainersDataAccess.class);
            UpdatedContainerInfoDataAccess updatedContainerInfoDA =
                (UpdatedContainerInfoDataAccess) RMStorageFactory
                    .getDataAccess(UpdatedContainerInfoDataAccess.class);
            FinishedApplicationsDataAccess faDA =
                (FinishedApplicationsDataAccess) RMStorageFactory
                    .getDataAccess(FinishedApplicationsDataAccess.class);
            RMContainerDataAccess rmcontainerDA =
                (RMContainerDataAccess) RMStorageFactory
                    .getDataAccess(RMContainerDataAccess.class);
            LaunchedContainersDataAccess launchedContainersDA =
                (LaunchedContainersDataAccess) RMStorageFactory
                    .getDataAccess(LaunchedContainersDataAccess.class);
            QueueMetricsDataAccess QMDA =
                (QueueMetricsDataAccess) RMStorageFactory
                    .getDataAccess(QueueMetricsDataAccess.class);

            FSSchedulerNodeDataAccess FSSNodeDA =
                (FSSchedulerNodeDataAccess) RMStorageFactory
                    .getDataAccess(FSSchedulerNodeDataAccess.class);
            PendingEventDataAccess persistedEventDA =
                (PendingEventDataAccess) RMStorageFactory
                    .getDataAccess(PendingEventDataAccess.class);
            NextHeartbeatDataAccess nextHeartbeatDA =
                (NextHeartbeatDataAccess) RMStorageFactory
                    .getDataAccess(NextHeartbeatDataAccess.class);

            if (rpcID >= 0) {
              RPC hop = new RPC(rpcID);
              DA.remove(hop);
            }
            //TODO put all of this in ts.persist
            ts.persistRMNodeToUpdate(rmnodeDA);
            ts.persistRmcontextInfo(rmnodeDA, resourceDA, nodeDA,
                rmctxInactiveNodesDA);

            ts.persistRMNodeInfo(hbDA, cidToCleanDA, justLaunchedContainersDA,
                updatedContainerInfoDA, faDA, csDA);
            ts.persist();
            ts.persistFicaSchedulerNodeInfo(resourceDA, ficaNodeDA,
                rmcontainerDA, launchedContainersDA);
            ts.persistFairSchedulerNodeInfo(FSSNodeDA);
            ts.persistSchedulerApplicationInfo(QMDA);
            ts.persistPendingEvents(persistedEventDA);

            connector.commit();

            if (ts.getRMNode() != null) {
              ts.getRMNode().setPersisted(true);
            }

            LOG.debug("HOP :: finishRPC - FINISH:" + rpcID);
            return null;
          }
        };
      setfinishRPCHandler.handle();
  }

  //for testing (todo: move in test class)
  public static Resource getResource(final String id, final int type,
      final int parent) throws IOException {
    LightWeightRequestHandler getResourceHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.readCommitted();
            ResourceDataAccess DA = (ResourceDataAccess) YarnAPIStorageFactory
                .getDataAccess(ResourceDataAccess.class);
            Resource res = ((Resource) DA.findEntry(id, type, parent));
            connector.commit();
            return res;
          }
        };

    return (Resource) getResourceHandler.handle();
  }

  public static Map<String, Map<Integer, Map<Integer, Resource>>> getAllNodesResources()
      throws IOException {
    LightWeightRequestHandler getResourceHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws StorageException {
            connector.beginTransaction();
            connector.readCommitted();
            ResourceDataAccess DA = (ResourceDataAccess) YarnAPIStorageFactory
                .getDataAccess(ResourceDataAccess.class);
            Map<String, Map<Integer, Map<Integer, Resource>>> res = DA.
                getAll();
            connector.commit();
            return res;
          }
        };

    return (Map<String, Map<Integer, Map<Integer, Resource>>>) getResourceHandler
        .
            handle();
  }
  
  public static Map<String, Load> getAllLoads() throws IOException {
    LightWeightRequestHandler getLoadHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {

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
  
  public static void InitializeDB() throws IOException {
    LightWeightRequestHandler setRMDTMasterKeyHandler =
        new LightWeightRequestHandler(YARNOperationType.TEST) {
          @Override
          public Object performTask() throws IOException {
            // TODO HDFS should already have initialized it?
            connector.formatStorage();
            return null;
          }
        };
    setRMDTMasterKeyHandler.handle();
    emptyStaticLists();
  }

  private static void emptyStaticLists() {
    alreadyRecoveredRMContextInactiveNodes.clear();
  }
  
  public static String getCallerMethod(String className) {
    StackTraceElement[] elements = Thread.currentThread().getStackTrace();
    StringBuilder sb = new StringBuilder();
    sb.append(", caller-");
    for (StackTraceElement elem : elements) {
      if (elem.getClassName().contains(className)) {
        sb.append(elem.getClassName());
        sb.append("-");
        sb.append(elem.getMethodName());
      }
    }
    return sb.toString();
  }
}
