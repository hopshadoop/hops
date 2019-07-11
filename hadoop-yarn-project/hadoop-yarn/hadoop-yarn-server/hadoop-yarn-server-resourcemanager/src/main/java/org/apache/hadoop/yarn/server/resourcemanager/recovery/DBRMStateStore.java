/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import com.google.common.annotations.VisibleForTesting;
import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.IntVariable;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.yarn.dal.ReservationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.ReservationState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationState;
import io.hops.metadata.yarn.entity.rmstatestore.DelegationToken;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.util.RMStorageFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import static org.apache.hadoop.yarn.server.resourcemanager.recovery.LeveldbRMStateStore.LOG;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMDelegationTokenIdentifierData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class DBRMStateStore extends RMStateStore {

  protected static final Version CURRENT_VERSION_INFO = Version
          .newInstance(1, 5);
  private Thread verifyActiveStatusThread;
  private int dbSessionTimeout;

  @Override
  public synchronized void initInternal(Configuration conf) throws Exception {
    dbSessionTimeout = conf.getInt(CommonConfigurationKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_KEY,
        CommonConfigurationKeys.DFS_LEADER_CHECK_INTERVAL_IN_MS_DEFAULT);
  }

  @Override
  public synchronized void startInternal() throws Exception {
    if (HAUtil.isHAEnabled(getConfig()) && !HAUtil
        .isAutomaticFailoverEnabled(getConfig())) {
      verifyActiveStatusThread = new VerifyActiveStatusThread();
      verifyActiveStatusThread.start();
    }
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
    if (verifyActiveStatusThread != null) {
      verifyActiveStatusThread.interrupt();
      verifyActiveStatusThread.join(1000);
    }
  }

  @Override
  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  @Override
  protected synchronized void storeVersion() throws Exception {
    final byte[] version = ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().
            toByteArray();
    storeVersiondb(version);
  }

  void storeVersiondb(byte[] version) throws Exception {
    setVariable(new ByteArrayVariable(Variable.Finder.RMStateStoreVersion,
            version));
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

  @Override
  protected synchronized Version loadVersion() throws Exception {
    byte[] protoFound = loadVersionInternal();
    Version versionFound = null;
    if (protoFound != null) {
      versionFound = new VersionPBImpl(VersionProto.parseFrom(protoFound));
    }
    return versionFound;
  }

  private byte[] loadVersionInternal() throws IOException {
    ByteArrayVariable var = (ByteArrayVariable) getVariable(
            Variable.Finder.RMStateStoreVersion);
    return (byte[]) var.getValue();
  }

  private Variable getVariableInt(Variable.Finder finder) throws
          StorageException {
    VariableDataAccess DA
            = (VariableDataAccess) RMStorageFactory
            .getDataAccess(VariableDataAccess.class);
    return (Variable) DA.getVariable(finder);

  }

  private Variable getVariable(final Variable.Finder finder) throws IOException {
    LightWeightRequestHandler getVersionHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.readCommitted();
        Variable var = getVariableInt(finder);

        connector.commit();
        return var;
      }
    };
    return (Variable) getVersionHandler.handle();
  }

  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    final Variable.Finder dbKey = Variable.Finder.RMStateStoreEpoch;
    
    LightWeightRequestHandler getAndIncrementEpochHandler
            = new LightWeightRequestHandler(
                    YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        VariableDataAccess DA
                = (VariableDataAccess) RMStorageFactory
                .getDataAccess(VariableDataAccess.class);
        LongVariable var = (LongVariable) DA.getVariable(dbKey);

        long currentEpoch = baseEpoch;
        if(var!=null && var.getValue()!=null){
          currentEpoch = var.getValue();
        }

        LongVariable newVar = new LongVariable(dbKey, nextEpoch(currentEpoch));

        DA.setVariable(newVar);

        connector.commit();
        return currentEpoch;
      }
    };
    return (long) getAndIncrementEpochHandler.handle();
  }

  @Override
  public synchronized RMState loadState() throws Exception {
    final RMState rmState = new RMState();
    LightWeightRequestHandler loadStateHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException, IOException {
        connector.beginTransaction();
        connector.readLock();
        loadRMDTSecretManagerState(rmState);
        loadRMApps(rmState);
        loadAMRMTokenSecretManagerState(rmState);
        loadReservationSystemState(rmState);
        connector.commit();
        return null;
      }
    };
    loadStateHandler.handle();
    return rmState;
  }

  private void loadAMRMTokenSecretManagerState(RMState rmState)
          throws IOException {
    ByteArrayVariable var = (ByteArrayVariable) getVariableInt(
            Variable.Finder.AMRMToken);
    if(var==null || var.getValue()==null){
      return;
    }
    AMRMTokenSecretManagerStatePBImpl stateData
            = new AMRMTokenSecretManagerStatePBImpl(
                    YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto.
                    parseFrom((byte[])var.getValue()));
    rmState.amrmTokenSecretManagerState = AMRMTokenSecretManagerState.
            newInstance(
                    stateData.getCurrentMasterKey(),
                    stateData.getNextMasterKey());
  }

  private void loadRMApps(RMState state) throws IOException {
    ApplicationStateDataAccess DA
            = (ApplicationStateDataAccess) RMStorageFactory
            .getDataAccess(ApplicationStateDataAccess.class);
    ApplicationAttemptStateDataAccess attemptDA
            = (ApplicationAttemptStateDataAccess) RMStorageFactory
            .getDataAccess(ApplicationAttemptStateDataAccess.class);

    List<ApplicationState> appStates = DA.getAll();

    Map<String, List<ApplicationAttemptState>> applicationAttemptStates
            = attemptDA.getAll();

    if (appStates != null) {
      for (ApplicationState hopAppState : appStates) {

        ApplicationStateData appState = createApplicationState(hopAppState.
                getApplicationid(),
                hopAppState.getAppstate());
        ApplicationId appId = appState.getApplicationSubmissionContext().
                getApplicationId();

        state.appState.put(appId, appState);

        if (applicationAttemptStates.get(hopAppState.getApplicationid()) != null) {
          for (ApplicationAttemptState hopsAttemptState
                  : applicationAttemptStates.get(hopAppState.getApplicationid())) {
            ApplicationAttemptStateData attemptState = createAttemptState(
                    hopsAttemptState.getApplicationattemptid(),
                    hopsAttemptState.getApplicationattemptstate());
            appState.attempts.put(attemptState.getAttemptId(), attemptState);
          }
        }
      }
    }
  }
  
  @VisibleForTesting
  ApplicationAttemptStateData loadRMAppAttemptState(
      final ApplicationAttemptId attemptId) throws IOException {
    LightWeightRequestHandler loadStateHandler = new LightWeightRequestHandler(
        YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException, IOException {
        ApplicationAttemptStateDataAccess attemptDA
            = (ApplicationAttemptStateDataAccess) RMStorageFactory
                .getDataAccess(ApplicationAttemptStateDataAccess.class);
        return attemptDA.get(attemptId.getApplicationId().toString(), attemptId.toString());
      }
    };
    return (ApplicationAttemptStateData) loadStateHandler.handle();
  }
  
  private void loadReservationSystemState(RMState rmState) throws IOException {

    ReservationStateDataAccess DA = (ReservationStateDataAccess) RMStorageFactory.getDataAccess(
        ReservationStateDataAccess.class);

    List<ReservationState> reservationStates = DA.getAll();

    for (ReservationState state : reservationStates) {

      if (!rmState.getReservationState().containsKey(state.getPlanName())) {
        rmState.getReservationState().put(state.getPlanName(),
            new HashMap<ReservationId, YarnProtos.ReservationAllocationStateProto>());
      };
      rmState.getReservationState().get(state.getPlanName()).put(ReservationId.parseReservationId(state.
          getReservationIdName()), YarnProtos.ReservationAllocationStateProto.parseFrom(state.getState()));
    }

  }

  private ApplicationStateData createApplicationState(String appIdStr,
          byte[] data) throws IOException {
    ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
    ApplicationStateDataPBImpl appState = new ApplicationStateDataPBImpl(
            YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto.
            parseFrom(data));
    if (!appId.equals(
            appState.getApplicationSubmissionContext().getApplicationId())) {
      throw new YarnRuntimeException("The database entry for " + appId
              + " contains data for "
              + appState.getApplicationSubmissionContext().getApplicationId());
    }
    return appState;
  }

  private ApplicationAttemptStateData createAttemptState(String itemName,
          byte[] data) throws IOException {
    ApplicationAttemptId attemptId = ConverterUtils.toApplicationAttemptId(
            itemName);
    ApplicationAttemptStateDataPBImpl attemptState
            = new ApplicationAttemptStateDataPBImpl(
                    YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto.
                    parseFrom(data));
    if (!attemptId.equals(attemptState.getAttemptId())) {
      throw new YarnRuntimeException("The database entry for " + attemptId
              + " contains data for " + attemptState.getAttemptId());
    }
    return attemptState;
  }

  private void loadRMDTSecretManagerState(RMState state) throws IOException {
    int numKeys = loadRMDTSecretManagerKeys(state);
    LOG.info("Recovered " + numKeys + " RM delegation token master keys ");
    int numTokens = loadRMDTSecretManagerTokens(state);
    LOG.info("Recovered " + numTokens + " RM delegation tokens");
    loadRMDTSecretManagerTokenSequenceNumber(state);
  }

  private int loadRMDTSecretManagerKeys(RMState state) throws
          IOException {
    int numKeys = 0;
    DelegationKeyDataAccess DA = (DelegationKeyDataAccess) RMStorageFactory
            .getDataAccess(DelegationKeyDataAccess.class);
    List<io.hops.metadata.yarn.entity.rmstatestore.DelegationKey> delKeys = DA.
            getAll();
    if (delKeys != null) {
      for (io.hops.metadata.yarn.entity.rmstatestore.DelegationKey delKey
              : delKeys) {

        DelegationKey masterKey = loadDelegationKey(delKey.getDelegationkey());
        state.rmSecretManagerState.masterKeyState.add(masterKey);

        numKeys++;

      }
    }
    return numKeys;
  }

  private DelegationKey loadDelegationKey(byte[] data) throws IOException {
    DelegationKey key = new DelegationKey();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      key.readFields(in);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    return key;
  }

  private int loadRMDTSecretManagerTokens(RMState state) throws IOException {
    int numTokens = 0;
    DelegationTokenDataAccess DA = (DelegationTokenDataAccess) RMStorageFactory.
            getDataAccess(DelegationTokenDataAccess.class);
    List<DelegationToken> delTokens = DA.getAll();

    if (delTokens != null) {
      for (DelegationToken delToken : delTokens) {
        RMDelegationTokenIdentifierData tokenData = loadDelegationToken(
                delToken.getRmdtidentifier());
        RMDelegationTokenIdentifier tokenId = tokenData.getTokenIdentifier();
        long renewDate = tokenData.getRenewDate();
        state.rmSecretManagerState.delegationTokenState.put(tokenId,
                renewDate);
        ++numTokens;
      }
    }
    return numTokens;
  }

  private RMDelegationTokenIdentifierData loadDelegationToken(byte[] data)
          throws IOException {
    RMDelegationTokenIdentifierData tokenData
            = new RMDelegationTokenIdentifierData();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    try {
      tokenData.readFields(in);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    return tokenData;
  }

  private void loadRMDTSecretManagerTokenSequenceNumber(RMState state)
          throws IOException {
    IntVariable var = (IntVariable) getVariableInt(
            Variable.Finder.RMDTSequenceNumber);
    if(var!=null && var.getValue()!=null){
      state.rmSecretManagerState.dtSequenceNumber = var.getValue();
    }
  }

  @Override
  public synchronized void storeApplicationStateInternal(ApplicationId appId,
          ApplicationStateData appStateDataPB) throws Exception {
    final String appIdString = appId.toString();
    final byte[] appState = appStateDataPB.getProto().toByteArray();
    final String user = appStateDataPB.getUser();
    final String name = appStateDataPB.getApplicationSubmissionContext().
            getApplicationName();
    String stateName = null;
    if(appStateDataPB.getState()!=null){
      stateName = appStateDataPB.getState().toString();
    }
    final String stateN=stateName;
    LightWeightRequestHandler setApplicationStateHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationStateDataAccess DA
                = (ApplicationStateDataAccess) RMStorageFactory
                .getDataAccess(ApplicationStateDataAccess.class);
        ApplicationState state = new ApplicationState(appIdString, appState,
                user, name, stateN);
        DA.add(state);
        connector.commit();
        return null;
      }
    };
    setApplicationStateHandler.handle();
  }

  @Override
  public synchronized void updateApplicationStateInternal(ApplicationId appId,
          ApplicationStateData appStateDataPB) throws Exception {
    storeApplicationStateInternal(appId, appStateDataPB);
  }

  @Override
  public synchronized void storeApplicationAttemptStateInternal(
          ApplicationAttemptId appAttemptId,
          ApplicationAttemptStateData attemptStateDataPB)
          throws Exception {
    final String appId = appAttemptId.getApplicationId().toString();
    final String attemptId = appAttemptId.toString();
    final byte[] attemptData = attemptStateDataPB.getProto().toByteArray();
    final String trakingURL = attemptStateDataPB.getTrackingUrl();
    LightWeightRequestHandler setApplicationAttemptIdHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationAttemptStateDataAccess DA
                = (ApplicationAttemptStateDataAccess) RMStorageFactory.
                getDataAccess(ApplicationAttemptStateDataAccess.class);
        DA.add(
                new ApplicationAttemptState(appId, attemptId, attemptData,
                trakingURL));
        connector.commit();
        return null;
      }
    };
    setApplicationAttemptIdHandler.handle();
  }

  @Override
  public synchronized void updateApplicationAttemptStateInternal(
          ApplicationAttemptId appAttemptId,
          ApplicationAttemptStateData attemptStateDataPB)
          throws Exception {
    storeApplicationAttemptStateInternal(appAttemptId, attemptStateDataPB);
  }

  @Override
  public synchronized void removeApplicationAttemptInternal(
      ApplicationAttemptId appAttemptId)
      throws Exception {
    final String appId = appAttemptId.getApplicationId().toString();
    final String appAttemptIdString = appAttemptId.toString();
    final List<ApplicationAttemptState> attemptsToRemove
            = new ArrayList<ApplicationAttemptState>();
    attemptsToRemove.add(new ApplicationAttemptState(appId, appAttemptIdString));
    LightWeightRequestHandler removeApplicationAttemptHandler
        = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationAttemptStateDataAccess attemptDA
            = (ApplicationAttemptStateDataAccess) RMStorageFactory
                .getDataAccess(ApplicationAttemptStateDataAccess.class);
        attemptDA.removeAll(attemptsToRemove);
        connector.commit();
        return null;
      }
    };
    removeApplicationAttemptHandler.handle();
  }

  @Override
  public synchronized void removeApplicationStateInternal(
          ApplicationStateData appState)
          throws Exception {
    final String appId = appState.getApplicationSubmissionContext().
            getApplicationId()
            .toString();

    //Get ApplicationAttemptIds for this 
    final List<ApplicationAttemptState> attemptsToRemove
            = new ArrayList<ApplicationAttemptState>();
    for (ApplicationAttemptId attemptId : appState.attempts.keySet()) {
      attemptsToRemove.add(new ApplicationAttemptState(appId, attemptId.
              toString()));
    }
    //Delete applicationstate and attempts from ndb
    LightWeightRequestHandler setApplicationStateHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        if (appId != null) {
          connector.beginTransaction();
          connector.writeLock();
          ApplicationStateDataAccess DA
                  = (ApplicationStateDataAccess) RMStorageFactory
                  .getDataAccess(ApplicationStateDataAccess.class);
          //Remove this particular appState from NDB
          ApplicationState hop = new ApplicationState(appId);
          DA.remove(hop);

          //Remove attempts of this app
          ApplicationAttemptStateDataAccess attemptDA
                  = (ApplicationAttemptStateDataAccess) RMStorageFactory
                  .getDataAccess(ApplicationAttemptStateDataAccess.class);
          attemptDA.removeAll(attemptsToRemove);
          connector.commit();
        }
        return null;
      }
    };
    setApplicationStateHandler.handle();
  }

  @Override
  protected synchronized void storeRMDelegationTokenState(
          final RMDelegationTokenIdentifier rmDTIdentifier, final Long renewDate)
          throws Exception {
    storeOrUpdateRMDT(rmDTIdentifier, renewDate, false);
  }

  @Override
  protected synchronized void removeRMDelegationTokenState(
          RMDelegationTokenIdentifier rmDTIdentifier) throws Exception {
    final int seqNumber = rmDTIdentifier.getSequenceNumber();
    LightWeightRequestHandler setDelegationTokenHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        if (seqNumber != Integer.MIN_VALUE) {
          connector.beginTransaction();
          connector.writeLock();
          DelegationTokenDataAccess DA
                  = (DelegationTokenDataAccess) RMStorageFactory
                  .getDataAccess(DelegationTokenDataAccess.class);
          DelegationToken dtToRemove = new DelegationToken(seqNumber);
          DA.remove(dtToRemove);
          connector.commit();
        }
        return null;
      }
    };
    setDelegationTokenHandler.handle();
  }

  @Override
  protected synchronized void updateRMDelegationTokenState(
          RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
          throws Exception {
    storeOrUpdateRMDT(rmDTIdentifier, renewDate, true);
  }

  private void storeOrUpdateRMDT(RMDelegationTokenIdentifier tokenId,
          Long renewDate, final boolean isUpdate) throws IOException {
    final int tokenNumber = tokenId.getSequenceNumber();
    final RMDelegationTokenIdentifierData tokenData
            = new RMDelegationTokenIdentifierData(tokenId, renewDate);

    LightWeightRequestHandler setTokenAndSequenceNumberHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();
        DelegationTokenDataAccess DA
                = (DelegationTokenDataAccess) RMStorageFactory
                .getDataAccess(DelegationTokenDataAccess.class);

        DA.add(new DelegationToken(tokenNumber, tokenData.toByteArray()));
        if (!isUpdate) {
          VariableDataAccess vDA = (VariableDataAccess) RMStorageFactory
                  .getDataAccess(VariableDataAccess.class);
          vDA.setVariable(new IntVariable(Variable.Finder.RMDTSequenceNumber,
                  tokenNumber));
        }
        connector.commit();
        return null;
      }
    };
    setTokenAndSequenceNumberHandler.handle();

  }

  @Override
  protected synchronized void storeRMDTMasterKeyState(
          DelegationKey delegationKey) throws Exception {
    final int keyId = delegationKey.getKeyId();
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      DataOutputStream fsOut = new DataOutputStream(os);
      delegationKey.write(fsOut);
    } finally {
      os.close();
    }

    LightWeightRequestHandler setRMDTMasterKeyHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        DelegationKeyDataAccess DA = (DelegationKeyDataAccess) RMStorageFactory
                .getDataAccess(DelegationKeyDataAccess.class);

        DA.add(
                new io.hops.metadata.yarn.entity.rmstatestore.DelegationKey(
                        keyId, os.toByteArray()));

        connector.commit();
        return null;
      }
    };
    setRMDTMasterKeyHandler.handle();
  }

  @Override
  protected synchronized void removeRMDTMasterKeyState(
          DelegationKey delegationKey) throws Exception {
    final int key = delegationKey.getKeyId();
    LightWeightRequestHandler setRMDTMasterKeyHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        LOG.debug("HOP :: key=" + key);
        if (key != Integer.MIN_VALUE) {
          connector.beginTransaction();
          connector.writeLock();
          DelegationKeyDataAccess DA
                  = (DelegationKeyDataAccess) RMStorageFactory
                  .getDataAccess(DelegationKeyDataAccess.class);
          //Remove this particular DK from NDB
          io.hops.metadata.yarn.entity.rmstatestore.DelegationKey dkeyToremove
                  = new io.hops.metadata.yarn.entity.rmstatestore.DelegationKey(
                          key, null);
          DA.remove(dkeyToremove);
          connector.commit();
          LOG.debug("HOP :: committed");
        }
        return null;
      }
    };
    setRMDTMasterKeyHandler.handle();
  }

  @Override
  public synchronized void deleteStore() throws Exception {
    LightWeightRequestHandler deleteStoreHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationAttemptStateDataAccess appAttemptDA
                = (ApplicationAttemptStateDataAccess) RMStorageFactory.
                getDataAccess(ApplicationAttemptStateDataAccess.class);
        ApplicationStateDataAccess appDA
                = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(
                        ApplicationStateDataAccess.class);
        DelegationKeyDataAccess dkDA
                = (DelegationKeyDataAccess) RMStorageFactory.getDataAccess(
                        DelegationKeyDataAccess.class);
        DelegationTokenDataAccess dtDA
                = (DelegationTokenDataAccess) RMStorageFactory.getDataAccess(
                        DelegationTokenDataAccess.class);
        appAttemptDA.removeAll();
        appDA.removeAll();
        dkDA.removeAll();
        dtDA.removeAll();

        VariableDataAccess vDA = (VariableDataAccess) RMStorageFactory.
                getDataAccess(VariableDataAccess.class);
        vDA.setVariable(new ByteArrayVariable(Variable.Finder.AMRMToken, null));
        vDA.setVariable(new ByteArrayVariable(
                Variable.Finder.RMStateStoreVersion,
                null));
        vDA.setVariable(new LongVariable(Variable.Finder.RMStateStoreEpoch, 0));
        vDA.setVariable(new IntVariable(Variable.Finder.RMDTSequenceNumber,
                0));
        connector.commit();
        return null;
      }
    };
    deleteStoreHandler.handle();
  }

  @Override
  public synchronized void storeOrUpdateAMRMTokenSecretManagerState(
          AMRMTokenSecretManagerState state,
          boolean isUpdate)
          throws Exception {
    AMRMTokenSecretManagerState data = AMRMTokenSecretManagerState.newInstance(
            state);
    byte[] stateData = data.getProto().toByteArray();
    setVariable(new ByteArrayVariable(Variable.Finder.AMRMToken, stateData));
  }

  @VisibleForTesting
  ApplicationStateData loadRMAppState(ApplicationId appId) throws IOException {
    final String appIdString = appId.toString();

    LightWeightRequestHandler getRMAppStateHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.readLock();
        ApplicationStateDataAccess appDA
                = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(
                        ApplicationStateDataAccess.class);
        ApplicationState appState = (ApplicationState) appDA.
                findByApplicationId(appIdString);
        connector.commit();
        return appState;
      }
    };
    ApplicationState appState = (ApplicationState) getRMAppStateHandler.handle();
    
    if(appState!=null){
      return createApplicationState(appId.toString(), appState.getAppstate());
    }else{
      return null;
    }
  }

  @VisibleForTesting
  public synchronized int getNumEntriesInDatabase() throws Exception {
    LightWeightRequestHandler countEntriesHandler
            = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationAttemptStateDataAccess appAttemptDA
                = (ApplicationAttemptStateDataAccess) RMStorageFactory.
                getDataAccess(ApplicationAttemptStateDataAccess.class);
        ApplicationStateDataAccess appDA
                = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(
                        ApplicationStateDataAccess.class);
        DelegationKeyDataAccess dkDA
                = (DelegationKeyDataAccess) RMStorageFactory.getDataAccess(
                        DelegationKeyDataAccess.class);
        DelegationTokenDataAccess dtDA
                = (DelegationTokenDataAccess) RMStorageFactory.getDataAccess(
                        DelegationTokenDataAccess.class);
        int numEntries = 0;
        for(Object o : appAttemptDA.getAll().values()){
          List<ApplicationAttemptState> l = (List<ApplicationAttemptState>)o;
          numEntries+=l.size();
        }
        numEntries+= appDA.getAll().size();
        numEntries+= dkDA.getAll().size();
        numEntries+= dtDA.getAll().size();

        connector.commit();
        return numEntries;
      }
    };
    return (Integer) countEntriesHandler.handle();
  }
  
  @Override
  public void removeApplication(final ApplicationId removeAppId) throws Exception {
    LightWeightRequestHandler removeApplicationHandler = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        if (removeAppId != null) {
          connector.beginTransaction();
          connector.writeLock();
          ApplicationStateDataAccess DA = (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(ApplicationStateDataAccess.class);
          //Remove this particular appState from NDB
          ApplicationState hop = new ApplicationState(removeAppId.toString());
          DA.remove(hop);

          //Remove attempts of this app
          ApplicationAttemptStateDataAccess attemptDA
                  = (ApplicationAttemptStateDataAccess) RMStorageFactory
                  .getDataAccess(ApplicationAttemptStateDataAccess.class);
          List<ApplicationAttemptState> attemptsToRemove = attemptDA.getByAppId(removeAppId.toString());
          attemptDA.removeAll(attemptsToRemove);
          connector.commit();
        }
        return null;
      }
    };
    removeApplicationHandler.handle();
  }
  
  protected void removeReservationState(final String planName, final String reservationIdName) throws Exception{
    LightWeightRequestHandler removeReservationStateHandler = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ReservationStateDataAccess DA = (ReservationStateDataAccess) RMStorageFactory.getDataAccess(
            ReservationStateDataAccess.class);

        DA.remove(new ReservationState(planName, reservationIdName));

        connector.commit();
        return null;
      }
    };
    removeReservationStateHandler.handle();
  }
  
  protected void storeReservationState(final YarnProtos.ReservationAllocationStateProto reservationAllocation,
      final String planName, final String reservationIdName) throws Exception {

    LightWeightRequestHandler storeReservationStateHandler = new LightWeightRequestHandler(YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ReservationStateDataAccess DA = (ReservationStateDataAccess) RMStorageFactory.getDataAccess(
            ReservationStateDataAccess.class);

        DA.add(new ReservationState(reservationAllocation.toByteArray(), planName, reservationIdName));

        connector.commit();
        return null;
      }
    };
    storeReservationStateHandler.handle();
  }
  
  private long localFenceID = 0;
  
  private void checkFence() throws StorageException {
    Variable var = getVariableInt(Variable.Finder.FenceID);
    long fenceId = (long) var.getValue();
    if (fenceId != localFenceID) {
      throw new StorageException("state store fenced");
    }
  }

  private void checkFenceTransaction() throws IOException {
    LightWeightRequestHandler fenceHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        checkFence();
        return null;
        }
    };
    fenceHandler.handle();
  }

  @Override
  public void fence() throws IOException {
    LightWeightRequestHandler fenceHandler = new LightWeightRequestHandler(
            YARNOperationType.OTHER) {
      @Override
      public Object performTask() throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        LongVariable var = (LongVariable) getVariableInt(Variable.Finder.FenceID);
        long storedFenceID = 0;
        if(var.getValue()!=null){
          storedFenceID = (long)var.getValue();
        }
        localFenceID = storedFenceID +1 ;
        var = new LongVariable(Variable.Finder.FenceID, localFenceID);
        
        VariableDataAccess vDA = (VariableDataAccess) RMStorageFactory
                .getDataAccess(VariableDataAccess.class);
        vDA.setVariable(var);
        connector.commit();
        return null;
      }
    };
    fenceHandler.handle();
  }
  
  /**
   * Helper class that periodically check the fence to ensure that
   * this RM continues to be the Active.
   */
  private class VerifyActiveStatusThread extends Thread {
    VerifyActiveStatusThread() {
      super(VerifyActiveStatusThread.class.getName());
    }

    @Override
    public void run() {
      try {
        while (!isFencedState()) {
          // Create and delete fencing node
          checkFenceTransaction();
          Thread.sleep(dbSessionTimeout);
        }
      } catch (InterruptedException ie) {
        LOG.info(getName() + " thread interrupted! Exiting!");
        interrupt();
      } catch (Exception e) {
        notifyStoreOperationFailed(new StoreFencedException());
      }
    }
  }
}
