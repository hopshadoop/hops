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
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.ByteArrayVariable;
import io.hops.metadata.common.entity.IntVariable;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
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
          .newInstance(1, 2);

  @Override
  public synchronized void initInternal(Configuration conf) throws Exception {
  }

  @Override
  public synchronized void startInternal() throws Exception {
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
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
    LightWeightRequestHandler setVersionHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        VariableDataAccess vDA = (VariableDataAccess)
            RMStorageFactory.getDataAccess(connector, VariableDataAccess.class);
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

  private Variable getVariableInt(StorageConnector connector, Variable.Finder finder) throws
          StorageException {
    VariableDataAccess DA = (VariableDataAccess)
        RMStorageFactory.getDataAccess(connector, VariableDataAccess.class);
    return (Variable) DA.getVariable(finder);

  }

  private Variable getVariable(final Variable.Finder finder) throws IOException {
    LightWeightRequestHandler getVersionHandler = new LightWeightRequestHandler(
            YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.readCommitted();
        Variable var = getVariableInt(connector, finder);

        connector.commit();
        return var;
      }
    };
    return (Variable) getVersionHandler.handle();
  }

  @Override
  public synchronized long getAndIncrementEpoch() throws Exception {
    final Variable.Finder dbKey = Variable.Finder.RMStateStoreEpoch;

    LightWeightRequestHandler getAndIncrementEpochHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        VariableDataAccess DA
                = (VariableDataAccess) RMStorageFactory.getDataAccess(connector, VariableDataAccess.class);
        LongVariable var = (LongVariable) DA.getVariable(dbKey);

        long currentEpoch = 0;
        if(var!=null && var.getValue()!=null){
          currentEpoch = var.getValue();
        }

        LongVariable newVar = new LongVariable(dbKey, currentEpoch + 1);

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
            YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException, IOException {
        connector.beginTransaction();
        connector.readLock();
        loadRMDTSecretManagerState(connector, rmState);
        loadRMApps(connector, rmState);
        loadAMRMTokenSecretManagerState(connector, rmState);
        connector.commit();
        return null;
      }
    };
    loadStateHandler.handle();
    return rmState;
  }

  private void loadAMRMTokenSecretManagerState(StorageConnector connector, RMState rmState)
          throws IOException {
    ByteArrayVariable var = (ByteArrayVariable) getVariableInt(connector, Variable.Finder.AMRMToken);
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

  private void loadRMApps(StorageConnector connector, RMState state) throws IOException {
    ApplicationStateDataAccess DA = (ApplicationStateDataAccess)
        RMStorageFactory.getDataAccess(connector, ApplicationStateDataAccess.class);
    ApplicationAttemptStateDataAccess attemptDA = (ApplicationAttemptStateDataAccess)
        RMStorageFactory.getDataAccess(connector, ApplicationAttemptStateDataAccess.class);
    List<ApplicationState> appStates = DA.getAll();
    Map<String, List<ApplicationAttemptState>> applicationAttemptStates = attemptDA.getAll();

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

  private void loadRMDTSecretManagerState(StorageConnector connector, RMState state) throws IOException {
    int numKeys = loadRMDTSecretManagerKeys(connector, state);
    LOG.info("Recovered " + numKeys + " RM delegation token master keys ");
    int numTokens = loadRMDTSecretManagerTokens(connector, state);
    LOG.info("Recovered " + numTokens + " RM delegation tokens");
    loadRMDTSecretManagerTokenSequenceNumber(connector, state);
  }

  private int loadRMDTSecretManagerKeys(StorageConnector connector, RMState state) throws
          IOException {
    int numKeys = 0;
    DelegationKeyDataAccess DA = (DelegationKeyDataAccess)
        RMStorageFactory.getDataAccess(connector, DelegationKeyDataAccess.class);
    List<io.hops.metadata.yarn.entity.rmstatestore.DelegationKey> delKeys = DA.getAll();
    if (delKeys != null) {
      for (io.hops.metadata.yarn.entity.rmstatestore.DelegationKey delKey : delKeys) {
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

  private int loadRMDTSecretManagerTokens(StorageConnector connector, RMState state) throws IOException {
    int numTokens = 0;
    DelegationTokenDataAccess DA = (DelegationTokenDataAccess)
        RMStorageFactory.getDataAccess(connector, DelegationTokenDataAccess.class);
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

  private void loadRMDTSecretManagerTokenSequenceNumber(StorageConnector connector, RMState state)
          throws IOException {
    IntVariable var = (IntVariable) getVariableInt(connector, Variable.Finder.RMDTSequenceNumber);
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
    LightWeightRequestHandler setApplicationStateHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationStateDataAccess DA = (ApplicationStateDataAccess)
            RMStorageFactory.getDataAccess(connector, ApplicationStateDataAccess.class);
        ApplicationState state = new ApplicationState(appIdString, appState, user, name, stateN);
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
    LightWeightRequestHandler setApplicationAttemptIdHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationAttemptStateDataAccess DA = (ApplicationAttemptStateDataAccess)
            RMStorageFactory.getDataAccess(connector, ApplicationAttemptStateDataAccess.class);
        DA.add(new ApplicationAttemptState(appId, attemptId, attemptData, trakingURL));
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
  public synchronized void removeApplicationStateInternal(
          ApplicationStateData appState)
          throws Exception {
    final String appId = appState.getApplicationSubmissionContext().
            getApplicationId()
            .toString();

    //Get ApplicationAttemptIds for this 
    final List<ApplicationAttemptState> attemptsToRemove
            = new ArrayList<>();
    for (ApplicationAttemptId attemptId : appState.attempts.keySet()) {
      attemptsToRemove.add(new ApplicationAttemptState(appId, attemptId.
              toString()));
    }
    //Delete applicationstate and attempts from ndb
    LightWeightRequestHandler setApplicationStateHandler
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        if (appId != null) {
          connector.beginTransaction();
          connector.writeLock();
          ApplicationStateDataAccess DA = (ApplicationStateDataAccess)
              RMStorageFactory.getDataAccess(connector, ApplicationStateDataAccess.class);
          //Remove this particular appState from NDB
          ApplicationState hop = new ApplicationState(appId);
          DA.remove(hop);

          //Remove attempts of this app
          ApplicationAttemptStateDataAccess attemptDA = (ApplicationAttemptStateDataAccess)
              RMStorageFactory.getDataAccess(connector, ApplicationAttemptStateDataAccess.class);
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
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws IOException {
        if (seqNumber != Integer.MIN_VALUE) {
          connector.beginTransaction();
          connector.writeLock();
          DelegationTokenDataAccess DA = (DelegationTokenDataAccess)
              RMStorageFactory.getDataAccess(connector, DelegationTokenDataAccess.class);
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
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws IOException {
        connector.beginTransaction();
        connector.writeLock();
        DelegationTokenDataAccess DA = (DelegationTokenDataAccess)
            RMStorageFactory.getDataAccess(connector, DelegationTokenDataAccess.class);

        DA.add(new DelegationToken(tokenNumber, tokenData.toByteArray()));
        if (!isUpdate) {
          VariableDataAccess vDA = (VariableDataAccess)
              RMStorageFactory.getDataAccess(connector, VariableDataAccess.class);
          vDA.setVariable(new IntVariable(Variable.Finder.RMDTSequenceNumber, tokenNumber));
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
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        DelegationKeyDataAccess DA = (DelegationKeyDataAccess)
            RMStorageFactory.getDataAccess(connector, DelegationKeyDataAccess.class);
        DA.add(new io.hops.metadata.yarn.entity.rmstatestore.DelegationKey(keyId, os.toByteArray()));
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
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        LOG.debug("HOP :: key=" + key);
        if (key != Integer.MIN_VALUE) {
          connector.beginTransaction();
          connector.writeLock();
          DelegationKeyDataAccess DA = (DelegationKeyDataAccess)
              RMStorageFactory.getDataAccess(connector, DelegationKeyDataAccess.class);
          //Remove this particular DK from NDB
          io.hops.metadata.yarn.entity.rmstatestore.DelegationKey dkeyToremove
                  = new io.hops.metadata.yarn.entity.rmstatestore.DelegationKey(key, null);
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
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationAttemptStateDataAccess appAttemptDA = (ApplicationAttemptStateDataAccess)
            RMStorageFactory.getDataAccess(connector, ApplicationAttemptStateDataAccess.class);
        ApplicationStateDataAccess appDA = (ApplicationStateDataAccess)
            RMStorageFactory.getDataAccess(connector, ApplicationStateDataAccess.class);
        DelegationKeyDataAccess dkDA = (DelegationKeyDataAccess)
            RMStorageFactory.getDataAccess(connector, DelegationKeyDataAccess.class);
        DelegationTokenDataAccess dtDA = (DelegationTokenDataAccess)
            RMStorageFactory.getDataAccess(connector, DelegationTokenDataAccess.class);
        appAttemptDA.removeAll();
        appDA.removeAll();
        dkDA.removeAll();
        dtDA.removeAll();

        VariableDataAccess vDA = (VariableDataAccess)
            RMStorageFactory.getDataAccess(connector, VariableDataAccess.class);
        vDA.setVariable(new ByteArrayVariable(Variable.Finder.AMRMToken, null));
        vDA.setVariable(new ByteArrayVariable(Variable.Finder.RMStateStoreVersion, null));
        vDA.setVariable(new LongVariable(Variable.Finder.RMStateStoreEpoch, 0));
        vDA.setVariable(new IntVariable(Variable.Finder.RMDTSequenceNumber, 0));
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
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.readLock();
        ApplicationStateDataAccess appDA =
            (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(connector, ApplicationStateDataAccess.class);
        ApplicationState appState = (ApplicationState) appDA.findByApplicationId(appIdString);
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
    LightWeightRequestHandler countEntriesHandler = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask(StorageConnector connector) throws StorageException {
        connector.beginTransaction();
        connector.writeLock();
        ApplicationAttemptStateDataAccess appAttemptDA =
            (ApplicationAttemptStateDataAccess) RMStorageFactory.getDataAccess(connector, ApplicationAttemptStateDataAccess.class);
        ApplicationStateDataAccess appDA =
            (ApplicationStateDataAccess) RMStorageFactory.getDataAccess(connector, ApplicationStateDataAccess.class);
        DelegationKeyDataAccess dkDA =
            (DelegationKeyDataAccess) RMStorageFactory.getDataAccess(connector, DelegationKeyDataAccess.class);
        DelegationTokenDataAccess dtDA =
            (DelegationTokenDataAccess) RMStorageFactory.getDataAccess(connector, DelegationTokenDataAccess.class);
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
}
