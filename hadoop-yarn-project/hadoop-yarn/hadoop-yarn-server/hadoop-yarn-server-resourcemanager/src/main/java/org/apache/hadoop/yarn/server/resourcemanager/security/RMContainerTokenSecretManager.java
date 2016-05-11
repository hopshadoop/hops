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

package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import java.util.Timer;
import java.util.TimerTask;

/**
 * SecretManager for ContainerTokens. This is RM-specific and rolls the
 * master-keys every so often.
 */
//TORECOVER TOKEN all the store action done in this class must be remplaced by transaction state actions (getStateStore)
public class RMContainerTokenSecretManager
    extends BaseContainerTokenSecretManager {

  private static final Log LOG =
      LogFactory.getLog(RMContainerTokenSecretManager.class);

  private MasterKeyData nextMasterKey;

  private final Timer timer;
  private final long rollingInterval;
  private final long activationDelay;
  private boolean stoped = true;
  private final RMContext rmContext;
  
  public RMContainerTokenSecretManager(Configuration conf,
      RMContext rmContext) {
    super(conf);
    this.rmContext = rmContext;
    this.timer = new Timer();
    this.rollingInterval = conf.getLong(
        YarnConfiguration.RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS) *
        1000;
    // Add an activation delay. This is to address the following race: RM may
    // roll over master-key, scheduling may happen at some point of time, a
    // container created with a password generated off new master key, but NM
    // might not have come again to RM to update the shared secret: so AM has a
    // valid password generated off new secret but NM doesn't know about the
    // secret yet.
    // Adding delay = 1.5 * expiry interval makes sure that all active NMs get
    // the updated shared-key.
    this.activationDelay = (long) (
        conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS) * 1.5);
    LOG.info("ContainerTokenKeyRollingInterval: " + this.rollingInterval +
        "ms and ContainerTokenKeyActivationDelay: " + this.activationDelay +
        "ms");
    if (rollingInterval <= activationDelay * 2) {
      throw new IllegalArgumentException(
          YarnConfiguration.RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS +
              " should be more than 2 X " +
              YarnConfiguration.RM_CONTAINER_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS);
    }
  }

  public void start() {
    if (!rmContext.isDistributedEnabled() || rmContext.isLeader()) {
      rollMasterKey();
      this.timer.scheduleAtFixedRate(new MasterKeyRoller(), rollingInterval,
              rollingInterval);
    }
    stoped = false;
  }

  public void stop() {
    this.timer.cancel();
    stoped = true;
  }

  /**
   * Creates a new master-key and sets it as the primary.
   */
  @Private
  public void rollMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Rolling master-key for container-tokens");
      if (this.currentMasterKey == null) { // Setting up for the first time.
        this.currentMasterKey = createNewMasterKey();
        rmContext.getStateStore()
            .storeRMTokenSecretManagerMasterKey(currentMasterKey.getMasterKey(),
                RMStateStore.KeyType.CURRENTCONTAINERTOKENMASTERKEY);
      } else {
        this.nextMasterKey = createNewMasterKey();
        rmContext.getStateStore()
            .storeRMTokenSecretManagerMasterKey(nextMasterKey.getMasterKey(),
                RMStateStore.KeyType.NEXTCONTAINERTOKENMASTERKEY);
        LOG.info("Going to activate master-key with key-id " +
            this.nextMasterKey.getMasterKey().getKeyId() + " in " +
            this.activationDelay + "ms");
        this.timer.schedule(new NextKeyActivator(), this.activationDelay);
      }
    } catch (Exception ex) {
      LOG.error(ex, ex);
    } finally {
      super.writeLock.unlock();
    }
  }

  public void setCurrentMasterKey(MasterKey currentMasterKey){
  super.writeLock.lock();
    try {
      this.currentMasterKey=new MasterKeyData(currentMasterKey,
          createSecretKey(currentMasterKey.getBytes().array()));
      LOG.info("set current master key: " + currentMasterKey.toString());
    } finally {
      super.writeLock.unlock();
    }
  }
  
  public void setNextMasterKey(MasterKey nextMasterKey){
    super.writeLock.lock();
    try {
      this.nextMasterKey = new MasterKeyData(nextMasterKey,
          createSecretKey(nextMasterKey.getBytes().array()));
    } finally {
      super.writeLock.unlock();
    }
  }
  
  @Private
  public MasterKey getNextKey() {
    super.readLock.lock();
    try {
      if (this.nextMasterKey == null) {
        return null;
      } else {
        return this.nextMasterKey.getMasterKey();
      }
    } finally {
      super.readLock.unlock();
    }
  }

  /**
   * Activate the new master-key
   */
  @Private
  public void activateNextMasterKey() {
    super.writeLock.lock();
    try {
      LOG.info("Activating next master key with id: " +
          this.nextMasterKey.getMasterKey().getKeyId());
      this.currentMasterKey = this.nextMasterKey;
      rmContext.getStateStore()
          .storeRMTokenSecretManagerMasterKey(currentMasterKey.getMasterKey(),
              RMStateStore.KeyType.CURRENTCONTAINERTOKENMASTERKEY);
      this.nextMasterKey = null;
      rmContext.getStateStore().removeRMTokenSecretManagerMasterKey(
          RMStateStore.KeyType.NEXTCONTAINERTOKENMASTERKEY);
    } catch (Exception ex) {
      LOG.error(ex);
    } finally {
      super.writeLock.unlock();
    }
  }

  private class MasterKeyRoller extends TimerTask {
    @Override
    public void run() {
      rollMasterKey();
    }
  }
  
  private class NextKeyActivator extends TimerTask {
    @Override
    public void run() {
      // Activation will happen after an absolute time interval. It will be good
      // if we can force activation after an NM updates and acknowledges a
      // roll-over. But that is only possible when we move to per-NM keys. TODO:
      activateNextMasterKey();
    }
  }

  /**
   * Helper function for creating ContainerTokens
   *
   * @param containerId
   * @param nodeId
   * @param appSubmitter
   * @param capability
   * @return the container-token
   */
  public Token createContainerToken(ContainerId containerId, NodeId nodeId,
      String appSubmitter, Resource capability) {
    byte[] password;
    ContainerTokenIdentifier tokenIdentifier;
    long expiryTimeStamp =
        System.currentTimeMillis() + containerTokenExpiryInterval;

    // Lock so that we use the same MasterKey's keyId and its bytes
    this.readLock.lock();
    try {
      tokenIdentifier =
          new ContainerTokenIdentifier(containerId, nodeId.toString(),
              appSubmitter, capability, expiryTimeStamp,
              this.currentMasterKey.getMasterKey().getKeyId(),
              ResourceManager.getClusterTimeStamp());
      password = this.createPassword(tokenIdentifier);

    } finally {
      this.readLock.unlock();
    }

    return BuilderUtils.newContainerToken(nodeId, password, tokenIdentifier);
  }

  public boolean isStoped() {
    return stoped;
  }

  public void recover(RMStateStore.RMState state) {
    MasterKey recoverKey = state.getSecretTokenMamagerKey(
        RMStateStore.KeyType.CURRENTCONTAINERTOKENMASTERKEY);
    if (recoverKey != null) {
      this.currentMasterKey = new MasterKeyData(recoverKey,
          createSecretKey(recoverKey.getBytes().array()));
    }
    recoverKey = state.getSecretTokenMamagerKey(
        RMStateStore.KeyType.NEXTCONTAINERTOKENMASTERKEY);
    if (recoverKey != null) {
      this.nextMasterKey = new MasterKeyData(recoverKey,
          createSecretKey(recoverKey.getBytes().array()));
    }
  }
}