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

package org.apache.hadoop.hdfs.security.token.delegation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import org.apache.hadoop.ipc.RetriableException;

//FIXME: needs to be presisted

/**
 * A HDFS specific delegation token secret manager.
 * The secret manager is responsible for generating and accepting the password
 * for each token.
 */
@InterfaceAudience.Private
public class DelegationTokenSecretManager
    extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

  private static final Log LOG =
      LogFactory.getLog(DelegationTokenSecretManager.class);
  
  private final FSNamesystem namesystem;

  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, FSNamesystem namesystem) {
    this(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval, false,
        namesystem);
  }

  /**
   * Create a secret manager
   *
   * @param delegationKeyUpdateInterval
   *     the number of seconds for rolling new
   *     secret keys.
   * @param delegationTokenMaxLifetime
   *     the maximum lifetime of the delegation
   *     tokens
   * @param delegationTokenRenewInterval
   *     how often the tokens must be renewed
   * @param delegationTokenRemoverScanInterval
   *     how often the tokens are scanned
   *     for expired tokens
   * @param storeTokenTrackingId whether to store the token's tracking id
   */
  public DelegationTokenSecretManager(long delegationKeyUpdateInterval,
      long delegationTokenMaxLifetime, long delegationTokenRenewInterval,
      long delegationTokenRemoverScanInterval, boolean storeTokenTrackingId,
      FSNamesystem namesystem) {
    super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
        delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    this.namesystem = namesystem;
    this.storeTokenTrackingId = storeTokenTrackingId;
  }

  @Override //SecretManager
  public DelegationTokenIdentifier createIdentifier() {
    return new DelegationTokenIdentifier();
  }

  @Override
  public byte[] retriableRetrievePassword(DelegationTokenIdentifier identifier)
      throws InvalidToken, StandbyException, RetriableException, IOException {
    try {
      return super.retrievePassword(identifier);
    } catch (InvalidToken it) {
      if (namesystem.inTransitionToActive()) {
        // if the namesystem is currently in the middle of transition to 
        // active state, let client retry since the corresponding editlog may 
        // have not been applied yet
        throw new RetriableException(it);
      } else {
        throw it;
      }
    }
  }

  /**
   * Returns expiry time of a token given its identifier.
   *
   * @param dtId
   *     DelegationTokenIdentifier of a token
   * @return Expiry time of the token
   * @throws IOException
   */
  public synchronized long getTokenExpiryTime(DelegationTokenIdentifier dtId)
      throws IOException {
    DelegationTokenInformation info = currentTokens.get(dtId);
    if (info != null) {
      return info.getRenewDate();
    } else {
      throw new IOException("No delegation token found for this identifier");
    }
  }
      
  /**
   * This method is intended to be used only while reading edit logs.
   *
   * @param identifier
   *     DelegationTokenIdentifier read from the edit logs or
   *     fsimage
   * @param expiryTime
   *     token expiry time
   * @throws IOException
   */
  public synchronized void addPersistedDelegationToken(
      DelegationTokenIdentifier identifier, long expiryTime)
      throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't add persisted delegation token to a running SecretManager.");
    }
    int keyId = identifier.getMasterKeyId();
    DelegationKey dKey = allKeys.get(keyId);
    if (dKey == null) {
      LOG.warn(
          "No KEY found for persisted identifier " + identifier.toString());
      return;
    }
    byte[] password = createPassword(identifier.getBytes(), dKey.getKey());
    if (identifier.getSequenceNumber() > this.delegationTokenSequenceNumber) {
      this.delegationTokenSequenceNumber = identifier.getSequenceNumber();
    }
    if (currentTokens.get(identifier) == null) {
      currentTokens.put(identifier,
          new DelegationTokenInformation(expiryTime, password, getTrackingIdIfEnabled(identifier)));
    } else {
      throw new IOException(
          "Same delegation token being added twice; invalid entry in fsimage or editlogs");
    }
  }

  /**
   * Add a MasterKey to the list of keys.
   *
   * @param key
   *     DelegationKey
   * @throws IOException
   */
  public synchronized void updatePersistedMasterKey(DelegationKey key)
      throws IOException {
    addKey(key);
  }
  
  /**
   * Update the token cache with renewal record in edit logs.
   *
   * @param identifier
   *     DelegationTokenIdentifier of the renewed token
   * @param expiryTime
   * @throws IOException
   */
  public synchronized void updatePersistedTokenRenewal(
      DelegationTokenIdentifier identifier, long expiryTime)
      throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't update persisted delegation token renewal to a running SecretManager.");
    }
    DelegationTokenInformation info = null;
    info = currentTokens.get(identifier);
    if (info != null) {
      int keyId = identifier.getMasterKeyId();
      byte[] password =
          createPassword(identifier.getBytes(), allKeys.get(keyId).getKey());
      currentTokens.put(identifier,
          new DelegationTokenInformation(expiryTime, password, getTrackingIdIfEnabled(identifier)));
    }
  }

  /**
   * Update the token cache with the cancel record in edit logs
   *
   * @param identifier
   *     DelegationTokenIdentifier of the canceled token
   * @throws IOException
   */
  public synchronized void updatePersistedTokenCancellation(
      DelegationTokenIdentifier identifier) throws IOException {
    if (running) {
      // a safety check
      throw new IOException(
          "Can't update persisted delegation token renewal to a running SecretManager.");
    }
    currentTokens.remove(identifier);
  }
  
  /**
   * Returns the number of delegation keys currently stored.
   *
   * @return number of delegation keys
   */
  public synchronized int getNumberOfKeys() {
    return allKeys.size();
  }

  /**
   * Call namesystem to update editlogs for new master key.
   */
  @Override //AbstractDelegationTokenManager
  protected void logUpdateMasterKey(DelegationKey key) throws IOException {
    synchronized (noInterruptsLock) {
      // The edit logging code will fail catastrophically if it
      // is interrupted during a logSync, since the interrupt
      // closes the edit log files. Doing this inside the
      // above lock and then checking interruption status
      // prevents this bug.
      if (Thread.interrupted()) {
        throw new InterruptedIOException(
            "Interrupted before updating master key");
      }
      namesystem.logUpdateMasterKey(key);
    }
  }

  /**
   * A utility method for creating credentials.
   */
  public static Credentials createCredentials(final NameNode namenode,
      final UserGroupInformation ugi, final String renewer) throws IOException {
    final Token<DelegationTokenIdentifier> token =
        namenode.getRpcServer().getDelegationToken(new Text(renewer));
    if (token == null) {
      return null;
    }

    final InetSocketAddress addr = namenode.getNameNodeAddress();
    SecurityUtil.setTokenService(token, addr);
    final Credentials c = new Credentials();
    c.addToken(new Text(ugi.getShortUserName()), token);
    return c;
  }
}
