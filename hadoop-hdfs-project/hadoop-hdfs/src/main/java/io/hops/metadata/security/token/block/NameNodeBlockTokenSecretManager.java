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
package io.hops.metadata.security.token.block;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.common.entity.Variable;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes.LockType;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Map;

/**
 * Persisted version of the BlockTokenSecretManager to be used by the NameNode
 * We add persistence by overriding only the methods used by the NameNode
 */
public class NameNodeBlockTokenSecretManager extends BlockTokenSecretManager {

  private Namesystem namesystem;

  /**
   * Constructor for masters.
   */
  public NameNodeBlockTokenSecretManager(long keyUpdateInterval,
      long tokenLifetime, String blockPoolId, String encryptionAlgorithm,
      Namesystem namesystem) throws IOException {
    super(true, keyUpdateInterval, tokenLifetime, blockPoolId,
        encryptionAlgorithm);
    this.namesystem = namesystem;
    this.setSerialNo(new SecureRandom().nextInt());
    if (isLeader()) {
      // TODO[Hooman]: Since Master is keeping the serialNo locally, so whenever
      // A namenode crashes it should remove all keys from the database.
      this.generateKeys();
    } else {
      retrieveBlockKeys();
    }
  }

  @Override
  public void setSerialNo(int serialNo) {
    this.serialNo = serialNo;
  }

  private void generateKeys() throws IOException {
    if (!isMaster) {
      return;
    }
    /*
     * Need to set estimated expiry dates for currentKey and nextKey so that if
     * NN crashes, DN can still expire those keys. NN will stop using the newly
     * generated currentKey after the first keyUpdateInterval, however it may
     * still be used by DN and Balancer to generate new tokens before they get a
     * chance to sync their keys with NN. Since we require keyUpdInterval to be
     * long enough so that all live DN's and Balancer will sync their keys with
     * NN at least once during the period, the estimated expiry date for
     * currentKey is set to now() + 2 * keyUpdateInterval + tokenLifetime.
     * Similarly, the estimated expiry date for nextKey is one keyUpdateInterval
     * more.
     */
    setSerialNo(serialNo + 1);
    currentKey = new BlockKey(serialNo,
        Time.now() + 2 * keyUpdateInterval + tokenLifetime, generateSecret());
    currentKey.setKeyType(BlockKey.KeyType.CurrKey);
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo,
        Time.now() + 3 * keyUpdateInterval + tokenLifetime, generateSecret());
    nextKey.setKeyType(BlockKey.KeyType.NextKey);
    addBlockKeys();
  }

  @Override
  public ExportedBlockKeys exportKeys() throws IOException {
    if (!isMaster) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exporting access keys");
    }
    BlockKey[] allKeys = getAllKeysAndSync();
    return new ExportedBlockKeys(true, keyUpdateInterval, tokenLifetime,
        currentKey, allKeys);

  }

  @Override
  public boolean updateKeys(final long updateTime) throws IOException {
    if (updateTime > keyUpdateInterval) {
      return updateKeys();
    }
    return false;
  }

  @Override
  public boolean updateKeys() throws IOException {
    if (!isMaster) {
      return false;
    }
    if (isLeader()) {
      LOG.info("Updating block keys");
      return updateBlockKeys();
    } else {
      retrieveBlockKeys();
    }
    return true;
  }

  @Override
  public DataEncryptionKey generateDataEncryptionKey() throws IOException {
    byte[] nonce = new byte[8];
    nonceGenerator.nextBytes(nonce);
    BlockKey key = getBlockKeyByType(BlockKey.KeyType.CurrKey);

    byte[] encryptionKey = createPassword(nonce, key.getKey());
    return new DataEncryptionKey(key.getKeyId(), blockPoolId, nonce,
        encryptionKey, Time.now() + tokenLifetime, encryptionAlgorithm);
  }

  @Override
  protected byte[] createPassword(BlockTokenIdentifier identifier) {
    BlockKey key;
    try {
      key = getBlockKeyByType(BlockKey.KeyType.CurrKey);
    } catch (IOException ex) {
      throw new IllegalStateException(
          "currentKey hasn't been initialized. [" + ex.getMessage() + "]");
    }
    if (key == null) {
      throw new IllegalStateException("currentKey hasn't been initialized.");
    }
    identifier.setExpiryDate(Time.now() + tokenLifetime);
    identifier.setKeyId(key.getKeyId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generating block token for " + identifier.toString());
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }

  @Override
  public byte[] retrievePassword(BlockTokenIdentifier identifier)
      throws InvalidToken {
    if (isExpired(identifier.getExpiryDate())) {
      throw new InvalidToken(
          "Block token with " + identifier.toString() + " is expired.");
    }
    BlockKey key = null;
    try {
      key = getBlockKeyById(identifier.getKeyId());
    } catch (IOException ex) {
    }

    if (key == null) {
      throw new InvalidToken(
          "Can't re-compute password for " + identifier.toString() +
              ", since the required block key (keyID=" + identifier.getKeyId() +
              ") doesn't exist.");
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }

  public void generateKeysIfNeeded() throws IOException {
    if (isLeader()) {
      retrieveBlockKeys();
      if (currentKey == null && nextKey == null) {
        generateKeys();
      }
    }
  }

  private void retrieveBlockKeys() throws IOException {
    currentKey = getBlockKeyByType(BlockKey.KeyType.CurrKey);
    nextKey = getBlockKeyByType(BlockKey.KeyType.NextKey);
  }

  private void addBlockKeys() throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.ADD_BLOCK_TOKENS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getVariableLock(Variable.Finder.BlockTokenKeys, LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        HdfsVariables.updateBlockTokenKeys(currentKey, nextKey);
        return null;
      }
    }.handle();
  }

  private BlockKey getBlockKeyById(int keyId) throws IOException {
    return HdfsVariables.getAllBlockTokenKeysByIDLW().get(keyId);
  }

  private BlockKey getBlockKeyByType(BlockKey.KeyType keytype)
      throws IOException {
    return HdfsVariables.getAllBlockTokenKeysByTypeLW().get(keytype.ordinal());
  }

  private BlockKey[] getAllKeysAndSync() throws IOException {
    BlockKey[] allKeysArr = null;
    Collection<BlockKey> allKeys = getAllKeys();
    if (allKeys != null) {
      for (BlockKey key : allKeys) {
        if (key.isCurrKey()) {
          currentKey = key;
        } else if (key.isNextKey()) {
          nextKey = key;
        }
      }
      allKeysArr = allKeys.toArray(new BlockKey[allKeys.size()]);
    }
    return allKeysArr;
  }

  private Collection<BlockKey> getAllKeys() throws IOException {
    return HdfsVariables.getAllBlockTokenKeysByIDLW().values();
  }

  private boolean updateBlockKeys() throws IOException {
    return (Boolean) new HopsTransactionalRequestHandler(
        HDFSOperationType.UPDATE_BLOCK_KEYS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(
            lf.getVariableLock(Variable.Finder.BlockTokenKeys, LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        Map<Integer, BlockKey> keys =
            HdfsVariables.getAllBlockTokenKeysByType();
        if (keys.isEmpty()) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("keys is not generated yet to be updated");
          }
          return false;
        }
        // set final expiry date of retiring currentKey
        // also modifying this key to mark it as 'simple key' instead of 'current key'
        BlockKey currentKeyFromDB =
            keys.get(BlockKey.KeyType.CurrKey.ordinal());
        currentKeyFromDB
            .setExpiryDate(Time.now() + keyUpdateInterval + tokenLifetime);
        currentKeyFromDB.setKeyType(BlockKey.KeyType.SimpleKey);

        // after above update, we only have a key marked as 'next key'
        // the 'next key' becomes the 'current key'
        // update the estimated expiry date of new currentKey
        BlockKey nextKeyFromDB = keys.get(BlockKey.KeyType.NextKey.ordinal());
        currentKey = new BlockKey(nextKeyFromDB.getKeyId(),
            Time.now() + 2 * keyUpdateInterval + tokenLifetime,
            nextKeyFromDB.getKey());
        currentKey.setKeyType(BlockKey.KeyType.CurrKey);

        // generate a new nextKey
        setSerialNo(serialNo + 1);
        nextKey = new BlockKey(serialNo,
            Time.now() + 3 * keyUpdateInterval + tokenLifetime,
            generateSecret());
        nextKey.setKeyType(BlockKey.KeyType.NextKey);

        HdfsVariables
            .updateBlockTokenKeys(currentKey, nextKey, currentKeyFromDB);
        return true;
      }
    }.handle();
  }
  
  private boolean isLeader() {
    if (namesystem != null) {
      return namesystem.isLeader();
    }
    return false;
  }
}
