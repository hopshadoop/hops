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
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.hadoop.util.Time;

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
  }

  @Override
  public synchronized boolean updateKeys() throws IOException {
    if (!isMaster) {
      return false;
    }
    if (isLeader()) {
      LOG.info("Updating block keys");
      return updateBlockKeys();
    } else {
      getAllKeysAndSync();
      removeExpiredKeys();
    }
    return true;
  }

  protected boolean updateKeysParent() throws IOException {
    return super.updateKeys();
  }

  public void initKeys() throws IOException {
    while (true) {
      if (isLeader()) {
        /*
         * If the NN is leader at this point it means that the all cluster was restarted
         * the NN should then write its newly generated keys to the database
         */
        addBlockKeys();
        return;
      } else {
        /*
         * if the NN is not the leader it try to get keys until the leader as persisted keys up to date neough to
         * the database
         */
        getAllKeysAndSync();
        if (currentKey.getExpiryDate() > Time.now() + 2 * keyUpdateInterval + tokenLifetime) {
          return;
        }
      }
    }
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

  private synchronized void getAllKeysAndSync() throws IOException {
    Collection<BlockKey> storedKeys = getAllKeys();
    if (storedKeys != null) {
      for (BlockKey key : storedKeys) {
        allKeys.put(key.getKeyId(), key);
        if (key.isCurrKey()) {
          currentKey = key;
        } else if (key.isNextKey()) {
          nextKey = key;
        }
      }
    }
  }

  private Collection<BlockKey> getAllKeys() throws IOException {
    return HdfsVariables.getAllBlockTokenKeysByIDLW().values();
  }

  private boolean updateBlockKeys() throws IOException {
    return (Boolean) new HopsTransactionalRequestHandler(HDFSOperationType.UPDATE_BLOCK_KEYS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getVariableLock(Variable.Finder.BlockTokenKeys, LockType.WRITE));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        Map<Integer, BlockKey> keys = HdfsVariables.getAllBlockTokenKeysByType();
        if (keys.isEmpty()) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("keys is not generated yet to be updated");
          }
          return false;
        }
        currentKey = keys.get(BlockKey.KeyType.CurrKey.ordinal());
        nextKey = keys.get(BlockKey.KeyType.NextKey.ordinal());
        int previousKeyId = currentKey.getKeyId();
        
        updateKeysParent();
        
        HdfsVariables.updateBlockTokenKeys(currentKey, nextKey, allKeys.get(previousKeyId));
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
