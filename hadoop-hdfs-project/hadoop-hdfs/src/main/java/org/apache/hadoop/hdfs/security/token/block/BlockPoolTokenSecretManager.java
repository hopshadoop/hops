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
package org.apache.hadoop.hdfs.security.token.block;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages a {@link BlockTokenSecretManager} per block pool. Routes the
 * requests
 * given a block pool Id to corresponding {@link BlockTokenSecretManager}
 */
public class BlockPoolTokenSecretManager
    extends SecretManager<BlockTokenIdentifier> {
  
  private final Map<String, BlockTokenSecretManager> map =
      new HashMap<>();

  /**
   * Add a block pool Id and corresponding {@link BlockTokenSecretManager} to
   * map
   *
   * @param bpid
   *     block pool Id
   * @param secretMgr
   *     {@link BlockTokenSecretManager}
   */
  public synchronized void addBlockPool(String bpid,
      BlockTokenSecretManager secretMgr) {
    map.put(bpid, secretMgr);
  }

  synchronized BlockTokenSecretManager get(String bpid) {
    BlockTokenSecretManager secretMgr = map.get(bpid);
    if (secretMgr == null) {
      throw new IllegalArgumentException(
          "Block pool " + bpid + " is not found");
    }
    return secretMgr;
  }
  
  public synchronized boolean isBlockPoolRegistered(String bpid) {
    return map.containsKey(bpid);
  }

  /**
   * Return an empty BlockTokenIdentifer
   */
  @Override
  public BlockTokenIdentifier createIdentifier() {
    return new BlockTokenIdentifier();
  }

  @Override
  public byte[] createPassword(BlockTokenIdentifier identifier) {
    return get(identifier.getBlockPoolId()).createPassword(identifier);
  }

  @Override
  public byte[] retrievePassword(BlockTokenIdentifier identifier)
      throws InvalidToken {
    return get(identifier.getBlockPoolId()).retrievePassword(identifier);
  }

  /**
   * See {@link BlockTokenSecretManager#checkAccess(BlockTokenIdentifier,
   * String, ExtendedBlock, AccessMode)}
   */
  public void checkAccess(BlockTokenIdentifier id, String userId,
      ExtendedBlock block, AccessMode mode) throws InvalidToken {
    get(block.getBlockPoolId()).checkAccess(id, userId, block, mode);
  }

  /**
   * See {@link BlockTokenSecretManager#checkAccess(Token, String,
   * ExtendedBlock, AccessMode)}
   */
  public void checkAccess(Token<BlockTokenIdentifier> token, String userId,
      ExtendedBlock block, AccessMode mode) throws InvalidToken {
    get(block.getBlockPoolId()).checkAccess(token, userId, block, mode);
  }

  /**
   * See {@link BlockTokenSecretManager#addKeys(ExportedBlockKeys)}
   */
  public void addKeys(String bpid, ExportedBlockKeys exportedKeys)
      throws IOException {
    get(bpid).addKeys(exportedKeys);
  }

  /**
   * See {@link BlockTokenSecretManager#generateToken(ExtendedBlock, EnumSet)}
   */
  public Token<BlockTokenIdentifier> generateToken(ExtendedBlock b,
      EnumSet<AccessMode> of) throws IOException {
    return get(b.getBlockPoolId()).generateToken(b, of);
  }
  
  @VisibleForTesting
  public void clearAllKeysForTesting() {
    for (BlockTokenSecretManager btsm : map.values()) {
      btsm.clearAllKeysForTesting();
    }
  }

  public DataEncryptionKey generateDataEncryptionKey(String blockPoolId)
      throws IOException {
    return get(blockPoolId).generateDataEncryptionKey();
  }
  
  public byte[] retrieveDataEncryptionKey(int keyId, String blockPoolId,
      byte[] nonce) throws IOException {
    return get(blockPoolId).retrieveDataEncryptionKey(keyId, nonce);
  }
}
