/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.security.token.block;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import javax.crypto.SecretKey;

/**
 * Key used for generating and verifying block tokens
 */
@InterfaceAudience.Private
public class BlockKey extends DelegationKey {

  public static enum KeyType {
    CurrKey,
    NextKey,
    SimpleKey
  }

  ;
  
  private KeyType keyType;

  
  public BlockKey() {
    super();
  }

  public BlockKey(int keyId, long expiryDate, SecretKey key, KeyType keyType) {
    super(keyId, expiryDate, key);
    this.keyType = keyType;
  }
  
  public BlockKey(int keyId, long expiryDate, SecretKey key) {
    super(keyId, expiryDate, key);
  }

  public BlockKey(int keyId, long expiryDate, byte[] encodedKey) {
    super(keyId, expiryDate, encodedKey);
  }

  public void setKeyType(KeyType keyType) {
    if (keyType == KeyType.CurrKey || keyType == KeyType.NextKey ||
        keyType == KeyType.SimpleKey) {
      this.keyType = keyType;
    } else {
      throw new IllegalArgumentException("Wrong key type " + keyType);
    }
  }

  public KeyType getKeyType() {
    return keyType;
  }

  public boolean isCurrKey() {
    return this.keyType == KeyType.CurrKey;
  }

  public boolean isNextKey() {
    return this.keyType == KeyType.NextKey;
  }

  public boolean isSimpleKey() {
    return this.keyType == KeyType.SimpleKey;
  }

}
