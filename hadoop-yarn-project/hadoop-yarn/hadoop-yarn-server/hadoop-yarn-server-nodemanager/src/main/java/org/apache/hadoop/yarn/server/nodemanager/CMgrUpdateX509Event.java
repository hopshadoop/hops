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
package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.yarn.api.records.ContainerId;

import java.nio.ByteBuffer;

public class CMgrUpdateX509Event extends CMgrUpdateSecurityMaterialEvent {
  private final ByteBuffer keyStore;
  private final char[] keyStorePassword;
  private final ByteBuffer trustStore;
  private final char[] trustStorePassword;
  private final int version;
  
  public CMgrUpdateX509Event(ContainerId containerId, ByteBuffer keyStore, char[] keyStorePassword,
      ByteBuffer trustStore, char[] trustStorePassword, int version) {
    super(containerId);
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
    this.version = version;
  }
  
  public ByteBuffer getKeyStore() {
    return keyStore.asReadOnlyBuffer();
  }
  
  public char[] getKeyStorePassword() {
    return keyStorePassword;
  }
  
  public ByteBuffer getTrustStore() {
    return trustStore.asReadOnlyBuffer();
  }
  
  public char[] getTrustStorePassword() {
    return trustStorePassword;
  }
  
  public int getVersion() {
    return version;
  }
}
