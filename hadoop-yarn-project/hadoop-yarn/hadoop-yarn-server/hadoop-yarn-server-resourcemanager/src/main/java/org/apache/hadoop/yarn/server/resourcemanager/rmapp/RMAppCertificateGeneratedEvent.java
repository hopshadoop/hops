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
package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class RMAppCertificateGeneratedEvent extends RMAppEvent {
  private final byte[] keyStore;
  private final char[] keyStorePassword;
  private final byte[] trustStore;
  private final char[] trustStorePassword;
  private final long expirationEpoch;
  
  public RMAppCertificateGeneratedEvent(ApplicationId appId, byte[] keyStore, char[] keyStorePassword,
      byte[] trustStore, char[] trustStorePassword, long expirationEpoch, RMAppEventType type) {
    super(appId, type);
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
    this.expirationEpoch = expirationEpoch;
  }
  
  public byte[] getKeyStore() {
    return keyStore;
  }
  
  public char[] getKeyStorePassword() {
    return keyStorePassword;
  }
  
  public byte[] getTrustStore() {
    return trustStore;
  }
  
  public char[] getTrustStorePassword() {
    return trustStorePassword;
  }
  
  public long getExpirationEpoch() {
    return expirationEpoch;
  }
}
