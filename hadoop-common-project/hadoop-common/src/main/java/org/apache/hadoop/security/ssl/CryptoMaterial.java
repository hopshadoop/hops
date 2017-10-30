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
package org.apache.hadoop.security.ssl;

import java.nio.ByteBuffer;

public class CryptoMaterial {
  private final String keyStoreLocation;
  private final int keyStoreSize;
  private final String trustStoreLocation;
  private final String passwdLocation;
  private final int trustStoreSize;
  private final ByteBuffer keyStoreMem;
  private final String keyStorePass;
  private final ByteBuffer trustStoreMem;
  private final String trustStorePass;
  
  // Number of applications using the same crypto material
  // The same user might have multiple applications running
  // at the same time
  private int requestedApplications;
  
  public CryptoMaterial(String keyStoreLocation, String trustStoreLocation,
      String passwdLocation, ByteBuffer kStore, String kStorePass,
      ByteBuffer tstore, String tstorePass) {
    this.keyStoreLocation = keyStoreLocation;
    this.keyStoreSize = kStore.capacity();
    this.trustStoreLocation = trustStoreLocation;
    this.trustStoreSize = tstore.capacity();
    this.passwdLocation = passwdLocation;
    
    this.keyStoreMem = kStore.asReadOnlyBuffer();
    this.keyStorePass = kStorePass;
    this.trustStoreMem = tstore.asReadOnlyBuffer();
    this.trustStorePass = tstorePass;
    
    requestedApplications = 1;
  }
  
  public String getKeyStoreLocation() {
    return keyStoreLocation;
  }
  
  public int getKeyStoreSize() {
    return keyStoreSize;
  }
  
  public String getTrustStoreLocation() {
    return trustStoreLocation;
  }
  
  public String getPasswdLocation() {
    return passwdLocation;
  }
  
  public int getTrustStoreSize() {
    return trustStoreSize;
  }
  
  public ByteBuffer getKeyStoreMem() {
    return keyStoreMem;
  }
  
  public String getKeyStorePass() {
    return keyStorePass;
  }
  
  public ByteBuffer getTrustStoreMem() {
    return trustStoreMem;
  }
  
  public String getTrustStorePass() {
    return trustStorePass;
  }
  
  public int getRequestedApplications() {
    return requestedApplications;
  }
  
  public void incrementRequestedApplications() {
    requestedApplications++;
  }
  
  public void decrementRequestedApplications() {
    requestedApplications--;
  }
  
  public boolean isSafeToRemove() {
    return requestedApplications == 0;
  }
}
