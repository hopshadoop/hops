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
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class CryptoMaterial {
  public enum STATE {
    NEW,
    ONGOING,
    FINISHED
  }
  
  private final Path certFolder;
  private final Path keyStoreLocation;
  private int keyStoreSize;
  private final Path trustStoreLocation;
  private final Path passwdLocation;
  private final int trustStoreSize;
  private ByteBuffer keyStoreMem;
  private String keyStorePass;
  private ByteBuffer trustStoreMem;
  private String trustStorePass;
  private final AtomicBoolean tombstone;
  
  private STATE state;
  
  // Number of applications using the same crypto material
  // The same user might have multiple applications running
  // at the same time
  private int requestedApplications;
  
  public CryptoMaterial(Path certFolder, Path keyStoreLocation, Path trustStoreLocation,
      Path passwdLocation, ByteBuffer kStore, String kStorePass,
      ByteBuffer tstore, String tstorePass) {
    this.certFolder = certFolder;
    this.keyStoreLocation = keyStoreLocation;
    this.keyStoreSize = kStore.capacity();
    this.trustStoreLocation = trustStoreLocation;
    this.trustStoreSize = tstore.capacity();
    this.passwdLocation = passwdLocation;
    
    this.keyStoreMem = kStore;
    this.keyStorePass = kStorePass;
    this.trustStoreMem = tstore;
    this.trustStorePass = tstorePass;
    
    requestedApplications = 1;
    
    state = STATE.NEW;
    tombstone = new AtomicBoolean(false);
  }

  public Path getCertFolder() { return certFolder; }
  
  public Path getKeyStoreLocation() {
    return keyStoreLocation;
  }
  
  public synchronized int getKeyStoreSize() {
    return keyStoreSize;
  }
  
  public Path getTrustStoreLocation() {
    return trustStoreLocation;
  }
  
  public Path getPasswdLocation() {
    return passwdLocation;
  }
  
  public int getTrustStoreSize() {
    return trustStoreSize;
  }
  
  public synchronized ByteBuffer getKeyStoreMem() {
    return keyStoreMem.asReadOnlyBuffer();
  }
  
  public synchronized void updateKeyStoreMem(ByteBuffer keyStoreMem) {
    this.keyStoreMem = keyStoreMem;
  }
  
  public synchronized String getKeyStorePass() {
    return keyStorePass;
  }
  
  public synchronized void updateKeyStorePass(String keyStorePass) {
    this.keyStorePass = keyStorePass;
  }
  
  public synchronized ByteBuffer getTrustStoreMem() {
    return trustStoreMem.asReadOnlyBuffer();
  }
  
  public synchronized void updateTrustStoreMem(ByteBuffer trustStoreMem) {
    this.trustStoreMem = trustStoreMem;
  }
  
  public synchronized String getTrustStorePass() {
    return trustStorePass;
  }
  
  public synchronized void updateTrustStorePass(String trustStorePass) {
    this.trustStorePass = trustStorePass;
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
  
  public synchronized void changeState(STATE state) {
    this.state = state;
  }
  
  public synchronized STATE getState() {
    return state;
  }
  
  public boolean hasBeenCanceled() {
    return tombstone.get();
  }
  
  public synchronized boolean tryToCancel() {
    if (state.equals(STATE.NEW)) {
      tombstone.set(true);
      return true;
    }
    return false;
  }
}
