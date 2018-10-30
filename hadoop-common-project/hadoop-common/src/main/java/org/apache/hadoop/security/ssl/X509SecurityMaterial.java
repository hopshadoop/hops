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

public final class X509SecurityMaterial extends SecurityMaterial {
  private final Path keyStoreLocation;
  private final int keyStoreSize;
  private final Path trustStoreLocation;
  private final Path passwdLocation;
  private final int trustStoreSize;
  private ByteBuffer keyStoreMem;
  private String keyStorePass;
  private ByteBuffer trustStoreMem;
  private String trustStorePass;
  
  public X509SecurityMaterial(Path certFolder, Path keyStoreLocation,
      Path trustStoreLocation, Path passwdLocation, ByteBuffer kStore,
      String kStorePass, ByteBuffer tstore, String tstorePass) {
    super(certFolder);
    this.keyStoreLocation = keyStoreLocation;
    this.keyStoreSize = kStore.capacity();
    this.trustStoreLocation = trustStoreLocation;
    this.passwdLocation = passwdLocation;
    this.trustStoreSize = tstore.capacity();
    this.keyStoreMem = kStore;
    this.keyStorePass = kStorePass;
    this.trustStoreMem = tstore;
    this.trustStorePass = tstorePass;
  }
  
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
}
