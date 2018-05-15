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
package org.apache.hadoop.net.hopssslchecks;

/**
 * Contains paths to keystore, truststore and their password. It is returned by HopsSSLCheck if the check succeeds
 */
public class HopsSSLCryptoMaterial {
  private final String keyStoreLocation;
  private final String keyStorePassword;
  private final String keyPassword;
  private final String trustStoreLocation;
  private final String trustStorePassword;
  private final String passwordFileLocation;
  private final boolean needsReloading;
  
  public HopsSSLCryptoMaterial(String keyStoreLocation, String keyStorePassword, String keyPassword,
      String trustStoreLocation, String trustStorePassword) {
    this(keyStoreLocation, keyStorePassword, keyPassword, trustStoreLocation, trustStorePassword, null, false);
  }
  
  public HopsSSLCryptoMaterial(String keyStoreLocation, String keyStorePassword, String keyPassword,
      String trustStoreLocation, String trustStorePassword, String passwordFileLocation, boolean needsReloading) {
    this.keyStoreLocation = keyStoreLocation;
    this.keyStorePassword = keyStorePassword;
    this.keyPassword = keyPassword;
    this.trustStoreLocation = trustStoreLocation;
    this.trustStorePassword = trustStorePassword;
    this.passwordFileLocation = passwordFileLocation;
    this.needsReloading = needsReloading;
  }
  
  public String getKeyStoreLocation() {
    return keyStoreLocation;
  }
  
  public String getKeyStorePassword() {
    return keyStorePassword;
  }
  
  public String getKeyPassword() {
    return keyPassword;
  }
  
  public String getTrustStoreLocation() {
    return trustStoreLocation;
  }
  
  public String getTrustStorePassword() {
    return trustStorePassword;
  }
  
  public String getPasswordFileLocation() {
    return passwordFileLocation;
  }
  
  public boolean needsReloading() {
    return needsReloading;
  }
}
