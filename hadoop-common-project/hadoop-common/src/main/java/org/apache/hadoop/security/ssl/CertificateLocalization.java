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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

public interface CertificateLocalization {

  void materializeCertificates(String username, String userFolder,
      ByteBuffer keyStore, String keyStorePassword,
      ByteBuffer trustStore, String trustStorePassword) throws InterruptedException;
  
  void materializeCertificates(String username, String applicationId, String userFolder,
      ByteBuffer keyStore, String keyStorePassword,
      ByteBuffer trustStore, String trustStorePassword) throws InterruptedException;
  
  void materializeJWT(String username, String applicationId, String userFolder, String jwt) throws InterruptedException;
  
  void removeX509Material(String username)
    throws InterruptedException;
  
  void removeX509Material(String username, String applicationId)
      throws InterruptedException;
  
  void removeJWTMaterial(String username, String applicationId)
    throws InterruptedException;
  
  X509SecurityMaterial getX509MaterialLocation(String username)
      throws FileNotFoundException, InterruptedException;
  
  X509SecurityMaterial getX509MaterialLocation(String username, String applicationId)
      throws FileNotFoundException, InterruptedException;
  
  JWTSecurityMaterial getJWTMaterialLocation(String username, String applicationId)
      throws FileNotFoundException, InterruptedException;
  
  void updateX509(String username, String applicationId, ByteBuffer keyStore, String keyStorePassword,
      ByteBuffer trustStore, String trustStorePassword) throws InterruptedException;
  
  void updateJWT(String username, String applicationId, String jwt) throws InterruptedException;
  
  String getSuperKeystoreLocation();
  
  String getSuperKeystorePass();
  
  String getSuperKeyPassword();
  
  String getSuperTruststoreLocation();
  
  String getSuperTruststorePass();
}
