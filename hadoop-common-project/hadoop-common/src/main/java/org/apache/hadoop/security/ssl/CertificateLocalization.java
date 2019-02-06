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
  
  void removeMaterial(String username)
    throws InterruptedException, ExecutionException;
  
  void removeMaterial(String username, String applicationId)
      throws InterruptedException, ExecutionException;
  
  CryptoMaterial getMaterialLocation(String username)
      throws FileNotFoundException, InterruptedException;
  
  CryptoMaterial getMaterialLocation(String username, String applicationId)
      throws FileNotFoundException, InterruptedException;
  
  void updateCryptoMaterial(String username, String applicationId, ByteBuffer keyStore, String keyStorePassword,
      ByteBuffer trustStore, String trustStorePassword) throws IOException, InterruptedException;
  
  String getSuperKeystoreLocation();
  
  String getSuperKeystorePass();
  
  String getSuperKeyPassword();
  
  String getSuperTruststoreLocation();
  
  String getSuperTruststorePass();
}
