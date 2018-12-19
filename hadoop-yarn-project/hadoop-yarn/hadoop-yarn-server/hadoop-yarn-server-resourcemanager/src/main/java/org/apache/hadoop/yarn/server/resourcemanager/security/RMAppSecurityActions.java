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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;

public interface RMAppSecurityActions {
  void init() throws MalformedURLException, GeneralSecurityException, IOException;
  void destroy();
  X509SecurityHandler.CertificateBundle sign(PKCS10CertificationRequest csr) throws URISyntaxException, IOException, GeneralSecurityException;
  int revoke(String certificateIdentifier) throws URISyntaxException, IOException, GeneralSecurityException;
  
  String generateJWT(JWTSecurityHandler.JWTMaterialParameter jwtParameter) throws URISyntaxException, IOException,
      GeneralSecurityException;
  void invalidateJWT(String signingKeyName) throws URISyntaxException, IOException, GeneralSecurityException;
  String renewJWT(JWTSecurityHandler.JWTMaterialParameter jwtParameter) throws URISyntaxException, IOException,
      GeneralSecurityException;
}
