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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.KeyLengthException;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.text.ParseException;

public class MockJWTIssuer {
  
  private final byte[] sharedSecret;
  private final JWSSigner signer;
  private final JWSVerifier verifier;
  
  public MockJWTIssuer(byte[] sharedSecret) throws KeyLengthException, JOSEException {
    this.sharedSecret = sharedSecret;
    signer = new MACSigner(sharedSecret);
    verifier = new MACVerifier(sharedSecret);
  }
  
  public String generate(JWTClaimsSet claims) throws JOSEException {
    SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
    signedJWT.sign(signer);
    
    return signedJWT.serialize();
  }
  
  public boolean verify(String token) throws ParseException, JOSEException {
    SignedJWT jwt = SignedJWT.parse(token);
    return jwt.verify(verifier);
  }
}
