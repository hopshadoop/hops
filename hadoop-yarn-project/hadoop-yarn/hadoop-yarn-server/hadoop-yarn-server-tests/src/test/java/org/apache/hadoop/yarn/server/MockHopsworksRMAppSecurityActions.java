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
package org.apache.hadoop.yarn.server;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jwt.JWTClaimsSet;
import org.apache.hadoop.yarn.server.resourcemanager.security.HopsworksRMAppSecurityActions;
import org.apache.hadoop.yarn.server.resourcemanager.security.MockJWTIssuer;

import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Random;

public class MockHopsworksRMAppSecurityActions extends HopsworksRMAppSecurityActions {
  private final byte[] secret;
  private final MockJWTIssuer jwtIssuer;
  
  public MockHopsworksRMAppSecurityActions() throws MalformedURLException, GeneralSecurityException, JOSEException {
    super();
    Random rand = new Random();
    secret = new byte[32];
    rand.nextBytes(secret);
    jwtIssuer = new MockJWTIssuer(secret);
  }
  
  @Override
  protected void loadMasterJWT() throws GeneralSecurityException {
    LocalDateTime masterExpiration = LocalDateTime.now().plus(10L, ChronoUnit.MINUTES);
    JWTClaimsSet claims = new JWTClaimsSet.Builder().expirationTime(Date.from(masterExpiration.atZone(ZoneId.
        systemDefault()).toInstant())).build();
    try {
      String masterToken = jwtIssuer.generate(claims);
      setMasterToken(masterToken);
      setMasterTokenExpiration(masterExpiration);
    } catch (JOSEException ex) {
      throw new GeneralSecurityException(ex.getMessage(), ex);
    }
  }
  
  @Override
  protected void loadRenewalJWTs() throws GeneralSecurityException {
    setRenewalTokens(new String[0]);
  }
}
