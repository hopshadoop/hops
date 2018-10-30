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
package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

import java.nio.ByteBuffer;

public abstract class UpdatedCryptoForApp {
  
  public enum UPDATE_TYPE {
    X509,
    JWT,
    X509_JWT,
    UNDEFINED
  }
  
  public static UpdatedCryptoForApp newInstance(ByteBuffer keyStore, char[] keyStorePassword, ByteBuffer trustStore,
      char[] trustStorePassword, int version) {
    UpdatedCryptoForApp updatedCryptoForApp = Records.newRecord(UpdatedCryptoForApp.class);
    updatedCryptoForApp.setKeyStore(keyStore);
    updatedCryptoForApp.setKeyStorePassword(keyStorePassword);
    updatedCryptoForApp.setTrustStore(trustStore);
    updatedCryptoForApp.setTrustStorePassword(trustStorePassword);
    updatedCryptoForApp.setVersion(version);
    
    return updatedCryptoForApp;
  }
  
  public static UpdatedCryptoForApp newInstance(int x509Version, long jwtExpiration) {
    UpdatedCryptoForApp updatedCryptoForApp = Records.newRecord(UpdatedCryptoForApp.class);
    updatedCryptoForApp.setVersion(x509Version);
    updatedCryptoForApp.setJWTExpiration(jwtExpiration);
    return updatedCryptoForApp;
  }
  
  public abstract ByteBuffer getKeyStore();
  
  public abstract void setKeyStore(ByteBuffer keyStore);
  
  public abstract char[] getKeyStorePassword();
  
  public abstract void setKeyStorePassword(char[] keyStorePassword);
  
  public abstract ByteBuffer getTrustStore();
  
  public abstract void setTrustStore(ByteBuffer trustStore);
  
  public abstract char[] getTrustStorePassword();
  
  public abstract void setTrustStorePassword(char[] trustStorePassword);
  
  public abstract int getVersion();
  
  public abstract void setVersion(int version);
  
  public abstract String getJWT();
  
  public abstract void setJWT(String jwt);
  
  public abstract long getJWTExpiration();
  
  public abstract void setJWTExpiration(long jwtExpiration);
  
  public UPDATE_TYPE determineUpdateType() {
    if (getKeyStore() != null && getJWT() != null) {
      return UPDATE_TYPE.X509_JWT;
    }
    
    if (getKeyStore() != null) {
      return UPDATE_TYPE.X509;
    }
    
    if (getJWT() != null) {
      return UPDATE_TYPE.JWT;
    }
    
    return UPDATE_TYPE.UNDEFINED;
  }
}
