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

public abstract class MaterializeCryptoKeysRequest {
  
  public static MaterializeCryptoKeysRequest newInstance(String username,
      ByteBuffer keystore, ByteBuffer truststore) {
    MaterializeCryptoKeysRequest request = Records.newRecord
        (MaterializeCryptoKeysRequest.class);
    request.setUsername(username);
    request.setKeystore(keystore);
    request.setTruststore(truststore);
    
    return request;
  }
  
  public abstract String getUsername();
  
  public abstract void setUsername(String username);
  
  public abstract ByteBuffer getKeystore();
  
  public abstract void setKeystore(ByteBuffer keystore);
  
  public abstract String getKeystorePassword();
  
  public abstract void setKeystorePassword(String password);
  
  public abstract ByteBuffer getTruststore();
  
  public abstract void setTruststore(ByteBuffer truststore);
  
  public abstract String getTruststorePassword();
  
  public abstract void setTruststorePassword(String password);

  public abstract String getUserFolder();

  public abstract void setUserFolder(String userFolder);

  @Override
  public String toString() {
    return "MaterializeCryptoRequest username: " + getUsername() + " " +
        "Keystore size: " + getKeystore().position() + " Truststore size: " +
        getTruststore().position();
  }
}
