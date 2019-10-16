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
package io.hops.security;

import io.hops.common.security.FsSecurityActions;
import io.hops.common.security.HopsworksFsSecurityActions;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

public class TestingFsSecurityActions extends AbstractSecurityActions implements FsSecurityActions {
  private final Map<String, HopsworksFsSecurityActions.X509CredentialsDTO> credentials;
  
  public TestingFsSecurityActions() {
    super("TestingFsSecurityActions");
    credentials = new HashMap<>(5);
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // Nothing to do
  }
  
  @Override
  protected void serviceStart() throws Exception {
    // Nothing to do
  }
  
  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }
  
  public void setX509Credentials(String username, HopsworksFsSecurityActions.X509CredentialsDTO credentials) {
    this.credentials.put(username, credentials);
  }
  
  @Override
  public HopsworksFsSecurityActions.X509CredentialsDTO getX509Credentials(String username)
      throws URISyntaxException, GeneralSecurityException, IOException {
    if (credentials.containsKey(username)) {
      return credentials.get(username);
    }
    throw new IOException("Could not find X.509 credentials for " + username);
  }
}
