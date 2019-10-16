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
package io.hops.common.security;

import io.hops.security.AbstractSecurityActions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;

public class HopsworksFsSecurityActions extends AbstractSecurityActions implements FsSecurityActions {
  
  private boolean configured = false;
  
  public HopsworksFsSecurityActions() {
    super("HopsworksFsSecurityActions");
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (conf.getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      super.serviceInit(conf);
    }
  }
  
  @Override
  protected void serviceStart() throws Exception {
    if (getConfig().getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      super.serviceStart();
      configured = true;
    }
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (getConfig().getBoolean(CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED,
        CommonConfigurationKeys.IPC_SERVER_SSL_ENABLED_DEFAULT)) {
      super.serviceStop();
    }
  }
  
  @Override
  public X509CredentialsDTO getX509Credentials(String username)
      throws URISyntaxException, GeneralSecurityException, IOException {
    if (!configured) {
      notConfigured("getX509Credentials");
    }
    URI requestURI = new URIBuilder(getConfig().get(DFSConfigKeys.FS_SECURITY_ACTIONS_X509_PATH_KEY,
        DFSConfigKeys.DEFAULT_FS_SECURITY_ACTIONS_X509_PATH))
        .addParameter("username", username)
        .build();
    HttpGet request = new HttpGet(requestURI);
    addJWTAuthHeader(request, serviceJWTManager.getMasterToken());
    try {
      return httpClient.execute(remoteHost, request, new GetX509CredentialsHandler(username));
    } catch (ClientProtocolException ex) {
      throw new IOException(ex);
    }
  }
  
  private void notConfigured(String methodName) throws GeneralSecurityException {
    throw new GeneralSecurityException("Called method " + methodName + " of "
      + HopsworksFsSecurityActions.class.getSimpleName() + " but it is not configured yet");
  }
  
  private class GetX509CredentialsHandler implements ResponseHandler<X509CredentialsDTO> {
    private final String username;
    
    private GetX509CredentialsHandler(String username) {
      this.username = username;
    }
    
    @Override
    public X509CredentialsDTO handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
      int status = response.getStatusLine().getStatusCode();
      if (status == HttpStatus.SC_OK) {
        return parser.fromJson(EntityUtils.toString(response.getEntity()), X509CredentialsDTO.class);
      }
      throw new ClientProtocolException("Failed to get X.509 for " + username
        + " Status: " + status + " Reason: " + response.getStatusLine().getReasonPhrase());
    }
  }
  
  public static class X509CredentialsDTO {
    private String fileExtension;
    private String kStore;
    private String tStore;
    private String password;
  
    public X509CredentialsDTO() {}
    
    public String getFileExtension() {
      return fileExtension;
    }
  
    public void setFileExtension(String fileExtension) {
      this.fileExtension = fileExtension;
    }
  
    public String getkStore() {
      return kStore;
    }
  
    public void setkStore(String kStore) {
      this.kStore = kStore;
    }
  
    public String gettStore() {
      return tStore;
    }
  
    public void settStore(String tStore) {
      this.tStore = tStore;
    }
  
    public String getPassword() {
      return password;
    }
  
    public void setPassword(String password) {
      this.password = password;
    }
  }
}
