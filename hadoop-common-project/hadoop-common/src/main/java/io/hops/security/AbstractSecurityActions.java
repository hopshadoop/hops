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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.service.CompositeService;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class AbstractSecurityActions extends CompositeService {
  public static final Pattern JWT_PATTERN = Pattern.compile("^Bearer\\s(.+)");
  
  protected static final int MAX_CONNECTIONS_PER_ROUTE = 50;
  
  public static final String BEARER_AUTH_HEADER_CONTENT = "Bearer %s";
  private static final Set<Integer> ACCEPTABLE_HTTP_RESPONSES = new HashSet<>(2);
  static {
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_OK);
    ACCEPTABLE_HTTP_RESPONSES.add(HttpStatus.SC_NO_CONTENT);
  }
  
  protected final ServiceJWTManager serviceJWTManager;
  
  protected CloseableHttpClient httpClient;
  protected HttpHost remoteHost;
  protected Gson parser;
  
  private PoolingHttpClientConnectionManager httpConnectionManager;
  
  public AbstractSecurityActions(String name) {
    super(name);
    serviceJWTManager = createJWTManager();
    addIfService(serviceJWTManager);
    parser = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
        // Super important. Do NOT EVER change the date format
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected ServiceJWTManager createJWTManager() {
    return new ServiceJWTManager("JWT Manager");
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    remoteHost = HttpHost.create(conf.get(CommonConfigurationKeys.HOPS_HOPSWORKS_HOST_KEY,
        "https://127.0.0.1"));
    super.serviceInit(conf);
  }
  
  protected PoolingHttpClientConnectionManager createHTTPConnectionManager() throws GeneralSecurityException {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);
    return connectionManager;
  }
  
  @Override
  protected void serviceStart() throws Exception {
    httpConnectionManager = createHTTPConnectionManager();
    httpClient = HttpClients.custom().setConnectionManager(httpConnectionManager).build();
    if (serviceJWTManager != null) {
      serviceJWTManager.setHTTPClient(httpClient);
    }
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    if (httpConnectionManager != null) {
      httpConnectionManager.shutdown();
    }
  }
  
  protected void checkHTTPResponseCode(HttpResponse response, String extraMessage) throws IOException {
    int code = response.getStatusLine().getStatusCode();
    if (!ACCEPTABLE_HTTP_RESPONSES.contains(code)) {
      throw new IOException("HTTP error, response code " + code + " Reason: " + response.getStatusLine()
          .getReasonPhrase() + " Message: " + extraMessage);
    }
  }
  
  protected void addJWTAuthHeader(HttpRequest request, String token) {
    String authHeaderContent = String.format(BEARER_AUTH_HEADER_CONTENT, token);
    request.addHeader(HttpHeaders.AUTHORIZATION, authHeaderContent);
  }
  
  protected void addJSONContentType(HttpRequest request) {
    addContentTypeHeader(request, ContentType.APPLICATION_JSON.toString());
  }
  
  protected void addTextPlainContentType(HttpRequest request) {
    addContentTypeHeader(request, ContentType.TEXT_PLAIN.toString());
  }
  
  private void addContentTypeHeader(HttpRequest request, String contentType) {
    request.addHeader(HttpHeaders.CONTENT_TYPE, contentType);
  }
  
}
