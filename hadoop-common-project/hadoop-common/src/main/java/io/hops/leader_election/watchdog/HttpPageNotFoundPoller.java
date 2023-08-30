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
package io.hops.leader_election.watchdog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.net.URI;

public class HttpPageNotFoundPoller implements AliveWatchdogPoller {
  private final static Log LOG = LogFactory.getLog(HttpPageNotFoundPoller.class);

  private static final int MAX_CONNECTIONS_PER_ROUTE = 20;
  private Configuration conf;
  private HttpGet request;
  private PoolingHttpClientConnectionManager httpConnectionManager;
  private CloseableHttpClient httpClient;

  @Override
  public Boolean shouldIBeAlive() throws Exception {
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
    }
  }

  @Override
  public void init() throws Exception {
    String url = conf.get(HttpUtils.POLL_ENDPOINT);
    if (url == null) {
      String msg = "Alive watchdog HTTP poller url is empty. Make sure you have set " + HttpUtils.POLL_ENDPOINT;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    URI endpoint = new URI(url);
    request = new HttpGet(endpoint);
    httpConnectionManager = HttpUtils.createHTTPConnectionManager(conf, MAX_CONNECTIONS_PER_ROUTE);
    httpClient = HttpClients.custom().setConnectionManager(httpConnectionManager).build();
  }

  @Override
  public void destroy() throws Exception {
    if (httpClient != null) {
      httpClient.close();
    }
    if (httpConnectionManager != null) {
      httpConnectionManager.close();
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
