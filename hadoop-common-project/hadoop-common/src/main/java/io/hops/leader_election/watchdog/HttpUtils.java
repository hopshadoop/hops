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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

public class HttpUtils {
  static final String POLL_ENDPOINT = CommonConfigurationKeys.ALIVE_WATCHDOG_PREFIX
      + "http-poll.url";
  static final String POLL_TRUSTSTORE = CommonConfigurationKeys.ALIVE_WATCHDOG_PREFIX
      + "http-poll.truststore";
  static final String POLL_TRUSTSTORE_PASSWORD = CommonConfigurationKeys.ALIVE_WATCHDOG_PREFIX
      + "http-poll.truststore-password";

  protected static PoolingHttpClientConnectionManager createHTTPConnectionManager(Configuration conf,
      int maxConnectionsPerRoute)
      throws IOException, GeneralSecurityException {
    RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
    registryBuilder.register("http", PlainConnectionSocketFactory.getSocketFactory());

    String trustStoreLocation = conf.get(HttpUtils.POLL_TRUSTSTORE);
    String trustStorePassword = conf.get(HttpUtils.POLL_TRUSTSTORE_PASSWORD);
    if (trustStoreLocation != null) {
      TrustStrategy acceptAll = (certChain, authType) -> true;
      SSLContext sslCtx = SSLContexts.custom()
          .loadTrustMaterial(new File(trustStoreLocation), trustStorePassword.toCharArray(), acceptAll)
          .build();
      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslCtx);
      registryBuilder.register("https", sslSocketFactory);
    }
    PoolingHttpClientConnectionManager connectionManager =
        new PoolingHttpClientConnectionManager(registryBuilder.build());
    connectionManager.setDefaultMaxPerRoute(maxConnectionsPerRoute);
    return connectionManager;
  }
}
