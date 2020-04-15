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
package io.hops.net;

import com.google.common.annotations.VisibleForTesting;
import com.logicalclocks.servicediscoverclient.Builder;
import com.logicalclocks.servicediscoverclient.ServiceDiscoveryClient;
import com.logicalclocks.servicediscoverclient.exceptions.ServiceDiscoveryException;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

public class ServiceDiscoveryClientFactory {
  
  private static volatile ServiceDiscoveryClientFactory SELF;
  private ServiceDiscoveryClient testClient;
  
  private ServiceDiscoveryClientFactory() {
  }
  
  public static ServiceDiscoveryClientFactory getInstance() {
    if (SELF == null) {
      synchronized (ServiceDiscoveryClientFactory.class) {
        if (SELF == null) {
          SELF = new ServiceDiscoveryClientFactory();
        }
      }
    }
    return SELF;
  }
  
  public ServiceDiscoveryClient getClient(Builder builder) throws ServiceDiscoveryException {
    return testClient != null ? testClient : builder.build();
  }

  public Pair<String, Integer> getNameserver(Configuration conf) {
    String dnsHost = conf.get(CommonConfigurationKeys.SERVICE_DISCOVERY_DNS_HOST,
            CommonConfigurationKeys.DEFAULT_SERVICE_DISCOVERY_DNS_HOST);
    if (dnsHost.isEmpty()) {
      dnsHost = null;
    }
    Integer dnsPort = conf.getInt(CommonConfigurationKeys.SERVICE_DISCOVERY_DNS_PORT,
            CommonConfigurationKeys.DEFAULT_SERVICE_DISCOVERY_DNS_PORT);
    if (dnsPort == -1) {
      dnsPort = null;
    }
    return new Pair<>(dnsHost, dnsPort);
  }

  @VisibleForTesting
  public void setClient(ServiceDiscoveryClient sdClient) {
    testClient = sdClient;
  }
}
