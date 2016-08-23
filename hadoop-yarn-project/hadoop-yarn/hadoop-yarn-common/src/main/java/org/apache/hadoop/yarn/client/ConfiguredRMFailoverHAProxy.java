/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ConfiguredRMFailoverHAProxy<T>
        implements RMFailoverProxyProvider<T> {

  private static final Log LOG = LogFactory.getLog(
          ConfiguredRMFailoverHAProxy.class);

  private T currentProxty;
  private String currentRMId;

  Map<String, T> oldProxies = new HashMap<String, T>();

  private RMProxy<T> rmProxy;
  private Class<T> protocol;
  protected YarnConfiguration conf;
  protected String[] rmServiceIds;

  @Override
  public void init(Configuration configuration, RMProxy<T> rmProxy,
          Class<T> protocol) {
    this.rmProxy = rmProxy;
    this.protocol = protocol;
    this.rmProxy.checkAllowedProtocols(this.protocol);
    this.conf = new YarnConfiguration(configuration);
    Collection<String> rmIds = HAUtil.getRMHAIds(conf);
    this.rmServiceIds = rmIds.toArray(new String[rmIds.size()]);

    conf.
            setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
                    conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES,
                            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES));

    conf.setInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            conf.getInt(
                    YarnConfiguration.CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS,
                    YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS));
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    return new ProxyInfo<T>(currentProxty, currentRMId);
  }

  @Override
  public synchronized void performFailover(T currentProxy) {

    oldProxies.put(currentRMId, currentProxy);

    conf.set(YarnConfiguration.RM_HA_ID, currentRMId);
    LOG.info("Failing over to " + currentRMId);
  }

  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (T proxy : oldProxies.values()) {
      if (proxy instanceof Closeable) {
        ((Closeable) proxy).close();
      } else {
        RPC.stopProxy(proxy);
      }
    }
  }
}
