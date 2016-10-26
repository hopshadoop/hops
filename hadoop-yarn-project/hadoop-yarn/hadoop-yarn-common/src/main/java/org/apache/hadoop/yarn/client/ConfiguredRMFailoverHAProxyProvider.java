/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.client;

import io.hops.leader_election.node.ActiveNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

//TODO make it work when YarnConfiguration.AUTO_FAILOVER_ENABLED is false
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class ConfiguredRMFailoverHAProxyProvider<T>
    implements RMFailoverProxyProvider<T> {

  private static final Log LOG =
      LogFactory.getLog(ConfiguredRMFailoverHAProxyProvider.class);

  private RMProxy<T> rmProxy;
  private T currentProxy;
  private String currentRMId = null;

  Map<String, T> oldProxies = new HashMap<String, T>();

  private Class<T> protocol;
  protected YarnConfiguration conf;
  protected GroupMembershipProxyService groupMembership;

  @Override
  public void init(Configuration configuration, RMProxy<T> rmProxy,
      Class<T> protocol) {
    this.rmProxy = rmProxy;
    this.protocol = protocol;
    rmProxy.checkAllowedProtocols(this.protocol);
    this.conf = new YarnConfiguration(configuration);

    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES));

    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(
            YarnConfiguration.CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS));

    groupMembership = new GroupMembershipProxyService(conf, rmProxy);
  }

  private T getProxyFromActiveNode(ActiveNode leader) {
    T proxy = oldProxies.get(leader.getHostname());
    if (proxy != null) {
      return proxy;
    }
    return createProxy(leader);
  }

  private T createProxy(ActiveNode leader) {
    try {

      final InetSocketAddress rmAddress = rmProxy
          //.getRMAddress(conf, protocol, leader.getIpAddress(),
          //    leader.getPort());
          .getRMAddress(conf, protocol, leader.getIpAddress());
      LOG.info("creating proxy from active nodes " + currentRMId + " " +
          rmAddress.getPort());
      return RMProxy.getProxy(conf, protocol, rmAddress);
    } catch (IOException ioe) {
      LOG.error(
          "Unable to create proxy to the ResourceManager " + leader.toString(),
          ioe);
      return null;
    }
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    if (currentProxy == null) {
      performFailover(currentProxy);
    }
    return new ProxyInfo<T>(currentProxy, currentRMId);
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    LOG.info("performing failover");
    if (currentProxy != null) {
      oldProxies.put(currentRMId, this.currentProxy);
    }

    int nbTry = 0;
    do {
      ActiveNode leader = getActiveNode();
      if (leader != null) {
        if (currentRMId == null || !currentRMId.equals(leader.getHostname())) {
          currentRMId = leader.getHostname();
          this.currentProxy = getProxyFromActiveNode(leader);
        } else {
          this.currentProxy = createProxy(leader);
          if (this.currentProxy == null) {
            nbTry++;
            try {
              //TODO put base time and max time as parameters
              Thread.sleep(
                      RetryPolicies.calculateExponentialTime(500L, nbTry, 10000L));
            } catch (Exception e) {
              LOG.error(e);
            }

          }
        }
      }
    } while (this.currentProxy == null);
    conf.set(YarnConfiguration.RM_HA_ID, currentRMId);
    LOG.info("Failing over to " + currentRMId);
  }

  protected abstract ActiveNode getActiveNode();

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
    groupMembership.close();
    for (T proxy : oldProxies.values()) {
      if (proxy instanceof Closeable) {
        ((Closeable) proxy).close();
      } else {
        RPC.stopProxy(proxy);
      }
    }
  }

}
