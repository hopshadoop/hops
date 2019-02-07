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
package org.apache.hadoop.hdfs.server.namenode.ha;

import com.google.common.base.Preconditions;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.FailoverProxyHelper.AddressRpcProxyPair;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * A FailoverProxyProvider implementation which allows one to configure two URIs
 * to connect to during fail-over. The first configured address is tried first,
 * and on a fail-over event the other address is tried.
 */
public class HopsRandomStickyFailoverProxyProvider<T> implements
        FailoverProxyProvider<T> {

  public static final Log LOG =
          LogFactory.getLog(HopsRandomStickyFailoverProxyProvider.class);

  private final Configuration conf;
  private final List<AddressRpcProxyPair<T>> proxies =
          new ArrayList<AddressRpcProxyPair<T>>();
  private final Map<Integer, List<AddressRpcProxyPair<T>>> proxiesByDomainId =
      new HashMap();
  
  private final UserGroupInformation ugi;
  private final Class<T> xface;
  private final Random rand = new Random((UUID.randomUUID()).hashCode());
  private final URI uri;

  protected String name = this.getClass().getSimpleName()+" ("+this.hashCode()+") ";

  protected int currentProxyIndex = -1;
  
  private final int locationDomainId;
  
  public HopsRandomStickyFailoverProxyProvider(Configuration conf, URI uri,
                                               Class<T> xface) {
    Preconditions.checkArgument(
            xface.isAssignableFrom(NamenodeProtocols.class),
            "Interface class %s is not a valid NameNode protocol!");
    this.xface = xface;

    this.conf = new Configuration(conf);
    int maxRetries = this.conf.getInt(
            DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            maxRetries);

    int maxRetriesOnSocketTimeouts = this.conf.getInt(
            DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            maxRetriesOnSocketTimeouts);
    
    this.locationDomainId = conf.getInt(DFSConfigKeys.DFS_LOCATION_DOMAIN_ID,
        DFSConfigKeys.DFS_LOCATION_DOMAIN_ID_DEFAULT);
    
    try {
      ugi = UserGroupInformation.getCurrentUser();

      this.uri = uri;

      List<ActiveNode> anl = FailoverProxyHelper.getActiveNamenodes(conf, xface, ugi, uri);
      updateProxies(anl);
      setRandProxyIndex();

      // The client may have a delegation token set for the logical
      // URI of the cluster. Clone this token to apply to each of the
      // underlying IPC addresses so that the IPC code can find it.
      //  HAUtil.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<T> getInterface() {
    return xface;
  }

  /**
   * Lazily initialize the RPC proxy object.
   */
  @SuppressWarnings("unchecked")
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    try {
      if (currentProxyIndex == -1) {
        LOG.debug(name + " returning default proxy");
        return new ProxyInfo<T>((T) NameNodeProxies.createNonHAProxy(conf, NameNode.getAddress(uri),
                xface, ugi, false).getProxy(), null);
//      return new ProxyInfo<T>((T) null, null);
      }

      AddressRpcProxyPair current = proxies.get(currentProxyIndex);
      if (current.namenode == null) {
        current.namenode = NameNodeProxies.createNonHAProxy(conf,
                current.address, xface, ugi, false).getProxy();
      }

      LOG.debug(name + " returning proxy for index: " + currentProxyIndex + " address: " +
              "" + current .address + " " + "Total proxies are: " + proxies.size());
      return new ProxyInfo<T>((T) current.namenode, null);

    } catch (IOException e) {
      LOG.error(name + " failed to create RPC proxy to NameNode", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    try {

      LOG.debug(name+" failover happened");
      //refresh list of namenodes
      List<ActiveNode> anl = FailoverProxyHelper.getActiveNamenodes(conf, xface, ugi, uri);
      LOG.debug(name+" failover happened 2");

      updateProxies(anl);
      setRandProxyIndex();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }


  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (AddressRpcProxyPair<T> proxy : proxies) {
      if (proxy.namenode != null) {
        if (proxy.namenode instanceof Closeable) {
          ((Closeable) proxy.namenode).close();
        } else {
          RPC.stopProxy(proxy.namenode);
        }
        proxy.namenode = null;
      }
    }
    proxies.clear();
  }

  private void setRandProxyIndex() {
    if(locationDomainId != DFSConfigKeys.DFS_LOCATION_DOMAIN_ID_DEFAULT){
      List<AddressRpcProxyPair<T>> domainProxies =
          proxiesByDomainId.get(locationDomainId);
      if(domainProxies != null && !domainProxies.isEmpty()){
        int randomNN = rand.nextInt(domainProxies.size());
        currentProxyIndex = domainProxies.get(randomNN).index;
        LOG.debug(name + " random proxy index is set to: " + currentProxyIndex + " NN address: " + proxies
            .get(currentProxyIndex).address + " LocationDomainId: " + locationDomainId);
        return;
      }
    }
    
    if(proxies.size()>0) {
      currentProxyIndex = rand.nextInt(proxies.size());
      LOG.debug(name + " random proxy index is set to: " + currentProxyIndex + " NN address: " + proxies
              .get(currentProxyIndex).address);
    }
  }


  void updateProxies(List<ActiveNode> anl) throws IOException {
    if (anl != null) {
      this.close(); // close existing proxies
      int index = 0;
      for (ActiveNode node : anl) {
        AddressRpcProxyPair<T> pair =
            new AddressRpcProxyPair<T>(node.getRpcServerAddressForClients(),
                index);
        proxies.add(pair);
        
        if(!proxiesByDomainId.containsKey(node.getLocationDomainId())){
          proxiesByDomainId.put(node.getLocationDomainId(),
              new ArrayList<AddressRpcProxyPair<T>>());
        }
        proxiesByDomainId.get(node.getLocationDomainId()).add(pair);
        index++;
      }

      LOG.debug(name+" new set of proxies are: "+ Arrays.toString(anl.toArray()));
    } else {
      LOG.warn(name+" no new namenodes were found");
    }
  }

}
