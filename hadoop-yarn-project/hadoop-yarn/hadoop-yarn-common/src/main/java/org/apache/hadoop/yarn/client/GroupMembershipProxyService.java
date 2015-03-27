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

import io.hops.leader_election.node.ActiveNode;
import io.hops.yarn.groupMembership.SortedActiveRMList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.GroupMembership;
import org.apache.hadoop.yarn.api.impl.pb.client.GroupMembershipPBClientImpl;
import org.apache.hadoop.yarn.api.protocolrecords.LiveRMsResponse;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class GroupMembershipProxyService implements Closeable {

  private static final Log LOG =
      LogFactory.getLog(GroupMembershipProxyService.class);

  Random random = new Random();
  SortedActiveRMList anList = null;
  private RMProxy<GroupMembership> rmProxy;
  private Class<GroupMembership> protocol;

  protected YarnConfiguration conf;
  protected String[] rmServiceIds;
  private int currentProxyIndex = 0;

  private Map<InetSocketAddress, GroupMembership> oldProxies =
      new HashMap<InetSocketAddress, GroupMembership>();

  public GroupMembershipProxyService(Configuration conf, RMProxy rmProxy) {
    this.conf = new YarnConfiguration(conf);
    this.rmProxy = rmProxy;
    protocol = GroupMembership.class;
    Collection<String> rmIds = HAUtil.getRMHAIds(conf);
    this.rmServiceIds = rmIds.toArray(new String[rmIds.size()]);
    conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds[currentProxyIndex]);
  }

  public ActiveNode getLeader() {
    updateActiveNodeList();
    if (anList == null) {
      return null;
    } else {
      return anList.getLeader();
    }
  }

  public ActiveNode getLeastLoadedRM() {
    updateActiveNodeList();
    if (anList == null) {
      return null;
    } else {
      return anList.getLeastLoaded();
    }
  }

  private void updateActiveNodeList() {
    if (anList == null || anList.isEmpty()) {
      updateFromConfigFile();
    } else {
      updateFromActiveNodeList();
    }
  }

  private void updateFromActiveNodeList() {
    while (!anList.isEmpty()) {
      List<ActiveNode> activeNodes = anList.getActiveNodes();

      ActiveNode nextNode = activeNodes.get(random.nextInt(activeNodes.size()));
      try {
        GroupMembership proxy = oldProxies.get(nextNode.getInetSocketAddress());
        if (proxy == null) {
          proxy = new GroupMembershipPBClientImpl(1,
              nextNode.getInetSocketAddress(), conf);
          oldProxies.put(nextNode.getInetSocketAddress(), proxy);
        }

        LiveRMsResponse response = proxy.getLiveRMList();
        anList = response.getLiveRMsList();
        return;
      } catch (Exception e) {
        activeNodes.remove(nextNode);
        anList = new SortedActiveRMList(activeNodes);
        continue;
      }
    }
    updateFromConfigFile();
  }

  private void updateFromConfigFile() {
    int tries = 0;
    while (tries < rmServiceIds.length) {
      currentProxyIndex = (currentProxyIndex + 1) % rmServiceIds.length;
      conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds[currentProxyIndex]);
      try {
        LOG.info("connecting to " + rmServiceIds[currentProxyIndex]);
        final InetSocketAddress rmAddress =
            rmProxy.getRMAddress(conf, protocol);
        GroupMembership proxy = oldProxies.get(rmAddress);
        if (proxy == null) {
          proxy = RMProxy.getProxy(conf, protocol, rmAddress);
          oldProxies.put(rmAddress, proxy);
        }

        LiveRMsResponse response = proxy.getLiveRMList();
        anList = response.getLiveRMsList();
        return;
      } catch (Exception e) {
        LOG.error("Unable to create proxy to the ResourceManager " +
            rmServiceIds[currentProxyIndex]);
        anList = null;
        tries++;
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    for (GroupMembership proxy : oldProxies.values()) {
      if (proxy instanceof Closeable) {
        ((Closeable) proxy).close();
      } else {
        RPC.stopProxy(proxy);
      }
    }
  }

}
