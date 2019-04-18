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
package io.hops.leaderElection.experiments;

import io.hops.leaderElection.LeaderElection;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.election.entity.LeDescriptorFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Light weight NameNode class used for testing the Leader
 * Election Protocol
 */
public class LightWeightNameNode {

  private static final Log LOG = LogFactory.getLog(LightWeightNameNode.class);
  protected LeaderElection leaderElection;

  public LightWeightNameNode(LeDescriptorFactory ldf, final long time_period,
      final int max_missed_hb_threshold, final long time_period_increment,
      final String http_address, final String rpc_address)
      throws IOException, CloneNotSupportedException {
    leaderElection =
        new LeaderElection(ldf, time_period, max_missed_hb_threshold,
            time_period_increment, http_address, rpc_address);
    leaderElection.start();
    LOG.debug("NameNode has started");
  }

  public long getLeCurrentId() {
    return leaderElection.getCurrentId();
  }

  public boolean isLeader() {
    return leaderElection.isLeader();
  }

  public void stop() {
    if (!leaderElection.isStopped()) {
      try {
        leaderElection.stopElectionThread();
      } catch (InterruptedException e) {
        LOG.warn("LeaderElection stopped", e);
      }
    }
  }

  public LeaderElection getLeaderElectionInstance() {
    return leaderElection;
  }

  public InetSocketAddress getNameNodeAddress() {
    Pattern p = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");
    Matcher m = p.matcher(leaderElection.getRpcAddress());
    if (m.matches()) {
      String host = m.group(1);
      int port = Integer.parseInt(m.group(2));
      return new InetSocketAddress(host, port);
    } else {
      return null;
    }
  }

  public SortedActiveNodeList getActiveNameNodes() {
    return leaderElection.getActiveNamenodes();
  }

  public long getLeTimePeriod() {
    return leaderElection.getCurrentTimePeriod();
  }
}
