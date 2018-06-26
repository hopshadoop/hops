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
package io.hops.leaderElection;

import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.election.entity.LeDescriptorFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LEContext {

  protected LeaderElectionRole.Role role;
  protected long last_hb_time;
  protected long leader;
  protected long time_period;
  protected List<HashMap<Long, LeDescriptor>> history;
  protected long id;
  protected boolean init_phase;
  protected int max_missed_hb_threshold;
  protected String rpc_addresses;
  protected String http_address;
  protected SortedActiveNodeList memberShip;
  protected long time_period_increment;
  protected boolean nextTimeTakeStrongerLocks;
  protected List<LeDescriptor> removedNodes; //nodes removed during this round

  private LEContext() {
  }

  public LEContext(LEContext context, LeDescriptorFactory leFactory) {
    role = context.role;
    last_hb_time = context.last_hb_time;
    leader = context.leader;
    time_period = context.time_period;
    id = context.id;
    init_phase = context.init_phase;
    max_missed_hb_threshold = context.max_missed_hb_threshold;
    rpc_addresses = context.rpc_addresses;
    http_address = context.http_address;
    time_period_increment = context.time_period_increment;
    nextTimeTakeStrongerLocks = context.nextTimeTakeStrongerLocks;

    //clone history
    history = new ArrayList<HashMap<Long, LeDescriptor>>();
    if (!context.history.isEmpty()) {
      for (HashMap<Long, LeDescriptor> map : context.history) {
        HashMap<Long, LeDescriptor> newMap = new HashMap<Long, LeDescriptor>();
        for (LeDescriptor process : map.values()) {
          LeDescriptor processClone =
              (LeDescriptor) leFactory.cloneDescriptor(process);
          newMap.put(processClone.getId(), processClone);
        }
        history.add(newMap);
      }
    }
    
    //ToDo clone membership
    memberShip = context.memberShip;

    removedNodes = context.removedNodes;
  }

  public static LEContext initialContext() {
    LEContext context = new LEContext();
    context.role = LeaderElectionRole.Role.NON_LEADER;
    context.last_hb_time = 0;
    context.leader = -1;
    context.time_period = 0;
    context.history = new ArrayList<HashMap<Long, LeDescriptor>>();
    context.id = LeaderElection.LEADER_INITIALIZATION_ID;
    context.init_phase = false;
    context.max_missed_hb_threshold = 2;
    context.memberShip = null;
    context.http_address = null;
    context.rpc_addresses = null;
    context.time_period_increment = 0;
    context.nextTimeTakeStrongerLocks = false;
    context.removedNodes = new ArrayList<LeDescriptor>();
    return context;
  }
}
