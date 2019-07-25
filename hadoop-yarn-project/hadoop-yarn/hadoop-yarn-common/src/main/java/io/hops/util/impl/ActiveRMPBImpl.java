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
package io.hops.util.impl;

import io.hops.leader_election.node.ActiveNodePBImpl;
import io.hops.leader_election.proto.ActiveNodeProtos.ActiveNodeProto;
import io.hops.leader_election.proto.ActiveNodeProtos.ActiveNodeProtoOrBuilder;
import io.hops.util.ActiveRM;
import org.apache.hadoop.yarn.proto.GroupMembership;

//TODO change it to avoid going through the proto when it is not needed
public class ActiveRMPBImpl extends ActiveNodePBImpl implements ActiveRM {

  public ActiveRMPBImpl(ActiveNodeProto proto) {
    super(proto);
  }

  public ActiveRMPBImpl(long id, String hostname, String ipAddress, int port,
      String httpAddress) {
    super(id, hostname, ipAddress, port, httpAddress,"",0);
    maybeInitBuilder();
  }

  @Override
  public boolean equals(Object obj) {
    // objects are equal if the belong to same NN
    // namenode id is not taken in to account
    // sometimes the id of the namenode may change even without 
    //namenode restart
    if (!(obj instanceof ActiveRM)) {
      return false;
    }
    ActiveRM that = (ActiveRM) obj;
    return this.getRpcServerAddressForClients().equals(that.getRpcServerAddressForClients());
  }
}
