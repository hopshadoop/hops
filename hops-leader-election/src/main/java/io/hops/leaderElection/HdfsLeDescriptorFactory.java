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

import io.hops.metadata.common.FinderType;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.election.entity.LeDescriptorFactory;

public class HdfsLeDescriptorFactory extends LeDescriptorFactory {
  @Override
  public FinderType<LeDescriptor> getAllFinder() {
    LeDescriptor.HdfsLeDescriptor.Finder finder =
        LeDescriptor.HdfsLeDescriptor.Finder.All;
    return finder;
  }
  
  @Override
  public FinderType<LeDescriptor> getByIdFinder() {
    LeDescriptor.HdfsLeDescriptor.Finder finder =
        LeDescriptor.HdfsLeDescriptor.Finder.ById;
    return finder;
  }

  @Override
  public LeDescriptor getNewDescriptor(long id, long counter, String hostName,
      String httpAddress, byte locationDomainId) {
    return new LeDescriptor.HdfsLeDescriptor(id, counter, hostName,
        httpAddress, locationDomainId);
  }

  @Override
  public LeDescriptor cloneDescriptor(LeDescriptor desc) {
    return new LeDescriptor.HdfsLeDescriptor(desc.getId(), desc.getCounter(),
        desc.getRpcAddresses(), desc.getHttpAddress(), desc.getLocationDomainId());
  }

  @Override
  public Variable.Finder getVarsFinder() {
    return Variable.Finder.HdfsLeParams;
  }
}
