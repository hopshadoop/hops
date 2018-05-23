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
package io.hops.security;

import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;

import java.io.IOException;
import java.util.List;

public class HopsGroupsWithFallBack extends
    JniBasedUnixGroupsMappingWithFallback{

  @Override
  public List<String> getGroups(final String user) throws IOException {
    List<String> groups = UsersGroups.getGroups(user);

    if (groups != null && !groups.isEmpty()) {
      return groups;
    }

    return super.getGroups(user);
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
    super.cacheGroupsRefresh();
    UsersGroups.clearCache();
  }

}
