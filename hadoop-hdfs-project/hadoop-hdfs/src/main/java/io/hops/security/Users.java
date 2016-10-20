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

import java.io.IOException;

public class Users {

  public static void addUserToGroup(String user, String group)
      throws IOException {
    UsersGroups.addUserToGroups(user, new String[]{group});
  }

  public static void flushCache(){
    flushCache(null, null);
  }

  public static void flushCache(String user, String group){
    if(user == null && group == null){
      UsersGroups.clearCache();
    }else{
      UsersGroups.flushUser(user);
      UsersGroups.flushGroup(group);
    }
  }
}
