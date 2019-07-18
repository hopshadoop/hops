/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves all UGIs to the submitting user.
 */
public class SubmitterUserResolver implements UserResolver {
  public static final Logger LOG = LoggerFactory.getLogger(SubmitterUserResolver.class);
  
  private UserGroupInformation ugi = null;

  public SubmitterUserResolver() throws IOException {
    LOG.info(" Current user resolver is SubmitterUserResolver ");
    ugi = UserGroupInformation.getLoginUser();
  }

  public synchronized boolean setTargetUsers(URI userdesc, Configuration conf)
      throws IOException {
    return false;
  }

  public synchronized UserGroupInformation getTargetUgi(
      UserGroupInformation ugi) {
    return this.ugi;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Since {@link SubmitterUserResolver} returns the user name who is running
   * gridmix, it doesn't need a target list of users.
   */
  public boolean needsTargetUsersList() {
    return false;
  }
}
