/*
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
package org.apache.hadoop.cli.util;

import org.apache.hadoop.hdfs.tools.CacheAdmin;
import org.apache.hadoop.util.ToolRunner;

public class CacheAdminCmdExecutor extends CommandExecutor {
  protected String namenode = null;
  protected CacheAdmin admin = null;

  public CacheAdminCmdExecutor(String namenode, CacheAdmin admin) {
    this.namenode = namenode;
    this.admin = admin;
  }

  @Override
  protected int execute(final String cmd) throws Exception {
    String[] args = getCommandAsArgs(cmd, "NAMENODE", this.namenode);
    return ToolRunner.run(admin, args);
  }
}
