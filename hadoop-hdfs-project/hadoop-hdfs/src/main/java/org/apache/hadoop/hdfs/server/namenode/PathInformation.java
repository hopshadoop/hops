/*
 * Copyright 2019 Apache Software Foundation.
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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;
import org.apache.hadoop.fs.permission.AclEntry;

public class PathInformation {

  private String path;
  private byte[][] pathComponents;
  private INodesInPath IIP;
  private boolean dir;
  private QuotaCounts usage;
  private QuotaCounts quota;
  
  private final List<AclEntry>[] pathInodeAcls;

  public PathInformation(String path,
      byte[][] pathComponents, INodesInPath IIP,
      boolean dir, QuotaCounts quota, QuotaCounts usage, List<AclEntry>[] pathInodeAcls) {
    this.path = path;
    this.pathComponents = pathComponents;
    this.IIP = IIP;
    this.dir = dir;
    this.quota = quota;
    this.usage = usage;
    this.pathInodeAcls = pathInodeAcls;
  }

  public String getPath() {
    return path;
  }

  public byte[][] getPathComponents() {
    return pathComponents;
  }

  public boolean isDir() {
    return dir;
  }

  public INodesInPath getINodesInPath(){
    return IIP;
  }
  
  public QuotaCounts getQuota() {
    return quota;
  }

  public QuotaCounts getUsage() {
    return usage;
  }

  public List<AclEntry>[] getPathInodeAcls() {
    return pathInodeAcls;
  }
}
