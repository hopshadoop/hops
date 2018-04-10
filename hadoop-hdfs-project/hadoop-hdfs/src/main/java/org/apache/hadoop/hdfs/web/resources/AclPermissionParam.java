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
package org.apache.hadoop.hdfs.web.resources;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.commons.lang.StringUtils;

/** AclPermission parameter. */
public class AclPermissionParam extends StringParam {
  /** Parameter name. */
  public static final String NAME = "aclspec";
  /** Default parameter value. */
  public static final String DEFAULT = "";

  private static Domain DOMAIN = new Domain(NAME,
      Pattern.compile(DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));

  /**
   * Constructor.
   * 
   * @param str a string representation of the parameter value.
   */
  public AclPermissionParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT) ? null : str);
  }

  public AclPermissionParam(List<AclEntry> acl) {
    super(DOMAIN,parseAclSpec(acl).equals(DEFAULT) ? null : parseAclSpec(acl));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public List<AclEntry> getAclPermission(boolean includePermission) {
    final String v = getValue();
    return (v != null ? AclEntry.parseAclSpec(v, includePermission) : AclEntry
        .parseAclSpec(DEFAULT, includePermission));
  }

  /**
   * Parse the list of AclEntry and returns aclspec.
   * 
   * @param List <AclEntry>
   * @return String
   */
  private static String parseAclSpec(List<AclEntry> aclEntry) {
    return StringUtils.join(aclEntry, ",");
  }
}