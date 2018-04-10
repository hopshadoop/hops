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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

/**
 * Class that helps in checking file system permission.
 * The state of this class need not be synchronized as it has data structures
 * that
 * are read-only.
 * <p/>
 * Some of the helper methods are gaurded by {@link FSNamesystem#readLock()}.
 */
class FSPermissionChecker {
  static final Log LOG = LogFactory.getLog(UserGroupInformation.class);
  
  
  /** @return a string for throwing {@link AccessControlException} */
  private static String toAccessControlString(INode inode) throws StorageException, TransactionContextException,
      IOException {
    return "\"" + inode.getLocalName() + "\":"
        + inode.getUserName() + ":" + inode.getGroupName()
        + ":" + (inode.isDirectory()? "d": "-") + inode.getFsPermission();
  }
  
  /** @return a string for throwing {@link AccessControlException} */
  private static String toAccessControlString(ProjectedINode inode) throws StorageException, TransactionContextException,
      IOException {
    return "\"" + inode.getName() + "\":"
        + inode.getUserName() + ":" + inode.getGroupName()
        + ":" + (inode.isDirectory()? "d": "-") + new FsPermission(inode.getPermission());
  }
  
  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INode inode,
      FsAction access, FsPermission mode) throws IOException {
    return toAccessControlString(inode, access, mode, null);
  }

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INode inode,
      FsAction access, FsPermission mode, List<AclEntry> featureEntries) throws IOException {
    StringBuilder sb = new StringBuilder("Permission denied: ")
      .append("user=").append(user).append(", ")
      .append("access=").append(access).append(", ")
      .append("inode=\"").append(inode.getFullPathName()).append("\":")
      .append(inode.getUserName()).append(':')
      .append(inode.getGroupName()).append(':')
      .append(inode.isDirectory() ? 'd' : '-')
      .append(mode);
    if (featureEntries != null) {
      sb.append(':').append(StringUtils.join(",", featureEntries));
    }
    return sb.toString();
  }
  
  private String toAccessControlString(ProjectedINode inode,
      FsAction access, FsPermission mode, List<AclEntry> featureEntries) throws IOException {
    StringBuilder sb = new StringBuilder("Permission denied: ")
        .append("user=").append(user).append(", ")
        .append("access=").append(access).append(", ")
        .append("projectedInode=\"").append(inode.getName()).append("\":")
        .append(inode.getUserName()).append(':')
        .append(inode.getGroupName()).append(':')
        .append(inode.isDirectory() ? 'd' : '-')
        .append(mode);
    if (featureEntries != null) {
      sb.append(':').append(StringUtils.join(",", featureEntries));
    }
    return sb.toString();
  }

  private final UserGroupInformation ugi;
  private final String user;
  /**
   * A set with group namess. Not synchronized since it is unmodifiable
   */
  private final Set<String> groups;
  private final boolean isSuper;
  
  FSPermissionChecker(String fsOwner, String supergroup,
      UserGroupInformation callerUgi) {
    ugi = callerUgi;
    HashSet<String> s = new HashSet<>(Arrays.asList(ugi.getGroupNames()));
    groups = Collections.unmodifiableSet(s);
    user = ugi.getShortUserName();
    isSuper = user.equals(fsOwner) || groups.contains(supergroup);
  }

  /**
   * Check if the callers group contains the required values.
   *
   * @param group
   *     group to check
   */
  public boolean containsGroup(String group) {
    return groups.contains(group);
  }

  public String getUser() {
    return user;
  }
  
  public boolean isSuperUser() {
    return isSuper;
  }
  
  /**
   * Verify if the caller has the required permission. This will result into
   * an exception if the caller is not allowed to access the resource.
   */
  public void checkSuperuserPrivilege() throws AccessControlException {
    if (!isSuper) {
      throw new AccessControlException("Access denied for user " + user +
          ". Superuser privilege is required");
    }
  }
  
  void checkPermission(String path, INodeDirectory root, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess) throws AccessControlException, UnresolvedLinkException, StorageException,
      TransactionContextException, IOException {
    checkPermission(path, root, doCheckOwner, ancestorAccess, parentAccess, access, subAccess, false);
  }
  
  /**
   * Check whether current user have permissions to access the path.
   * Traverse is always checked.
   * <p/>
   * Parent path means the parent directory for the path.
   * Ancestor path means the last (the closest) existing ancestor directory
   * of the path.
   * Note that if the parent path exists,
   * then the parent path and the ancestor path are the same.
   * <p/>
   * For example, suppose the path is "/foo/bar/baz".
   * No matter baz is a file or a directory,
   * the parent path is "/foo/bar".
   * If bar exists, then the ancestor path is also "/foo/bar".
   * If bar does not exist and foo exists,
   * then the ancestor path is "/foo".
   * Further, if both foo and bar do not exist,
   * then the ancestor path is "/".
   *
   * @param doCheckOwner
   *     Require user to be the owner of the path?
   * @param ancestorAccess
   *     The access required by the ancestor of the path.
   * @param parentAccess
   *     The access required by the parent of the path.
   * @param access
   *     The access required by the path.
   * @param subAccess
   *     If path is a directory,
   *     it is the access required of the path and all the sub-directories.
   *     If path is not a directory, there is no effect.
   * @throws AccessControlException
   * @throws UnresolvedLinkException
   *     Guarded by {@link FSNamesystem#readLock()}
   *     Caller of this method must hold that lock.
   */
  void checkPermission(String path, INodeDirectory root, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess, boolean resolveLink) throws AccessControlException, UnresolvedLinkException, StorageException,
      TransactionContextException, IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ACCESS CHECK: " + this + ", doCheckOwner=" + doCheckOwner +
          ", ancestorAccess=" + ancestorAccess + ", parentAccess=" +
          parentAccess + ", access=" + access + ", subAccess=" + subAccess + ", resolveLink=" + resolveLink);
    }
    // check if (parentAccess != null) && file exists, then check sb
    // If resolveLink, the check is performed on the link target.
      final INode[] inodes = root.getExistingPathINodes(path, resolveLink).getINodes();
      int ancestorIndex = inodes.length - 2;
      for(; ancestorIndex >= 0 && inodes[ancestorIndex] == null;
          ancestorIndex--);
      checkTraverse(inodes, ancestorIndex);

      final INode last = inodes[inodes.length - 1];
      if (parentAccess != null && parentAccess.implies(FsAction.WRITE)
          && inodes.length > 1 && last != null) {
        checkStickyBit(inodes[inodes.length - 2], last);
      }
      if (ancestorAccess != null && inodes.length > 1) {
        check(inodes, ancestorIndex, ancestorAccess);
      }
      if (parentAccess != null && inodes.length > 1) {
        check(inodes, inodes.length - 2, parentAccess);
      }
      if (access != null) {
        check(last, access);
      }
      if (subAccess != null) {
        checkSubAccess(last, subAccess);
      }
      if (doCheckOwner) {
        checkOwner(last);
      }
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkOwner(INode inode)
      throws IOException {
    if (inode != null && user.equals(inode.getUserName())) {
      return;
    }
    throw new AccessControlException("Permission denied");
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkTraverse(INode[] inodes, int last)
      throws IOException {
    for (int j = 0; j <= last; j++) {
      check(inodes[j], FsAction.EXECUTE);
    }
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkSubAccess(INode inode, FsAction access)
      throws IOException {
    if (inode == null || !inode.isDirectory()) {
      return;
    }

    Stack<INodeDirectory> directories = new Stack<>();
    for (directories.push(inode.asDirectory()); !directories.isEmpty(); ) {
      INodeDirectory d = directories.pop();
      check(d, access);

      for (INode child : d.getChildrenList()) {
        if (child.isDirectory()) {
          directories.push(child.asDirectory());
        }
      }
    }
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void check(INode[] inodes, int i, FsAction access)
      throws IOException {
    check(i >= 0 ? inodes[i] : null, access);
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  void check(INode inode, FsAction access)
      throws IOException {
    if (inode == null) {
      return;
    }
    FsPermission mode = inode.getFsPermission();
  
    AclFeature aclFeature = inode.getAclFeature();//snapshotId);
    if (aclFeature != null) {
      List<AclEntry> featureEntries = aclFeature.getEntries();
      // It's possible that the inode has a default ACL but no access ACL.
      if (featureEntries.get(0).getScope() == AclEntryScope.ACCESS) {
        checkAccessAcl(inode, access, mode, featureEntries);
        return;
      }
    }
    //checkFsPermission(inode, access, mode); //TODO maybe remove this call
    
    check(inode, access, mode, inode.getUserName(),
        inode.getGroupName());
    
  }
  
  void check(INode inode, FsAction access, List<AclEntry> aclEntries) throws IOException {
    if (inode == null){
      return;
    }
    
    FsPermission mode = inode.getFsPermission();
    if (aclEntries != null && !aclEntries.isEmpty()){
      if (aclEntries.get(0).getScope() == AclEntryScope.ACCESS) {
        checkAccessAcl(inode, access, mode, aclEntries);
        return;
      }
    }
    check(inode, access, mode, inode.getUserName(),
        inode.getGroupName());
  }

  /**
   * Checks requested access against an Access Control List.  This method relies
   * on finding the ACL data in the relevant portions of {@link FsPermission} and
   * {@link AclFeature} as implemented in the logic of {@link AclStorage}.  This
   * method also relies on receiving the ACL entries in sorted order.  This is
   * assumed to be true, because the ACL modification methods in
   * {@link AclTransformation} sort the resulting entries.
   *
   * More specifically, this method depends on these invariants in an ACL:
   * - The list must be sorted.
   * - Each entry in the list must be unique by scope + type + name.
   * - There is exactly one each of the unnamed user/group/other entries.
   * - The mask entry must not have a name.
   * - The other entry must not have a name.
   * - Default entries may be present, but they are ignored during enforcement.
   *
   * @param inode INode accessed inode
   * @param access FsAction requested permission
   * @param mode FsPermission mode from inode
   * @param featureEntries List<AclEntry> ACL entries from AclFeature of inode
   * @throws AccessControlException if the ACL denies permission
   */
  private void checkAccessAcl(INode inode, FsAction access,
      FsPermission mode, List<AclEntry> featureEntries)
      throws IOException {
    boolean foundMatch = false;

    // Use owner entry from permission bits if user is owner.
    if (user.equals(inode.getUserName())) {
      if (mode.getUserAction().implies(access)) {
        return;
      }
      foundMatch = true;
    }

    // Check named user and group entries if user was not denied by owner entry.
    if (!foundMatch) {
      for (AclEntry entry: featureEntries) {
        if (entry.getScope() == AclEntryScope.DEFAULT) {
          break;
        }
        AclEntryType type = entry.getType();
        String name = entry.getName();
        if (type == AclEntryType.USER) {
          // Use named user entry with mask from permission bits applied if user
          // matches name.
          if (user.equals(name)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
            break;
          }
        } else if (type == AclEntryType.GROUP) {
          // Use group entry (unnamed or named) with mask from permission bits
          // applied if user is a member and entry grants access.  If user is a
          // member of multiple groups that have entries that grant access, then
          // it doesn't matter which is chosen, so exit early after first match.
          String group = name == null ? inode.getGroupName() : name;
          if (groups.contains(group)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
          }
        }
      }
    }

    // Use other entry if user was not denied by an earlier match.
    if (!foundMatch && mode.getOtherAction().implies(access)) {
      return;
    }

    throw new AccessControlException(
      toAccessControlString(inode, access, mode, featureEntries));
  }
  
  private void checkAccessAcl(ProjectedINode inode, FsAction access,
      FsPermission mode, List<AclEntry> featureEntries)
      throws IOException {
    boolean foundMatch = false;
    
    // Use owner entry from permission bits if user is owner.
    if (user.equals(inode.getUserName())) {
      if (mode.getUserAction().implies(access)) {
        return;
      }
      foundMatch = true;
    }
    
    // Check named user and group entries if user was not denied by owner entry.
    if (!foundMatch) {
      for (AclEntry entry: featureEntries) {
        if (entry.getScope() == AclEntryScope.DEFAULT) {
          break;
        }
        AclEntryType type = entry.getType();
        String name = entry.getName();
        if (type == AclEntryType.USER) {
          // Use named user entry with mask from permission bits applied if user
          // matches name.
          if (user.equals(name)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
            break;
          }
        } else if (type == AclEntryType.GROUP) {
          // Use group entry (unnamed or named) with mask from permission bits
          // applied if user is a member and entry grants access.  If user is a
          // member of multiple groups that have entries that grant access, then
          // it doesn't matter which is chosen, so exit early after first match.
          String group = name == null ? inode.getGroupName() : name;
          if (groups.contains(group)) {
            FsAction masked = entry.getPermission().and(mode.getGroupAction());
            if (masked.implies(access)) {
              return;
            }
            foundMatch = true;
          }
        }
      }
    }
    
    // Use other entry if user was not denied by an earlier match.
    if (!foundMatch && mode.getOtherAction().implies(access)) {
      return;
    }
    
    throw new AccessControlException(
        toAccessControlString(inode, access, mode, featureEntries));
  }

  void check(ProjectedINode inode, FsAction access, List<AclEntry> aclEntries) throws IOException {
    if (inode == null) {
      return;
    }
    
    FsPermission mode = new FsPermission(inode.getPermission());
    if (aclEntries != null && !aclEntries.isEmpty()){
      if (aclEntries.get(0).getScope() == AclEntryScope.ACCESS) {
        checkAccessAcl(inode, access, mode, aclEntries);
        return;
      }
    }
    
    if(!check(inode.getId(), access, mode, inode
        .getUserName(), inode.getGroupName())){
      throw new AccessControlException(
          "Permission denied: user=" + user + ", access=" + access + ", inode=" + toAccessControlString(inode));
    }
  }

  void check(INode inode, FsAction access, FsPermission mode, String userName,
      String groupName) throws AccessControlException, TransactionContextException, IOException {
    if (!check(inode.getId(), access, mode, userName, groupName)) {
      throw new AccessControlException(
          "Permission denied: user=" + user + ", access=" + access + ", inode=" + toAccessControlString(inode));
    }
  }
  
  boolean check(int inodeId, FsAction access, FsPermission mode, String userName,
      String groupName) throws AccessControlException, TransactionContextException, IOException {
    if (user.equals(userName)) { //user class
      if (mode.getUserAction().implies(access)) {
        return true;
      }
    } else if (groups.contains(groupName)) { //group class
      if (mode.getGroupAction().implies(access)) {
        return true;
      }
    } else { //other class
      if (mode.getOtherAction().implies(access)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Guarded by {@link FSNamesystem#readLock()}
   */
  private void checkStickyBit(INode parent, INode inode)
      throws IOException {
    if (!parent.getFsPermission().getStickyBit()) {
      return;
    }

    // If this user is the directory owner, return
    if (parent.getUserName().equals(user)) {
      return;
    }

    // if this user is the file owner, return
    if (inode.getUserName().equals(user)) {
      return;
    }

    throw new AccessControlException(
        "Permission denied by sticky bit setting:" +
            " user=" + user + ", inode=" + inode);
  }
}
