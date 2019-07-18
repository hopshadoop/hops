/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAclOperationException;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * AbfsAclHelper provides convenience methods to implement modifyAclEntries / removeAclEntries / removeAcl / removeDefaultAcl
 * from setAcl and getAcl.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class AbfsAclHelper {

  private AbfsAclHelper() {
    // not called
  }

  public static Map<String, String> deserializeAclSpec(final String aclSpecString) {
    final Map<String, String> aclEntries  = new HashMap<>();
    final String[] aclArray = aclSpecString.split(AbfsHttpConstants.COMMA);
    for (String acl : aclArray) {
      int idx = acl.lastIndexOf(AbfsHttpConstants.COLON);
      aclEntries.put(acl.substring(0, idx), acl.substring(idx + 1));
    }
    return aclEntries;
  }

  public static String serializeAclSpec(final Map<String, String> aclEntries) {
    final StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> aclEntry : aclEntries.entrySet()) {
      sb.append(aclEntry.getKey() + AbfsHttpConstants.COLON + aclEntry.getValue() + AbfsHttpConstants.COMMA);
    }
    if (sb.length() > 0) {
      sb.setLength(sb.length() - 1);
    }
    return sb.toString();
  }

  public static String processAclString(final String aclSpecString) {
    final List<String> aclEntries = Arrays.asList(aclSpecString.split(AbfsHttpConstants.COMMA));
    final StringBuilder sb = new StringBuilder();

    boolean containsMask = false;
    for (int i = aclEntries.size() - 1; i >= 0; i--) {
      String ace = aclEntries.get(i);
      if (ace.startsWith(AbfsHttpConstants.ACCESS_OTHER)|| ace.startsWith(AbfsHttpConstants.ACCESS_USER + AbfsHttpConstants.COLON)) {
        // skip
      } else if (ace.startsWith(AbfsHttpConstants.ACCESS_MASK)) {
        containsMask = true;
        // skip
      } else if (ace.startsWith(AbfsHttpConstants.ACCESS_GROUP + AbfsHttpConstants.COLON) && !containsMask) {
        // skip
      } else {
        sb.insert(0, ace + AbfsHttpConstants.COMMA);
      }
    }

    return sb.length() == 0 ? AbfsHttpConstants.EMPTY_STRING : sb.substring(0, sb.length() - 1);
  }

  public static void removeAclEntriesInternal(Map<String, String> aclEntries, Map<String, String> toRemoveEntries)
      throws AzureBlobFileSystemException {
    boolean accessAclTouched = false;
    boolean defaultAclTouched = false;

    final Set<String> removeIndicationSet = new HashSet<>();

    for (String entryKey : toRemoveEntries.keySet()) {
      final boolean isDefaultAcl = isDefaultAce(entryKey);
      if (removeNamedAceAndUpdateSet(entryKey, isDefaultAcl, removeIndicationSet, aclEntries)) {
        if (isDefaultAcl) {
          defaultAclTouched = true;
        } else {
          accessAclTouched = true;
        }
      }
    }
    if (accessAclTouched) {
      if (removeIndicationSet.contains(AbfsHttpConstants.ACCESS_MASK)) {
        aclEntries.remove(AbfsHttpConstants.ACCESS_MASK);
      }
      recalculateMask(aclEntries, false);
    }
    if (defaultAclTouched) {
      if (removeIndicationSet.contains(AbfsHttpConstants.DEFAULT_MASK)) {
        aclEntries.remove(AbfsHttpConstants.DEFAULT_MASK);
      }
      if (removeIndicationSet.contains(AbfsHttpConstants.DEFAULT_USER)) {
        aclEntries.put(AbfsHttpConstants.DEFAULT_USER, aclEntries.get(AbfsHttpConstants.ACCESS_USER));
      }
      if (removeIndicationSet.contains(AbfsHttpConstants.DEFAULT_GROUP)) {
        aclEntries.put(AbfsHttpConstants.DEFAULT_GROUP, aclEntries.get(AbfsHttpConstants.ACCESS_GROUP));
      }
      if (removeIndicationSet.contains(AbfsHttpConstants.DEFAULT_OTHER)) {
        aclEntries.put(AbfsHttpConstants.DEFAULT_OTHER, aclEntries.get(AbfsHttpConstants.ACCESS_OTHER));
      }
      recalculateMask(aclEntries, true);
    }
  }

  private static boolean removeNamedAceAndUpdateSet(String entry, boolean isDefaultAcl, Set<String> removeIndicationSet,
                                                    Map<String, String> aclEntries)
      throws AzureBlobFileSystemException {
    final int startIndex = isDefaultAcl ? 1 : 0;
    final String[] entryParts = entry.split(AbfsHttpConstants.COLON);
    final String tag = isDefaultAcl ? AbfsHttpConstants.DEFAULT_SCOPE + entryParts[startIndex] + AbfsHttpConstants.COLON
        : entryParts[startIndex] + AbfsHttpConstants.COLON;

    if ((entry.equals(AbfsHttpConstants.ACCESS_USER) || entry.equals(AbfsHttpConstants.ACCESS_GROUP)
        || entry.equals(AbfsHttpConstants.ACCESS_OTHER))
        && !isNamedAce(entry)) {
      throw new InvalidAclOperationException("Cannot remove user, group or other entry from access ACL.");
    }

    boolean touched = false;
    if (!isNamedAce(entry)) {
      removeIndicationSet.add(tag); // this must not be a access user, group or other
      touched = true;
    } else {
      if (aclEntries.remove(entry) != null) {
        touched = true;
      }
    }
    return touched;
  }

  private static void recalculateMask(Map<String, String> aclEntries, boolean isDefaultMask) {
    FsAction umask = FsAction.NONE;
    if (!isExtendAcl(aclEntries, isDefaultMask)) {
      return;
    }

    for (Map.Entry<String, String> aclEntry : aclEntries.entrySet()) {
      if (isDefaultMask) {
        if ((isDefaultAce(aclEntry.getKey()) && isNamedAce(aclEntry.getKey()))
            || aclEntry.getKey().equals(AbfsHttpConstants.DEFAULT_GROUP)) {
          umask = umask.or(FsAction.getFsAction(aclEntry.getValue()));
        }
      } else {
        if ((!isDefaultAce(aclEntry.getKey()) && isNamedAce(aclEntry.getKey()))
            || aclEntry.getKey().equals(AbfsHttpConstants.ACCESS_GROUP)) {
          umask = umask.or(FsAction.getFsAction(aclEntry.getValue()));
        }
      }
    }

    aclEntries.put(isDefaultMask ? AbfsHttpConstants.DEFAULT_MASK : AbfsHttpConstants.ACCESS_MASK, umask.SYMBOL);
  }

  private static boolean isExtendAcl(Map<String, String> aclEntries, boolean checkDefault) {
    for (String entryKey : aclEntries.keySet()) {
      if (checkDefault && !(entryKey.equals(AbfsHttpConstants.DEFAULT_USER)
          || entryKey.equals(AbfsHttpConstants.DEFAULT_GROUP)
          || entryKey.equals(AbfsHttpConstants.DEFAULT_OTHER) || !isDefaultAce(entryKey))) {
        return true;
      }
      if (!checkDefault && !(entryKey.equals(AbfsHttpConstants.ACCESS_USER)
          || entryKey.equals(AbfsHttpConstants.ACCESS_GROUP)
          || entryKey.equals(AbfsHttpConstants.ACCESS_OTHER) || isDefaultAce(entryKey))) {
        return true;
      }
    }
    return false;
  }

  private static boolean isDefaultAce(String entry) {
    return entry.startsWith(AbfsHttpConstants.DEFAULT_SCOPE);
  }

  private static boolean isNamedAce(String entry) {
    return entry.charAt(entry.length() - 1) != AbfsHttpConstants.COLON.charAt(0);
  }
}