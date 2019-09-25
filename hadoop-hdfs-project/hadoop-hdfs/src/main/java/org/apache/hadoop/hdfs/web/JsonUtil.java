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
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * JSON Utilities
 */
public class JsonUtil {
  private static final Object[] EMPTY_OBJECT_ARRAY = {};

  /**
   * Convert a token object to a Json string.
   */
  public static String toJsonString(
      final Token<? extends TokenIdentifier> token) throws IOException {
    return toJsonString(Token.class, toJsonMap(token));
  }

  private static Map<String, Object> toJsonMap(
      final Token<? extends TokenIdentifier> token) throws IOException {
    if (token == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<>();
    m.put("urlString", token.encodeToUrlString());
    return m;
  }

  /** Convert an exception object to a Json string. */
  public static String toJsonString(final Exception e) {
    final Map<String, Object> m = new TreeMap<>();
    m.put("exception", e.getClass().getSimpleName());
    m.put("message", e.getMessage());
    m.put("javaClassName", e.getClass().getName());
    return toJsonString(RemoteException.class, m);
  }

  private static String toJsonString(final Class<?> clazz, final Object value) {
    return toJsonString(clazz.getSimpleName(), value);
  }

  /**
   * Convert a key-value pair to a Json string.
   */
  public static String toJsonString(final String key, final Object value) {
    final Map<String, Object> m = new TreeMap<>();
    m.put(key, value);
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  /**
   * Convert a FsPermission object to a string.
   */
  private static String toString(final FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  /** Convert a HdfsFileStatus object to a Json string. */
  public static String toJsonString(final HdfsFileStatus status,
      boolean includeType) throws IOException {
    if (status == null) {
      return null;
    }
    final Map<String, Object> m = new TreeMap<>();
    m.put("pathSuffix", status.getLocalName());
    m.put("type", WebHdfsConstants.PathType.valueOf(status));
    if (status.isSymlink()) {
      m.put("symlink", status.getSymlink());
    }
  
    m.put("length", status.getLen());
    m.put("owner", status.getOwner());
    m.put("group", status.getGroup());
    FsPermission perm = status.getPermission();
    m.put("permission", toString(perm));
    if (perm.getAclBit()) {
      m.put("aclBit", true);
    }
    if (perm.getEncryptedBit()) {
      m.put("encBit", true);
    }
    m.put("accessTime", status.getAccessTime());
    m.put("modificationTime", status.getModificationTime());
    m.put("blockSize", status.getBlockSize());
    m.put("replication", status.getReplication());
    m.put("fileId", status.getFileId());
    m.put("childrenNum", status.getChildrenNum());
    m.put("storagePolicy", status.getStoragePolicy());
    ObjectMapper mapper = new ObjectMapper();
    try {
      return includeType ?
          toJsonString(FileStatus.class, m) : mapper.writeValueAsString(m);
    } catch (IOException ignored) {
    }
    return null;
  }

  /** Convert an ExtendedBlock to a Json map. */
  private static Map<String, Object> toJsonMap(final ExtendedBlock extendedblock) {
    if (extendedblock == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<>();
    m.put("blockPoolId", extendedblock.getBlockPoolId());
    m.put("blockId", extendedblock.getBlockId());
    m.put("numBytes", extendedblock.getNumBytes());
    m.put("generationStamp", extendedblock.getGenerationStamp());
    m.put("cloudBucketID", extendedblock.getCloudBucketID());
    return m;
  }

  /** Convert a DatanodeInfo to a Json map. */
  static Map<String, Object> toJsonMap(final DatanodeInfo datanodeinfo) {
    if (datanodeinfo == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<>();
    m.put("ipAddr", datanodeinfo.getIpAddr());
    // 'name' is equivalent to ipAddr:xferPort. Older clients (1.x, 0.23.x) 
    // expects this instead of the two fields.
    m.put("name", datanodeinfo.getXferAddr());
    m.put("hostName", datanodeinfo.getHostName());
    m.put("storageID", datanodeinfo.getDatanodeUuid());
    m.put("xferPort", datanodeinfo.getXferPort());
    m.put("infoPort", datanodeinfo.getInfoPort());
    m.put("infoSecurePort", datanodeinfo.getInfoSecurePort());
    m.put("ipcPort", datanodeinfo.getIpcPort());

    m.put("capacity", datanodeinfo.getCapacity());
    m.put("dfsUsed", datanodeinfo.getDfsUsed());
    m.put("remaining", datanodeinfo.getRemaining());
    m.put("blockPoolUsed", datanodeinfo.getBlockPoolUsed());
    m.put("cacheCapacity", datanodeinfo.getCacheCapacity());
    m.put("cacheUsed", datanodeinfo.getCacheUsed());
    m.put("lastUpdate", datanodeinfo.getLastUpdate());
    m.put("lastUpdateMonotonic", datanodeinfo.getLastUpdateMonotonic());
    m.put("xceiverCount", datanodeinfo.getXceiverCount());
    m.put("networkLocation", datanodeinfo.getNetworkLocation());
    m.put("adminState", datanodeinfo.getAdminState().name());
    return m;
  }

  /** Convert a DatanodeInfo[] to a Json array. */
  private static Object[] toJsonArray(final DatanodeInfo[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.length];
      for (int i = 0; i < array.length; i++) {
        a[i] = toJsonMap(array[i]);
      }
      return a;
    }
  }

  /** Convert a LocatedBlock to a Json map. */
  private static Map<String, Object> toJsonMap(final LocatedBlock locatedblock
      ) throws IOException {
    if (locatedblock == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<>();
    m.put("blockToken", toJsonMap(locatedblock.getBlockToken()));
    m.put("isCorrupt", locatedblock.isCorrupt());
    m.put("startOffset", locatedblock.getStartOffset());
    m.put("block", toJsonMap(locatedblock.getBlock()));
    m.put("locations", toJsonArray(locatedblock.getLocations()));
    m.put("cachedLocations", toJsonArray(locatedblock.getCachedLocations()));
    return m;
  }

  /** Convert a LocatedBlock[] to a Json array. */
  private static Object[] toJsonArray(final List<LocatedBlock> array
      ) throws IOException {
    if (array == null) {
      return null;
    } else if (array.size() == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.size()];
      for (int i = 0; i < array.size(); i++) {
        a[i] = toJsonMap(array.get(i));
      }
      return a;
    }
  }

  /** Convert LocatedBlocks to a Json string. */
  public static String toJsonString(final LocatedBlocks locatedblocks
      ) throws IOException {
    if (locatedblocks == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<>();
    m.put("fileLength", locatedblocks.getFileLength());
    m.put("isUnderConstruction", locatedblocks.isUnderConstruction());

    m.put("locatedBlocks", toJsonArray(locatedblocks.getLocatedBlocks()));
    m.put("lastLocatedBlock", toJsonMap(locatedblocks.getLastLocatedBlock()));
    m.put("isLastBlockComplete", locatedblocks.isLastBlockComplete());
    return toJsonString(LocatedBlocks.class, m);
  }

  /** Convert a ContentSummary to a Json string. */
  public static String toJsonString(final ContentSummary contentsummary) {
    if (contentsummary == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<>();
    m.put("length", contentsummary.getLength());
    m.put("fileCount", contentsummary.getFileCount());
    m.put("directoryCount", contentsummary.getDirectoryCount());
    m.put("quota", contentsummary.getQuota());
    m.put("spaceConsumed", contentsummary.getSpaceConsumed());
    m.put("spaceQuota", contentsummary.getSpaceQuota());
    return toJsonString(ContentSummary.class, m);
  }

  /** Convert a MD5MD5CRC32FileChecksum to a Json string. */
  public static String toJsonString(final MD5MD5CRC32FileChecksum checksum) {
    if (checksum == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<>();
    m.put("algorithm", checksum.getAlgorithmName());
    m.put("length", checksum.getLength());
    m.put("bytes", StringUtils.byteToHexString(checksum.getBytes()));
    return toJsonString(FileChecksum.class, m);
  }

  /** Convert a AclStatus object to a Json string. */
  public static String toJsonString(final AclStatus status) {
    if (status == null) {
      return null;
    }

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("owner", status.getOwner());
    m.put("group", status.getGroup());
    m.put("stickyBit", status.isStickyBit());

    final List<String> stringEntries = new ArrayList<>();
    for (AclEntry entry : status.getEntries()) {
      stringEntries.add(entry.toString());
    }
    m.put("entries", stringEntries);

    FsPermission perm = status.getPermission();
    if (perm != null) {
      m.put("permission", toString(perm));
      if (perm.getAclBit()) {
        m.put("aclBit", true);
      }
      if (perm.getEncryptedBit()) {
        m.put("encBit", true);
      }
    }
    final Map<String, Map<String, Object>> finalMap =
        new TreeMap<String, Map<String, Object>>();
    finalMap.put(AclStatus.class.getSimpleName(), m);

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(finalMap);
    } catch (IOException ignored) {
    }
    return null;
  }
  
  
  public static String toJsonString(final List<XAttr> xAttrs)
      throws IOException {
    final List<String> names = Lists.newArrayListWithCapacity(xAttrs.size());
    for (XAttr xAttr : xAttrs) {
      names.add(XAttrHelper.getPrefixName(xAttr));
    }
    ObjectMapper mapper = new ObjectMapper();
    String ret = mapper.writeValueAsString(names);
    final Map<String, Object> finalMap = new TreeMap<String, Object>();
    finalMap.put("XAttrNames", ret);
    return mapper.writeValueAsString(finalMap);
  }
  
  private static Map<String, Object> toJsonMap(final XAttr xAttr,
      final XAttrCodec encoding) throws IOException {
    if (xAttr == null) {
      return null;
    }
 
    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put("name", XAttrHelper.getPrefixName(xAttr));
    m.put("value", xAttr.getValue() != null ?
        XAttrCodec.encodeValue(xAttr.getValue(), encoding) : null);
    return m;
  }
  
  private static Object[] toJsonArray(final List<XAttr> array,
      final XAttrCodec encoding) throws IOException {
    if (array == null) {
      return null;
    } else if (array.size() == 0) {
      return EMPTY_OBJECT_ARRAY;
    } else {
      final Object[] a = new Object[array.size()];
      for(int i = 0; i < array.size(); i++) {
        a[i] = toJsonMap(array.get(i), encoding);
      }
      return a;
    }
  }
  
  public static String toJsonString(final List<XAttr> xAttrs,
      final XAttrCodec encoding) throws IOException {
    final Map<String, Object> finalMap = new TreeMap<String, Object>();
    finalMap.put("XAttrs", toJsonArray(xAttrs, encoding));
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(finalMap);
  }
  
  public static byte[] getXAttr(final Map<?, ?> json, final String name)
      throws IOException {
    if (json == null) {
      return null;
    }
    
    Map<String, byte[]> xAttrs = toXAttrs(json);
    if (xAttrs != null) {
      return xAttrs.get(name);
    }
    
    return null;
  }
  
  static List<?> getList(Map<?, ?> m, String key) {
    Object list = m.get(key);
    if (list instanceof List<?>) {
      return (List<?>) list;
    } else {
      return null;
    }
  }
    
  public static Map<String, byte[]> toXAttrs(final Map<?, ?> json)
      throws IOException {
    if (json == null) {
      return null;
    }

    return toXAttrMap(getList(json, "XAttrs"));
  }
  
  public static List<String> toXAttrNames(final Map<?, ?> json)
      throws IOException {
    if (json == null) {
      return null;
    }
    
    final String namesInJson = (String) json.get("XAttrNames");
    ObjectReader reader = new ObjectMapper().reader(List.class);
    final List<Object> xattrs = reader.readValue(namesInJson);
    final List<String> names =
        Lists.newArrayListWithCapacity(json.keySet().size());
    
    for (Object xattr : xattrs) {
      names.add((String) xattr);
    }
    return names;
  }
  
  private static Map<String, byte[]> toXAttrMap(final List<?> objects)
      throws IOException {
    if (objects == null) {
      return null;
    } else if (objects.isEmpty()) {
      return Maps.newHashMap();
    } else {
      final Map<String, byte[]> xAttrs = Maps.newHashMap();
      for (Object object : objects) {
        Map<?, ?> m = (Map<?, ?>) object;
        String name = (String) m.get("name");
        String value = (String) m.get("value");
        xAttrs.put(name, decodeXAttrValue(value));
      }
      return xAttrs;
    }
  }
  
  private static byte[] decodeXAttrValue(String value) throws IOException {
    if (value != null) {
      return XAttrCodec.decodeValue(value);
    } else {
      return new byte[0];
    }
  }
}
