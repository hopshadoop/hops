/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.ipc.RemoteException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class TestLargeXAttr {
  
  @Test
  public void testConfiguration() throws Exception {
    int maxSize = XAttrStorage.getMaxXAttrSize();
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, maxSize + 1);
    try{
      final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1)
          .build();
      fail("Should throw an exception if the xattr size is larger than the " +
          "hard limit.");
    }catch (IllegalArgumentException ex){
      assertEquals("The maximum size of an xattr should be <= maximum size " +
          "hard limit " + maxSize + ": (dfs.namenode.fs-limits.max-xattr-size).",
          ex.getMessage());
    }
  
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, 0);
    
    try{
      final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1)
          .build();
      fail("Should throw an exception if the xattr size is 0");
    }catch (IllegalArgumentException ex){
      assertEquals("The maximum size of an xattr should be > 0: (dfs.namenode" +
              ".fs-limits.max-xattr-size).", ex.getMessage());
    }
  }
  
  @Test
  public void testSetXAttrLargerThanConfiguredSize() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY,
        XAttrStorage.getDefaultXAttrSize());
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    try{
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path dir = new Path("/dir");
      dfs.mkdirs(dir);
      
      String name = "testXAttr";
      byte[] value = XAttrTestHelpers.generateRandomByteArray(XAttrStorage.getDefaultXAttrValueSize() + 1);
      try{
        dfs.setXAttr(dir, "user." + name, value);
        fail("Should fail to setXAttr with value larger than the configured " +
            "limit");
      }catch (RemoteException ex){
        assertEquals(HadoopIllegalArgumentException.class.getCanonicalName(), ex.getClassName());
        assertTrue(ex.getMessage().contains("The XAttr value is too big. The " +
            "maximum size of the value is " + XAttrStorage.getDefaultXAttrValueSize() + ", but " +
            "the value size is " + value.length));
      }
  
      name = XAttrTestHelpers.generateRandomXAttrName(XAttrStorage.getMaxXAttrNameSize() + 1);
      value = XAttrTestHelpers.generateRandomByteArray(100);
      
      try{
        dfs.setXAttr(dir, "user." + name, value);
        fail("Should fail to setXAttr with name larger than the maximum limit");
      }catch (RemoteException ex){
        assertEquals(HadoopIllegalArgumentException.class.getCanonicalName(), ex.getClassName());
        assertTrue(ex.getMessage().contains("The XAttr name is too big. The " +
            "maximum size of the name is " +XAttrStorage.getMaxXAttrNameSize()
            + ", but the name size is " +  XAttrStorage.getXAttrByteSize(name)));
      }
      
    }finally {
      if(cluster != null){
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testXAttr135KB() throws Exception {
    testXAttr(10, 5);
  }
  
  @Test
  public void testXAttr1_35MB() throws Exception {
    testXAttr(100, 5);
  }
  
  @Test
  public void testXAttr3_44MB() throws Exception {
    testXAttr(255,5);
  }
  
  private void testXAttr(int rows, int numXAttrs) throws Exception {
    int MAX_VALUE_SIZE = rows * XAttrStorage.getDefaultXAttrValueSize();
    int MAX_SIZE = XAttrStorage.getMaxXAttrNameSize() + MAX_VALUE_SIZE;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, numXAttrs);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, MAX_SIZE);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    try{
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path dir = new Path("/dir");
      dfs.mkdirs(dir);
      
      String nameBase = "user.test";
      Map<String, byte[]> testXAttrs = new HashMap<>();
      
      // setXAttrs
      for(int i=0; i < numXAttrs; i++){
        String name = nameBase+ i;
        int k = RandomUtils.nextInt(0, 101);
        byte[] value =
            XAttrTestHelpers.generateRandomByteArray(MAX_VALUE_SIZE - k);
        testXAttrs.put(name, value);
        dfs.setXAttr(dir, name, value);
        byte[] returnedValue = dfs.getXAttr(dir, name);
        assertArrayEquals(value, returnedValue);
      }
      
      assertEquals(numXAttrs * rows, XAttrTestHelpers.getXAttrTableRowCount());
      
      //get all xattrs attached to dir
      Map<String, byte[]> returnedXAttrs =dfs.getXAttrs(dir);
      assertEquals(numXAttrs, returnedXAttrs.size());
      for(Map.Entry<String, byte[]> entry : testXAttrs.entrySet()){
        assertArrayEquals(returnedXAttrs.get(entry.getKey()), entry.getValue());
      }
  
      // get xattrs by their name
      returnedXAttrs = dfs.getXAttrs(dir,
          new ArrayList<String>(testXAttrs.keySet()));
      assertEquals(numXAttrs, returnedXAttrs.size());
      for(Map.Entry<String, byte[]> entry : testXAttrs.entrySet()){
        assertArrayEquals(returnedXAttrs.get(entry.getKey()), entry.getValue());
      }
  
      // replace all xattrs
      for(int i=0; i < numXAttrs; i++){
        String name = nameBase+ i;
        int k = RandomUtils.nextInt(0, 101);
        byte[] value = XAttrTestHelpers.generateRandomByteArray(MAX_VALUE_SIZE - k);
        testXAttrs.put(name, value);
        dfs.setXAttr(dir, name, value, EnumSet.of(XAttrSetFlag.REPLACE));
        byte[] returnedValue = dfs.getXAttr(dir, name);
        assertArrayEquals(value, returnedValue);
      }
  
      assertEquals(numXAttrs * rows, XAttrTestHelpers.getXAttrTableRowCount());
      
      //get all xattrs attached to dir
      returnedXAttrs =dfs.getXAttrs(dir);
      assertEquals(numXAttrs, returnedXAttrs.size());
      for(Map.Entry<String, byte[]> entry : testXAttrs.entrySet()){
        assertArrayEquals(returnedXAttrs.get(entry.getKey()), entry.getValue());
      }
      
      // remove xattrs one by one
      for(int i=0; i< numXAttrs; i++){
        String name = nameBase + i;
        dfs.removeXAttr(dir, name);
        returnedXAttrs = dfs.getXAttrs(dir);
        assertEquals(numXAttrs - (i+1), returnedXAttrs.size());
        for(Map.Entry<String, byte[]> entry : returnedXAttrs.entrySet()){
          assertArrayEquals(entry.getValue(), testXAttrs.get(entry.getKey()));
        }
      }
  
      assertEquals(0, XAttrTestHelpers.getXAttrTableRowCount());
    }finally {
      if(cluster != null){
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testReplaceLargeXAttr2() throws Exception {
    testReplaceLargeXAttr(2, 1);
  }
  
  @Test
  public void testReplaceLargeXAttr10() throws Exception {
    testReplaceLargeXAttr(10, 2);
  }
  
  @Test
  public void testReplaceLargeXAttr10ForAll() throws Exception {
    int rows = 10;
    for(int row=1; row <= rows; row++){
      testReplaceLargeXAttr(rows, row);
    }
  }
  
  private void testReplaceLargeXAttr(final int rows, final int replaceRows) throws Exception {
    final int MAX_VALUE_ROW_SIZE = XAttrStorage.getDefaultXAttrValueSize();
    final int MAX_VALUE_SIZE = rows * MAX_VALUE_ROW_SIZE;
    final int MAX_SIZE = XAttrStorage.getMaxXAttrNameSize() + MAX_VALUE_SIZE;
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 5);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY, MAX_SIZE);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    try{
      DistributedFileSystem dfs = cluster.getFileSystem();
      Path dir = new Path("/dir");
      dfs.mkdirs(dir);
      
      String name = "user.test";
      
      // setXAttrs
      byte[] value =
          XAttrTestHelpers.generateRandomByteArray(MAX_VALUE_SIZE);
      dfs.setXAttr(dir, name, value);
      byte[] returnedValue = dfs.getXAttr(dir, name);
      assertArrayEquals(value, returnedValue);
  
      assertEquals(rows, XAttrTestHelpers.getXAttrTableRowCount());
  
      // update xattr
      byte[] value2 =
          XAttrTestHelpers.generateRandomByteArray(MAX_VALUE_ROW_SIZE * replaceRows);
      dfs.setXAttr(dir, name, value2, EnumSet.of(XAttrSetFlag.REPLACE));
      returnedValue = dfs.getXAttr(dir, name);
      assertArrayEquals(value2, returnedValue);
  
      assertEquals(replaceRows, XAttrTestHelpers.getXAttrTableRowCount());
      
    }finally {
      if(cluster != null){
        cluster.shutdown();
      }
    }
  }
}
