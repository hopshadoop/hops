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
package org.apache.hadoop.hdfs.tools;

import com.google.common.base.Joiner;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.tools.GetConf.Command;
import org.apache.hadoop.hdfs.tools.GetConf.CommandHandler;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link GetConf}
 */
public class TestGetConf {
  enum TestType {
    NAMENODE,
    NNRPCADDRESSES
  }

  FileSystem localFileSys; 
  
  /*
   * Convert the list returned from DFSUtil functions to an array of
   * addresses represented as "host:port"
   */
  private String[] toStringArray(List<InetSocketAddress> list) {
    String[] ret = new String[list.size()];
    for (int i = 0; i < list.size(); i++) {
      ret[i] = NetUtils.getHostPortString(list.get(i));
    }
    return ret;
  }

  /**
   * Using DFSUtil methods get the list of given {@code type} of address
   */
  private List<InetSocketAddress> getAddressListFromConf(TestType type,
      HdfsConfiguration conf) throws IOException {
    switch (type) {
      case NAMENODE:
        return DFSUtil.getNameNodesServiceRpcAddresses(conf);
      case NNRPCADDRESSES:
        return DFSUtil.getNameNodesServiceRpcAddresses(conf);
    }
    return null;
  }
  
  private String runTool(HdfsConfiguration conf, String[] args, boolean success)
      throws Exception {
    ByteArrayOutputStream o = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(o, true);
    try {
      int ret = ToolRunner.run(new GetConf(conf, out, out), args);
      out.flush();
      System.err.println("Output: " + o.toString());
      assertEquals("Expected " + (success ? "success" : "failure") +
              " for args: " + Joiner.on(" ").join(args) + "\n" +
              "Output: " + o.toString(), success, ret == 0);
      return o.toString();
    } finally {
      o.close();
      out.close();
    }
  }
  
  /**
   * Get address list for a given type of address. Command expected to
   * fail if {@code success} is false.
   *
   * @return returns the success or error output from the tool.
   */
  private String getAddressListFromTool(TestType type, HdfsConfiguration conf,
      boolean success) throws Exception {
    String[] args = new String[1];
    switch (type) {
      case NAMENODE:
        args[0] = Command.NAMENODE.getName();
        break;
      case NNRPCADDRESSES:
        args[0] = Command.NNRPCADDRESSES.getName();
        break;
    }
    return runTool(conf, args, success);
  }

  /**
   * Using {@link GetConf} methods get the list of given {@code type} of
   * addresses
   *
   * @param type,
   *     TestType
   * @param conf,
   *     configuration
   * @param checkPort,
   *     If checkPort is true, verify NNPRCADDRESSES whose
   *     expected value is hostname:rpc-port.  If checkPort is false, the
   *     expected is hostname only.
   * @param expected,
   *     expected addresses
   */
  private void getAddressListFromTool(TestType type, HdfsConfiguration conf,
      boolean checkPort, List<InetSocketAddress> expected) throws Exception {
    String out = getAddressListFromTool(type, conf, expected.size() != 0);
    List<String> values = new ArrayList<>();
    
    // Convert list of addresses returned to an array of string
    StringTokenizer tokenizer = new StringTokenizer(out);
    while (tokenizer.hasMoreTokens()) {
      String s = tokenizer.nextToken().trim();
      values.add(s);
    }
    String[] actual = values.toArray(new String[values.size()]);

    // Convert expected list to String[] of hosts
    int i = 0;
    String[] expectedHosts = new String[expected.size()];
    for (InetSocketAddress addr : expected) {
      if (!checkPort) {
        expectedHosts[i++] = addr.getHostName();
      } else {
        expectedHosts[i++] = addr.getHostName() + ":" + addr.getPort();
      }
    }

    // Compare two arrays
    assertTrue(Arrays.equals(expectedHosts, actual));
  }

  private void verifyAddresses(HdfsConfiguration conf, TestType type,
      boolean checkPort, String... expected) throws Exception {
    // Ensure DFSUtil returned the right set of addresses
    List<InetSocketAddress> list = getAddressListFromConf(type, conf);
    String[] actual = toStringArray(list);
    Arrays.sort(actual);
    Arrays.sort(expected);
    assertTrue(Arrays.equals(expected, actual));

    // Test GetConf returned addresses
    getAddressListFromTool(type, conf, checkPort, list);
  }

  private static String getNameServiceId(int index) {
    return "ns" + index;
  }

  /**
   * Test empty configuration
   */
  @Test
  public void testEmptyConf() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);
    // Verify getting addresses fails
    getAddressListFromTool(TestType.NAMENODE, conf, false);
    getAddressListFromTool(TestType.NNRPCADDRESSES, conf, false);
    for (Command cmd : Command.values()) {
      String arg = cmd.getName();
      CommandHandler handler = Command.getHandler(arg);
      assertNotNull("missing handler: " + cmd, handler);
      if (handler.key != null) {
        // First test with configuration missing the required key
        String[] args = {handler.key};
        runTool(conf, args, false);
      }
    }
  }
  
  /**
   * Test invalid argument to the tool
   */
  @Test
  public void testInvalidArgument() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    String[] args = {"-invalidArgument"};
    String ret = runTool(conf, args, false);
    assertTrue(ret.contains(GetConf.USAGE));
  }

  /**
   * Tests to make sure the returned addresses are correct in case of default
   * configuration with no federation
   */
  @Test
  public void testNonFederation() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);

    // Returned namenode address should match default address
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://localhost:1000");
    verifyAddresses(conf, TestType.NAMENODE, false, "localhost:1000");
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, "localhost:1000");

    // Returned namenode address should match service RPC address
    conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "localhost:1000");
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:1001");
    verifyAddresses(conf, TestType.NAMENODE, false, "localhost:1000");
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, "localhost:1000");

    // Returned address should match RPC address
    conf = new HdfsConfiguration();
    conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, "localhost:1001");
    verifyAddresses(conf, TestType.NAMENODE, false, "localhost:1001");
    verifyAddresses(conf, TestType.NNRPCADDRESSES, true, "localhost:1001");
  }

  @Test
  public void testGetSpecificKey() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set("mykey", " myval ");
    String[] args = {"-confKey", "mykey"};
    assertTrue(runTool(conf, args, true).equals("myval\n"));
  }
  
  @Test
  public void testExtraArgsThrowsError() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set("mykey", "myval");
    String[] args = {"-namenodes", "unexpected-arg"};
    assertTrue(runTool(conf, args, false)
        .contains("Did not expect argument: unexpected-arg"));
  }

  /**
   * Tests commands other than {@link Command#NAMENODE}, {@link
   * Command#BACKUP},
   * {@link Command#SECONDARY} and {@link Command#NNRPCADDRESSES}
   */
  @Test
  public void testTool() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration(false);
    for (Command cmd : Command.values()) {
      CommandHandler handler = Command.getHandler(cmd.getName());
      if (handler.key != null && !"-confKey".equals(cmd.getName())) {
        // Add the key to the conf and ensure tool returns the right value
        String[] args = {cmd.getName()};
        conf.set(handler.key, "value");
        assertTrue(runTool(conf, args, true).contains("value"));
      }
    }
  }
  
  @Test
  public void TestGetConfExcludeCommand() throws Exception{
  	HdfsConfiguration conf = new HdfsConfiguration();
    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir = new Path(workingDir, System.getProperty("test.build.data", "target/test/data") + "/Getconf/");
    Path hostsFile = new Path(dir, "hosts");
    Path excludeFile = new Path(dir, "exclude");
    
    // Setup conf
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    writeConfigFile(hostsFile, null);
    writeConfigFile(excludeFile, null);    
    String[] args = {"-excludeFile"};
    String ret = runTool(conf, args, true);
    assertEquals(excludeFile.toUri().getPath(),ret.trim());
    cleanupFile(localFileSys, excludeFile.getParent());
  }
  
  @Test
  public void TestGetConfIncludeCommand() throws Exception{
  	HdfsConfiguration conf = new HdfsConfiguration();
    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    Path dir = new Path(workingDir, System.getProperty("test.build.data", "target/test/data") + "/Getconf/");
    Path hostsFile = new Path(dir, "hosts");
    Path excludeFile = new Path(dir, "exclude");
    
    // Setup conf
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    writeConfigFile(hostsFile, null);
    writeConfigFile(excludeFile, null);    
    String[] args = {"-includeFile"};
    String ret = runTool(conf, args, true);
    assertEquals(hostsFile.toUri().getPath(),ret.trim());
    cleanupFile(localFileSys, excludeFile.getParent());
  }
  
  private void writeConfigFile(Path name, ArrayList<String> nodes) 
      throws IOException {
      // delete if it already exists
      if (localFileSys.exists(name)) {
        localFileSys.delete(name, true);
      }

      FSDataOutputStream stm = localFileSys.create(name);
      
      if (nodes != null) {
        for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
          String node = it.next();
          stm.writeBytes(node);
          stm.writeBytes("\n");
        }
      }
      stm.close();
    }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }
}
