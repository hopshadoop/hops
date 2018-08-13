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

package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_PORT_DEFAULT;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDFSUtil {
  
  /**
   * Reset to default UGI settings since some tests change them.
   */
  @Before
  public void resetUGI() {
    UserGroupInformation.setConfiguration(new Configuration());
  }
  
  /**
   * Test conversion of LocatedBlock to BlockLocation
   */
  @Test
  public void testLocatedBlocks2Locations() {
    DatanodeInfo d = DFSTestUtil.getLocalDatanodeInfo();
    DatanodeInfo[] ds = new DatanodeInfo[1];
    ds[0] = d;

    // ok
    ExtendedBlock b1 = new ExtendedBlock("bpid", 1, 1, 1);
    LocatedBlock l1 = new LocatedBlock(b1, ds, 0, false);

    // corrupt
    ExtendedBlock b2 = new ExtendedBlock("bpid", 2, 1, 1);
    LocatedBlock l2 = new LocatedBlock(b2, ds, 0, true);

    List<LocatedBlock> ls = Arrays.asList(l1, l2);
    LocatedBlocks lbs = new LocatedBlocks(10, false, ls, l2, true);

    BlockLocation[] bs = DFSUtil.locatedBlocks2Locations(lbs);

    assertTrue("expected 2 blocks but got " + bs.length, bs.length == 2);

    int corruptCount = 0;
    for (BlockLocation b : bs) {
      if (b.isCorrupt()) {
        corruptCount++;
      }
    }

    assertTrue("expected 1 corrupt files but got " + corruptCount,
        corruptCount == 1);

    // test an empty location
    bs = DFSUtil.locatedBlocks2Locations(new LocatedBlocks());
    assertEquals(0, bs.length);
  }
  
  @Test
  public void testGetInfoServer() throws IOException, URISyntaxException {
    HdfsConfiguration conf = new HdfsConfiguration();
    
    URI httpsport = DFSUtil.getInfoServer(null, conf, "https");
    assertEquals(new URI("https", null, "0.0.0.0",
        DFS_NAMENODE_HTTPS_PORT_DEFAULT, null, null, null), httpsport);
    
    URI httpport = DFSUtil.getInfoServer(null, conf, "http");
    assertEquals(new URI("http", null, "0.0.0.0",
        DFS_NAMENODE_HTTP_PORT_DEFAULT, null, null, null), httpport);

    URI httpAddress = DFSUtil.getInfoServer(new InetSocketAddress(
        "localhost", 8020), conf, "http");
    assertEquals(
        URI.create("http://localhost:" + DFS_NAMENODE_HTTP_PORT_DEFAULT),
        httpAddress);
  }

  @Test
  public void testSubstituteForWildcardAddress() throws IOException {
    assertEquals("foo:12345",
        DFSUtil.substituteForWildcardAddress("0.0.0.0:12345", "foo"));
    assertEquals("127.0.0.1:12345",
        DFSUtil.substituteForWildcardAddress("127.0.0.1:12345", "foo"));
  }
  
  @Test (timeout=15000)
  public void testIsValidName() {
    assertFalse(DFSUtil.isValidName("/foo/../bar"));
    assertFalse(DFSUtil.isValidName("/foo//bar"));
    assertTrue(DFSUtil.isValidName("/"));
    assertTrue(DFSUtil.isValidName("/bar/"));
  }
  
  @Test(timeout = 5000)
  public void testGetSpnegoKeytabKey() {
    HdfsConfiguration conf = new HdfsConfiguration();
    String defaultKey = "default.spengo.key";
    conf.unset(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY);
    assertEquals("Test spnego key in config is null", defaultKey,
        DFSUtil.getSpnegoKeytabKey(conf, defaultKey));

    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY, "");
    assertEquals("Test spnego key is empty", defaultKey,
        DFSUtil.getSpnegoKeytabKey(conf, defaultKey));

    String spengoKey = "spengo.key";
    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        spengoKey);
    assertEquals("Test spnego key is NOT null",
        DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
        DFSUtil.getSpnegoKeytabKey(conf, defaultKey));
  }
  
  @Test(timeout=1000)
  public void testDurationToString() throws Exception {
    assertEquals("000:00:00:00.000", DFSUtil.durationToString(0));
    assertEquals("001:01:01:01.000",
        DFSUtil.durationToString(((24*60*60)+(60*60)+(60)+1)*1000));
    assertEquals("000:23:59:59.999",
        DFSUtil.durationToString(((23*60*60)+(59*60)+(59))*1000+999));
    assertEquals("-001:01:01:01.000",
        DFSUtil.durationToString(-((24*60*60)+(60*60)+(60)+1)*1000));
    assertEquals("-000:23:59:59.574",
        DFSUtil.durationToString(-(((23*60*60)+(59*60)+(59))*1000+574)));
  }

  @Test(timeout=5000)
  public void testRelativeTimeConversion() throws Exception {
    try {
      DFSUtil.parseRelativeTime("1");
    } catch (IOException e) {
      assertExceptionContains("too short", e);
    }
    try {
      DFSUtil.parseRelativeTime("1z");
    } catch (IOException e) {
      assertExceptionContains("unknown time unit", e);
    }
    try {
      DFSUtil.parseRelativeTime("yyz");
    } catch (IOException e) {
      assertExceptionContains("is not a number", e);
    }
    assertEquals(61*1000, DFSUtil.parseRelativeTime("61s"));
    assertEquals(61*60*1000, DFSUtil.parseRelativeTime("61m"));
    assertEquals(0, DFSUtil.parseRelativeTime("0s"));
    assertEquals(25*60*60*1000, DFSUtil.parseRelativeTime("25h"));
    assertEquals(4*24*60*60*1000l, DFSUtil.parseRelativeTime("4d"));
    assertEquals(999*24*60*60*1000l, DFSUtil.parseRelativeTime("999d"));
  }
}
