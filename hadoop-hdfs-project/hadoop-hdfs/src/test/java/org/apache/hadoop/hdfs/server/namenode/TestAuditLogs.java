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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A JUnit test that audit logs are generated
 */
public class TestAuditLogs {
  static final String auditLogFile =
      System.getProperty("test.build.dir", "build/test") + "/audit.log";
  
  // Pattern for: 
  // allowed=(true|false) ugi=name ip=/address cmd={cmd} src={path} dst=null perm=null
  static final Pattern auditPattern = Pattern.compile("allowed=.*?\\s" +
          "ugi=.*?\\s" +
          "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" +
          "cmd=.*?\\ssrc=.*?\\sdst=null\\s" +
          "perm=.*?");
  static final Pattern successPattern = Pattern.compile(".*allowed=true.*");
  static final String username = "bob";
  static final String[] groups = {"group1"};
  static final String fileName = "/srcdat";

  DFSTestUtil util;
  MiniDFSCluster cluster;
  FileSystem fs;
  String fnames[];
  Configuration conf;
  UserGroupInformation userGroupInfo;

  @Before
  public void setupCluster() throws Exception {
    conf = new HdfsConfiguration();
    final long precision = 1L;
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
        precision);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    util = new DFSTestUtil.Builder().setName("TestAuditAllowed").
        setNumFiles(20).build();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
    fs = cluster.getFileSystem();
    util.createFiles(fs, fileName);

    fnames = util.getFileNames(fileName);
    util.waitReplication(fs, fileName, (short) 3);
    userGroupInfo = UserGroupInformation.createUserForTesting(username, groups);
  }

  @After
  public void teardownCluster() throws Exception {
    util.cleanup(fs, "/srcdat");
    fs.close();
    cluster.shutdown();
  }

  /**
   * test that allowed operation puts proper entry in audit log
   */
  @Test
  public void testAuditAllowed() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    setupAuditLogs();
    InputStream istream = userfs.open(file);
    int val = istream.read();
    istream.close();
    verifyAuditLogs(true);
    assertTrue("failed to read from file", val >= 0);
  }

  /**
   * test that allowed stat puts proper entry in audit log
   */
  @Test
  public void testAuditAllowedStat() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    setupAuditLogs();
    FileStatus st = userfs.getFileStatus(file);
    verifyAuditLogs(true);
    assertTrue("failed to stat file", st != null && st.isFile());
  }

  /**
   * test that denied operation puts proper entry in audit log
   */
  @Test
  public void testAuditDenied() throws Exception {
    final Path file = new Path(fnames[0]);
    FileSystem userfs = DFSTestUtil.getFileSystemAs(userGroupInfo, conf);

    fs.setPermission(file, new FsPermission((short) 0600));
    fs.setOwner(file, "root", null);

    setupAuditLogs();

    try {
      userfs.open(file);
      fail("open must not succeed");
    } catch (AccessControlException e) {
      System.out.println("got access denied, as expected.");
    }
    verifyAuditLogs(false);
  }

  /**
   * test that access via webhdfs puts proper entry in audit log
   */
  @Test
  public void testAuditWebHdfs() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short) 0644));
    fs.setOwner(file, "root", null);

    setupAuditLogs();

    WebHdfsFileSystem webfs =
        WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf);
    InputStream istream = webfs.open(file);
    int val = istream.read();
    istream.close();

    verifyAuditLogsRepeat(true, 3);
    assertTrue("failed to read from file", val >= 0);
  }

  /**
   * test that stat via webhdfs puts proper entry in audit log
   */
  @Test
  public void testAuditWebHdfsStat() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short) 0644));
    fs.setOwner(file, "root", null);

    setupAuditLogs();

    WebHdfsFileSystem webfs =
        WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf);
    FileStatus st = webfs.getFileStatus(file);

    verifyAuditLogs(true);
    assertTrue("failed to stat file", st != null && st.isFile());
  }

  /**
   * test that access via Hftp puts proper entry in audit log
   */
  @Test
  public void testAuditHftp() throws Exception {
    final Path file = new Path(fnames[0]);

    final String hftpUri =
        "hftp://" + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);

    HftpFileSystem hftpFs = null;

    setupAuditLogs();
    try {
      hftpFs = (HftpFileSystem) new Path(hftpUri).getFileSystem(conf);
      InputStream istream = hftpFs.open(file);
      int val = istream.read();
      istream.close();

      verifyAuditLogs(true);
    } finally {
      if (hftpFs != null) {
        hftpFs.close();
      }
    }
  }

  /**
   * test that denied access via webhdfs puts proper entry in audit log
   */
  @Test
  public void testAuditWebHdfsDenied() throws Exception {
    final Path file = new Path(fnames[0]);

    fs.setPermission(file, new FsPermission((short) 0600));
    fs.setOwner(file, "root", null);

    setupAuditLogs();
    try {
      WebHdfsFileSystem webfs =
          WebHdfsTestUtil.getWebHdfsFileSystemAs(userGroupInfo, conf);
      InputStream istream = webfs.open(file);
      int val = istream.read();
      fail("open+read must not succeed, got " + val);
    } catch (AccessControlException E) {
      System.out.println("got access denied, as expected.");
    }
    verifyAuditLogsRepeat(false, 2);
  }

  /**
   * Sets up log4j logger for auditlogs
   */
  private void setupAuditLogs() throws IOException {
    File file = new File(auditLogFile);
    if (file.exists()) {
      file.delete();
    }
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.INFO);
    PatternLayout layout = new PatternLayout("%m%n");
    RollingFileAppender appender =
        new RollingFileAppender(layout, auditLogFile);
    logger.addAppender(appender);
  }

  // Ensure audit log has only one entry
  private void verifyAuditLogs(boolean expectSuccess) throws IOException {
    verifyAuditLogsRepeat(expectSuccess, 1);
  }

  // Ensure audit log has exactly N entries
  private void verifyAuditLogsRepeat(boolean expectSuccess, int ndupe)
      throws IOException {
    // Turn off the logs
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.OFF);
    
    BufferedReader reader = new BufferedReader(new FileReader(auditLogFile));
    String line = null;
    boolean ret = true;

    try {
      for (int i = 0; i < ndupe; i++) {
        line = reader.readLine();
        assertNotNull(line);
        assertTrue("Expected audit event not found in audit log",
            auditPattern.matcher(line).matches());
        ret &= successPattern.matcher(line).matches();
      }
      assertNull("Unexpected event in audit log", reader.readLine());
      assertTrue("Expected success=" + expectSuccess, ret == expectSuccess);
    } finally {
      reader.close();
    }
  }
}
