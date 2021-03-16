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

package org.apache.hadoop.tools.contract;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contract test suite covering a file system's integration with DistCp.  The
 * tests coordinate two file system instances: one "local", which is the local
 * file system, and the other "remote", which is the file system implementation
 * under test.  The tests in the suite cover both copying from local to remote
 * (e.g. a backup use case) and copying from remote to local (e.g. a restore use
 * case).
 */
public abstract class AbstractContractDistCpTest
    extends AbstractFSContractTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractDistCpTest.class);

  public static final String SCALE_TEST_DISTCP_FILE_SIZE_KB
      = "scale.test.distcp.file.size.kb";

  public static final int DEFAULT_DISTCP_SIZE_KB = 1024;

  protected static final int MB = 1024 * 1024;

  @Rule
  public TestName testName = new TestName();

  /**
   * The timeout value is extended over the default so that large updates
   * are allowed to take time, especially to remote stores.
   * @return the current test timeout
   */
  protected int getTestTimeoutMillis() {
    return 15  * 60 * 1000;
  }

  private Configuration conf;
  private FileSystem localFS, remoteFS;
  private Path localDir, remoteDir;

  private Path inputDir;

  private Path inputSubDir1;

  private Path inputSubDir2;

  private Path inputSubDir4;

  private Path inputFile1;

  private Path inputFile2;

  private Path inputFile3;

  private Path inputFile4;

  private Path inputFile5;

  private Path outputDir;

  private Path outputSubDir1;

  private Path outputSubDir2;

  private Path outputSubDir4;

  private Path outputFile1;

  private Path outputFile2;

  private Path outputFile3;

  private Path outputFile4;

  private Path outputFile5;

  private Path inputDirUnderOutputDir;

  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = new Configuration();
    newConf.set("mapred.job.tracker", "local");
    return newConf;
  }

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();
    conf = getContract().getConf();
    localFS = FileSystem.getLocal(conf);
    remoteFS = getFileSystem();
    // Test paths are isolated by concrete subclass name and test method name.
    // All paths are fully qualified including scheme (not taking advantage of
    // default file system), so if something fails, the messages will make it
    // clear which paths are local and which paths are remote.
    String className = getClass().getSimpleName();
    String testSubDir = className + "/" + testName.getMethodName();
    localDir =
        localFS.makeQualified(new Path(new Path(
            GenericTestUtils.getTestDir().toURI()), testSubDir + "/local"));
    mkdirs(localFS, localDir);
    remoteDir = path(testSubDir + "/remote");
    mkdirs(remoteFS, remoteDir);
    // test teardown does this, but IDE-based test debugging can skip
    // that teardown; this guarantees the initial state is clean
    remoteFS.delete(remoteDir, true);
    localFS.delete(localDir, true);
  }

  /**
   * Set up both input and output fields.
   *
   * @param src  source tree
   * @param dest dest tree
   */
  protected void initPathFields(final Path src, final Path dest) {
    initInputFields(src);
    initOutputFields(dest);
  }

  /**
   * Output field setup.
   *
   * @param path path to set up
   */
  protected void initOutputFields(final Path path) {
    outputDir = new Path(path, "outputDir");
    inputDirUnderOutputDir = new Path(outputDir, "inputDir");
    outputFile1 = new Path(inputDirUnderOutputDir, "file1");
    outputSubDir1 = new Path(inputDirUnderOutputDir, "subDir1");
    outputFile2 = new Path(outputSubDir1, "file2");
    outputSubDir2 = new Path(inputDirUnderOutputDir, "subDir2/subDir2");
    outputFile3 = new Path(outputSubDir2, "file3");
    outputSubDir4 = new Path(inputDirUnderOutputDir, "subDir4/subDir4");
    outputFile4 = new Path(outputSubDir4, "file4");
    outputFile5 = new Path(outputSubDir4, "file5");
  }

  /**
   * this path setup is used across different methods (copy, update, track)
   * so they are set up as fields.
   *
   * @param srcDir source directory for these to go under.
   */
  protected void initInputFields(final Path srcDir) {
    inputDir = new Path(srcDir, "inputDir");
    inputFile1 = new Path(inputDir, "file1");
    inputSubDir1 = new Path(inputDir, "subDir1");
    inputFile2 = new Path(inputSubDir1, "file2");
    inputSubDir2 = new Path(inputDir, "subDir2/subDir2");
    inputFile3 = new Path(inputSubDir2, "file3");
    inputSubDir4 = new Path(inputDir, "subDir4/subDir4");
    inputFile4 = new Path(inputSubDir4, "file4");
    inputFile5 = new Path(inputSubDir4, "file5");
  }

  protected FileSystem getLocalFS() {
    return localFS;
  }

  protected FileSystem getRemoteFS() {
    return remoteFS;
  }

  protected Path getLocalDir() {
    return localDir;
  }

  protected Path getRemoteDir() {
    return remoteDir;
  }

  @Test
  public void deepDirectoryStructureToRemote() throws Exception {
    describe("copy a deep directory structure from local to remote");
    deepDirectoryStructure(localFS, localDir, remoteFS, remoteDir);
  }

  public void lsR(final String description,
                  final FileSystem fs,
                  final Path dir) throws IOException {
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(dir, true);
    LOG.info("{}: {}:", description, dir);
    StringBuilder sb = new StringBuilder();
    while(files.hasNext()) {
      LocatedFileStatus status = files.next();
      sb.append(String.format("  %s; type=%s; length=%d",
          status.getPath(),
          status.isDirectory()? "dir" : "file",
          status.getLen()));
    }
    LOG.info("{}", sb);
  }

  @Test
  public void largeFilesToRemote() throws Exception {
    describe("copy multiple large files from local to remote");
    largeFiles(localFS, localDir, remoteFS, remoteDir);
  }

  @Test
  public void deepDirectoryStructureFromRemote() throws Exception {
    describe("copy a deep directory structure from remote to local");
    deepDirectoryStructure(remoteFS, remoteDir, localFS, localDir);
  }

  @Test
  public void largeFilesFromRemote() throws Exception {
    describe("copy multiple large files from remote to local");
    largeFiles(remoteFS, remoteDir, localFS, localDir);
  }

  /**
   * Executes a test using a file system sub-tree with multiple nesting levels.
   *
   * @param srcFS source FileSystem
   * @param srcDir source directory
   * @param dstFS destination FileSystem
   * @param dstDir destination directory
   * @throws Exception if there is a failure
   */
  private void deepDirectoryStructure(FileSystem srcFS, Path srcDir,
      FileSystem dstFS, Path dstDir) throws Exception {
    Path inputDir = new Path(srcDir, "inputDir");
    Path inputSubDir1 = new Path(inputDir, "subDir1");
    Path inputSubDir2 = new Path(inputDir, "subDir2/subDir3");
    Path inputFile1 = new Path(inputDir, "file1");
    Path inputFile2 = new Path(inputSubDir1, "file2");
    Path inputFile3 = new Path(inputSubDir2, "file3");
    mkdirs(srcFS, inputSubDir1);
    mkdirs(srcFS, inputSubDir2);
    byte[] data1 = dataset(100, 33, 43);
    createFile(srcFS, inputFile1, true, data1);
    byte[] data2 = dataset(200, 43, 53);
    createFile(srcFS, inputFile2, true, data2);
    byte[] data3 = dataset(300, 53, 63);
    createFile(srcFS, inputFile3, true, data3);
    Path target = new Path(dstDir, "outputDir");
    runDistCp(inputDir, target);
    ContractTestUtils.assertIsDirectory(dstFS, target);
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS,
        new Path(target, "inputDir/subDir1/file2"), data2);
    verifyFileContents(dstFS,
        new Path(target, "inputDir/subDir2/subDir3/file3"), data3);
  }

  /**
   * Executes a test using multiple large files.
   *
   * @param srcFS source FileSystem
   * @param srcDir source directory
   * @param dstFS destination FileSystem
   * @param dstDir destination directory
   * @throws Exception if there is a failure
   */
  private void largeFiles(FileSystem srcFS, Path srcDir, FileSystem dstFS,
      Path dstDir) throws Exception {
    Path inputDir = new Path(srcDir, "inputDir");
    Path inputFile1 = new Path(inputDir, "file1");
    Path inputFile2 = new Path(inputDir, "file2");
    Path inputFile3 = new Path(inputDir, "file3");
    mkdirs(srcFS, inputDir);
    int fileSizeKb = conf.getInt("scale.test.distcp.file.size.kb", 10 * 1024);
    int fileSizeMb = fileSizeKb / 1024;
    getLog().info("{} with file size {}", testName.getMethodName(), fileSizeMb);
    byte[] data1 = dataset((fileSizeMb + 1) * 1024 * 1024, 33, 43);
    createFile(srcFS, inputFile1, true, data1);
    byte[] data2 = dataset((fileSizeMb + 2) * 1024 * 1024, 43, 53);
    createFile(srcFS, inputFile2, true, data2);
    byte[] data3 = dataset((fileSizeMb + 3) * 1024 * 1024, 53, 63);
    createFile(srcFS, inputFile3, true, data3);
    Path target = new Path(dstDir, "outputDir");
    runDistCp(inputDir, target);
    ContractTestUtils.assertIsDirectory(dstFS, target);
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS, new Path(target, "inputDir/file2"), data2);
    verifyFileContents(dstFS, new Path(target, "inputDir/file3"), data3);
  }

  /**
   * Executes DistCp and asserts that the job finished successfully.
   *
   * @param src source path
   * @param dst destination path
   * @throws Exception if there is a failure
   */
  private void runDistCp(Path src, Path dst) throws Exception {
    runDistCp(new DistCpOptions(Arrays.asList(src), dst));
  }

  /**
   * Run the distcp job.
   * @param options distcp options
   * @return the job. It will have already completed.
   * @throws Exception failure
   */
  private Job runDistCp(final DistCpOptions options) throws Exception {
    Job job = new DistCp(conf, options).execute();
    assertNotNull("Unexpected null job returned from DistCp execution.", job);
    assertTrue("DistCp job did not complete.", job.isComplete());
    assertTrue("DistCp job did not complete successfully.", job.isSuccessful());
    return job;
  }

  /**
   * Creates a directory and any ancestor directories required.
   *
   * @param fs FileSystem in which to create directories
   * @param dir path of directory to create
   * @throws Exception if there is a failure
   */
  private static void mkdirs(FileSystem fs, Path dir) throws Exception {
    assertTrue("Failed to mkdir " + dir, fs.mkdirs(dir));
  }

  @Test
  public void testDirectWrite() throws Exception {
    describe("copy file from local to remote using direct write option");
    directWrite(localFS, localDir, remoteFS, remoteDir, true);
  }

  @Test
  public void testNonDirectWrite() throws Exception {
    describe("copy file from local to remote without using direct write " +
        "option");
    directWrite(localFS, localDir, remoteFS, remoteDir, false);
  }

  /**
   * Executes a test with support for using direct write option.
   *
   * @param srcFS source FileSystem
   * @param srcDir source directory
   * @param dstFS destination FileSystem
   * @param dstDir destination directory
   * @param directWrite whether to use -directwrite option
   * @throws Exception if there is a failure
   */
  private void directWrite(FileSystem srcFS, Path srcDir, FileSystem dstFS,
          Path dstDir, boolean directWrite) throws Exception {
    initPathFields(srcDir, dstDir);

    // Create 2 test files
    mkdirs(srcFS, inputSubDir1);
    byte[] data1 = dataset(64, 33, 43);
    createFile(srcFS, inputFile1, true, data1);
    byte[] data2 = dataset(200, 43, 53);
    createFile(srcFS, inputFile2, true, data2);
    Path target = new Path(dstDir, "outputDir");
    if (directWrite) {
      runDistCpDirectWrite(inputDir, target);
    } else {
      runDistCp(inputDir, target);
    }
    ContractTestUtils.assertIsDirectory(dstFS, target);
    lsR("Destination tree after distcp", dstFS, target);

    // Verify copied file contents
    verifyFileContents(dstFS, new Path(target, "inputDir/file1"), data1);
    verifyFileContents(dstFS, new Path(target, "inputDir/subDir1/file2"),
        data2);
  }

  /**
   * Run distcp -direct srcDir destDir.
   * @param srcDir local source directory
   * @param destDir remote destination directory
   * @return the completed job
   * @throws Exception any failure.
   */
  private Job runDistCpDirectWrite(final Path srcDir, final Path destDir)
          throws Exception {
    describe("\nDistcp -direct from " + srcDir + " to " + destDir);
    DistCpOptions options = new DistCpOptions(Arrays.asList(srcDir), destDir);
    options.setDirectWrite(true);
    return runDistCp(options);
  }
}
