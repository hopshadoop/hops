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

package org.apache.hadoop.tools.rumen;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig;

import junit.framework.TestCase;

public class TestRumenJobTraces extends TestCase {

  public void testSmallTrace() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootInputDir = new Path(
        System.getProperty("test.tools.input.dir", "")).makeQualified(lfs);
    final Path rootTempDir = new Path(
        System.getProperty("test.build.data", "/tmp")).makeQualified(lfs);


    final Path rootInputFile = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenJobTraces");
    lfs.delete(tempDir, true);

    assertFalse("property test.build.data is not defined",
        "".equals(rootTempDir));
    assertFalse("property test.tools.input.dir is not defined",
        "".equals(rootInputDir));

    final Path topologyFile = new Path(tempDir, "topology.json");
    final Path traceFile = new Path(tempDir, "trace.json");

    final Path inputFile = new Path(rootInputFile, "sample-job-tracker-logs");

    System.out.println("topology result file = " + topologyFile);
    System.out.println("trace result file = " + traceFile);

    String[] args = new String[6];

    args[0] = "-v1";

    args[1] = "-write-topology";
    args[2] = topologyFile.toString();

    args[3] = "-write-job-trace";
    args[4] = traceFile.toString();

    args[5] = inputFile.toString();

    PrintStream old_stdout = System.out;

    final Path stdoutFile = new Path(tempDir, "stdout.text");

    System.out.println("stdout file = " + stdoutFile);

    PrintStream enveloped_stdout = new PrintStream(new BufferedOutputStream(
          lfs.create(stdoutFile, true)));

    final Path topologyGoldFile = new Path(rootInputFile, 
        "job-tracker-logs-topology-output");
    final Path traceGoldFile = new Path(rootInputFile,
        "job-tracker-logs-trace-output");

    try {
      System.setOut(enveloped_stdout);

      HadoopLogsAnalyzer analyzer = new HadoopLogsAnalyzer();

      int result = ToolRunner.run(analyzer, args);

      enveloped_stdout.close();

      assertEquals("Non-zero exit", 0, result);

    } finally {
      System.setOut(old_stdout);
    }

    jsonFileMatchesGold(lfs, topologyFile, topologyGoldFile,
        new LoggedNetworkTopology(), "topology");
    jsonFileMatchesGold(lfs, traceFile, traceGoldFile, new LoggedJob(),
        "trace");

  }

  /*
   * This block of methods is commented out because its methods require huge
   * test files to support them meaningfully. We expect to be able to fix this
   * problem in a furture release.
   * 
   * public void testBulkFilesJobDistro() throws IOException { String args[] = {
   * "-v1", "-delays", "-runtimes" }; statisticalTest(args,
   * "rumen/large-test-inputs/monolithic-files",
   * "rumen/large-test-inputs/gold-bulk-job-distribution.text", true); }
   * 
   * public void testIndividualFilesJobDistro() throws IOException { String
   * args[] = { "-v1", "-delays", "-runtimes" }; statisticalTest(args,
   * "rumen/large-test-inputs/individual-files",
   * "rumen/large-test-inputs/gold-individual-job-distribution.text", true); }
   * 
   * public void testSpreadsGZFile() throws IOException { String args[] = {
   * "-v1", "-delays", "-runtimes", "-spreads", "10", "90",
   * "-job-digest-spectra", "10", "50", "90" }; statisticalTest( args,
   * "rumen/large-test-inputs/monolithic-files/jobs-0-99-including-truncations.gz"
   * , "rumen/large-test-inputs/gold-single-gz-task-distribution.text", false);
   * }
   * 
   * public void testSpreadsSingleFile() throws IOException { String args[] = {
   * "-v1", "-delays", "-runtimes", "-spreads", "10", "90",
   * "-job-digest-spectra", "10", "50", "90" }; statisticalTest(args,
   * "rumen/large-test-inputs/monolithic-files/jobs-100-199",
   * "rumen/large-test-inputs/gold-single-bulk-task-distribution.text", false);
   * }
   */

  /**
   * 
   * A test case of HadoopLogsAnalyzer.main consists of a call to this function.
   * It succeeds by returning,fails by performing a junit assertion failure, and
   * can abend with an I/O error if some of the inputs aren't there or some of
   * the output cannot be written [due to quota, perhaps, or permissions
   * 
   * 
   * @param args
   *          these are the arguments that we eventually supply to
   *          HadoopLogsAnalyzer.main to test its functionality with regard to
   *          statistical output
   * @param inputFname
   *          this is the file name or directory name of the test input
   *          directory relative to the test cases data directory.
   * @param goldFilename
   *          this is the file name of the expected output relative to the test
   *          cases data directory.
   * @param inputIsDirectory
   *          this states whether the input is an entire directory, or a single
   *          file.
   * @throws IOException
   */
  private void statisticalTest(String args[], String inputFname,
      String goldFilename, boolean inputIsDirectory) throws Exception {
    File tempDirectory = new File(System.getProperty("test.build.data", "/tmp"));

    String rootInputDir = System.getProperty("test.tools.input.dir", "");
    String rootTempDir = System.getProperty("test.build.data", "");

    File rootInputDirFile = new File(new File(rootInputDir), inputFname);
    File tempDirFile = new File(rootTempDir);

    assertFalse("property test.build.data is not defined", ""
        .equals(rootTempDir));
    assertFalse("property test.tools.input.dir is not defined", ""
        .equals(rootInputDir));

    if (rootInputDir.charAt(rootInputDir.length() - 1) == '/') {
      rootInputDir = rootInputDir.substring(0, rootInputDir.length() - 1);
    }

    if (rootTempDir.charAt(rootTempDir.length() - 1) == '/') {
      rootTempDir = rootTempDir.substring(0, rootTempDir.length() - 1);
    }

    File jobDistroGold = new File(new File(rootInputDir), goldFilename);

    String[] newArgs = new String[args.length + 1];

    System.arraycopy(args, 0, newArgs, 0, args.length);

    newArgs[args.length + 1 - 1] = rootInputDirFile.getPath();

    String complaint = inputIsDirectory ? " is not a directory."
        : " does not exist.";

    boolean okay = inputIsDirectory ? rootInputDirFile.isDirectory()
        : rootInputDirFile.canRead();

    assertTrue("The input file " + rootInputDirFile.getPath() + complaint, okay);

    PrintStream old_stdout = System.out;

    File stdoutFile = File.createTempFile("stdout", "text", tempDirFile);

    // stdoutFile.deleteOnExit();

    PrintStream enveloped_stdout = new PrintStream(new BufferedOutputStream(
        new FileOutputStream(stdoutFile)));

    try {
      System.setOut(enveloped_stdout);

      HadoopLogsAnalyzer analyzer = new HadoopLogsAnalyzer();

      int result = ToolRunner.run(analyzer, args);

      enveloped_stdout.close();

      System.setOut(old_stdout);

      assertFilesMatch(stdoutFile, jobDistroGold);
      assertEquals("Non-zero exit", 0, result);
    } finally {
      System.setOut(old_stdout);
    }
  }

  static private Object readMapper(ObjectMapper mapper, JsonParser parser,
      Object obj) throws IOException {
    try {
      return mapper.readValue(parser, obj.getClass());
    } catch (EOFException e) {
      return null;
    }
  }

  static private void assertFilesMatch(File result, File gold)
      throws IOException {
    System.out.println("Comparing files: " + result.getPath() + " vrs. "
        + gold.getPath());

    int currentLineNumber = 1;
    FileInputStream goldStream = new FileInputStream(gold);
    BufferedReader goldReader = new BufferedReader(new InputStreamReader(
        goldStream));
    String currentGoldLine = goldReader.readLine();

    FileInputStream resultStream = new FileInputStream(result);
    BufferedReader resultReader = new BufferedReader(new InputStreamReader(
        resultStream));
    String currentResultLine = resultReader.readLine();

    while (currentGoldLine != null && currentResultLine != null
        && currentGoldLine.equals(currentResultLine)) {
      ++currentLineNumber;

      currentGoldLine = goldReader.readLine();
      currentResultLine = resultReader.readLine();
    }

    if (currentGoldLine == null && currentResultLine == null) {
      return;
    }

    assertFalse("Line number " + currentLineNumber + " disagrees", true);
  }

  static private void jsonFileMatchesGold(FileSystem lfs, Path result,
        Path gold, Object obj, String fileDescription) throws IOException {
    InputStream goldStream = lfs.open(gold);
    BufferedReader goldReader = new BufferedReader(new InputStreamReader(
        goldStream));

    InputStream resultStream = lfs.open(result);
    BufferedReader resultReader = new BufferedReader(new InputStreamReader(
        resultStream));

    ObjectMapper goldMapper = new ObjectMapper();
    ObjectMapper resultMapper = new ObjectMapper();
    goldMapper.configure(
        DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    resultMapper.configure(
        DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);

    JsonParser goldParser = goldMapper.getJsonFactory().createJsonParser(
        goldReader);
    JsonParser resultParser = resultMapper.getJsonFactory().createJsonParser(
        resultReader);

    DeepCompare goldJob = (DeepCompare) readMapper(goldMapper, goldParser, obj);
    DeepCompare resultJob = (DeepCompare) readMapper(resultMapper,
        resultParser, obj);

    while (goldJob != null && resultJob != null) {
      try {
        resultJob.deepCompare(goldJob, new TreePath(null, "<root>"));
      } catch (DeepInequalityException e) {
        String error = e.path.toString();

        assertFalse(fileDescription + " mismatches: " + error, true);
      }

      goldJob = (DeepCompare) readMapper(goldMapper, goldParser, obj);
      resultJob = (DeepCompare) readMapper(resultMapper, resultParser, obj);
    }

    if (goldJob != null) {
      assertFalse(
          "The Gold File has more logged jobs than the result of the run", true);
    }

    if (resultJob != null) {
      assertFalse("The result file has more logged jobs than the Gold File",
          true);
    }
  }
}
