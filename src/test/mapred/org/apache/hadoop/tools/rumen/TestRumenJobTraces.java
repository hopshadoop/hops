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
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig;

import junit.framework.TestCase;

public class TestRumenJobTraces extends TestCase {
  public void testSmallTrace() throws IOException {
    File tempDirectory = new File(System.getProperty("test.build.data", "/tmp"));

    String rootInputDir = System.getProperty("test.tools.input.dir", "");
    String rootTempDir = System.getProperty("test.build.data", "");

    File rootInputFile = new File(new File(rootInputDir),
        "rumen/small-trace-test");
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

    File topologyFile = File.createTempFile("topology", ".json", tempDirFile);
    File traceFile = File.createTempFile("trace", ".json", tempDirFile);

    File inputFile = new File(rootInputFile, "sample-job-tracker-logs");

    // topologyFile.deleteOnExit();
    // traceFile.deleteOnExit();
    System.out.println("topology result file = "
        + topologyFile.getCanonicalPath());
    System.out.println("trace result file = " + traceFile.getCanonicalPath());

    String[] args = new String[6];

    args[0] = "-v1";

    args[1] = "-write-topology";
    args[2] = topologyFile.getPath();

    args[3] = "-write-job-trace";
    args[4] = traceFile.getPath();

    args[5] = inputFile.getPath();

    assertTrue("The input file " + inputFile.getPath() + " does not exist.",
        inputFile.canRead());
    assertTrue("The output topology file " + topologyFile.getPath()
        + " cannot be written.", topologyFile.canWrite());
    assertTrue("The output trace file " + traceFile.getPath()
        + " cannot be written.", traceFile.canWrite());

    PrintStream old_stdout = System.out;

    File stdoutFile = File.createTempFile("stdout", ".text", tempDirFile);

    // stdoutFile.deleteOnExit();
    System.out.println("stdout file = " + stdoutFile.getCanonicalPath());

    PrintStream enveloped_stdout = new PrintStream(new BufferedOutputStream(
        new FileOutputStream(stdoutFile)));

    File topologyGoldFile = new File(rootInputFile,
        "job-tracker-logs-topology-output");
    File traceGoldFile = new File(rootInputFile,
        "job-tracker-logs-trace-output");

    try {
      System.setOut(enveloped_stdout);

      HadoopLogsAnalyzer.main(args);

      enveloped_stdout.close();
    } finally {
      System.setOut(old_stdout);
    }

    jsonFileMatchesGold(topologyFile, topologyGoldFile,
        new LoggedNetworkTopology(), "topology");
    jsonFileMatchesGold(traceFile, traceGoldFile, new LoggedJob(), "trace");

    System.out
        .println("These files have been erased because the tests have succeeded.");

    topologyFile.deleteOnExit();
    traceFile.deleteOnExit();
    stdoutFile.deleteOnExit();
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
      String goldFilename, boolean inputIsDirectory) throws IOException {
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

      HadoopLogsAnalyzer.main(newArgs);

      enveloped_stdout.close();

      System.setOut(old_stdout);

      assertFilesMatch(stdoutFile, jobDistroGold);
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

  static private void jsonFileMatchesGold(File result, File gold, Object obj,
      String fileDescription) throws IOException {
    FileInputStream goldStream = new FileInputStream(gold);
    BufferedReader goldReader = new BufferedReader(new InputStreamReader(
        goldStream));

    FileInputStream resultStream = new FileInputStream(result);
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
