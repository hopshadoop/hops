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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;

import junit.framework.TestCase;

import java.util.List;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 */
public class TestHistograms extends TestCase {

  /**
   * @throws IOException
   * 
   *           There should be files in the directory named by
   *           ${test.build.data}/rumen/histogram-test .
   * 
   *           There will be pairs of files, inputXxx.json and goldXxx.json .
   * 
   *           We read the input file as a HistogramRawTestData in json. Then we
   *           create a Histogram using the data field, and then a
   *           LoggedDiscreteCDF using the percentiles and scale field. Finally,
   *           we read the corresponding goldXxx.json as a LoggedDiscreteCDF and
   *           deepCompare them.
   */
  public void testHistograms() throws IOException {
    String rootInputDir = System.getProperty("test.tools.input.dir", "");

    File rootInputDirFile = new File(rootInputDir);

    File rootInputFile = new File(rootInputDirFile, "rumen/histogram-tests");

    if (rootInputDir.charAt(rootInputDir.length() - 1) == '/') {
      rootInputDir = rootInputDir.substring(0, rootInputDir.length() - 1);
    }

    String[] tests = rootInputFile.list();

    for (int i = 0; i < tests.length; ++i) {
      if (tests[i].length() > 5 && "input".equals(tests[i].substring(0, 5))) {
        File inputData = new File(rootInputFile, tests[i]);

        if (!(new File(rootInputFile, "build" + tests[i].substring(5)))
            .exists()
            && !(new File(rootInputFile, "gold" + tests[i].substring(5))
                .exists())
            && !(new File(rootInputFile, "silver" + tests[i].substring(5))
                .exists())) {
          System.out
              .println("Neither a build nor a gold file exists for the file, "
                  + inputData.getCanonicalPath());

          continue;
        }

        LoggedDiscreteCDF newResult = histogramFileToCDF(inputData.getPath());

        if ((new File(rootInputFile, "build" + tests[i].substring(5))).exists()
            && !(new File(rootInputFile, "gold" + tests[i].substring(5)))
                .exists()
            && !(new File(rootInputFile, "silver" + tests[i].substring(5)))
                .exists()) {
          try {
            System.out.println("Building a new gold file for the file, "
                + inputData.getCanonicalPath());
            System.out.println("Please inspect it thoroughly and rename it.");

            ObjectMapper mapper = new ObjectMapper();
            JsonFactory factory = mapper.getJsonFactory();
            PrintStream ostream = new PrintStream(new File(rootInputFile,
                "silver" + tests[i].substring(5)));
            JsonGenerator gen = factory.createJsonGenerator(ostream,
                JsonEncoding.UTF8);
            gen.useDefaultPrettyPrinter();

            gen.writeObject(newResult);

            gen.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        } else {
          System.out.println("Testing a Histogram built from the file, "
              + inputData.getCanonicalPath());
          File goldCDF = new File(rootInputFile, "gold" + tests[i].substring(5));
          FileInputStream goldStream = new FileInputStream(goldCDF);
          BufferedReader goldReader = new BufferedReader(new InputStreamReader(
              goldStream));
          ObjectMapper goldMapper = new ObjectMapper();
          JsonParser goldParser = goldMapper.getJsonFactory().createJsonParser(
              goldReader);
          LoggedDiscreteCDF DCDF = goldMapper.readValue(goldParser,
              LoggedDiscreteCDF.class);

          try {
            DCDF.deepCompare(newResult, new TreePath(null, "<root>"));
          } catch (DeepInequalityException e) {
            String error = e.path.toString();

            assertFalse(error, true);
          }
        }
      }
    }
  }

  private static LoggedDiscreteCDF histogramFileToCDF(String filename)
      throws IOException {

    File inputData = new File(filename);

    FileInputStream dataStream = new FileInputStream(inputData);
    BufferedReader dataReader = new BufferedReader(new InputStreamReader(
        dataStream));
    ObjectMapper dataMapper = new ObjectMapper();
    dataMapper.configure(
        DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    JsonParser dataParser = dataMapper.getJsonFactory().createJsonParser(
        dataReader);
    HistogramRawTestData data = dataMapper.readValue(dataParser,
        HistogramRawTestData.class);

    Histogram hist = new Histogram();

    List<Long> measurements = data.getData();

    List<Long> typeProbeData = new HistogramRawTestData().getData();

    assertTrue(
        "The data attribute of a jackson-reconstructed HistogramRawTestData "
            + " should be a " + typeProbeData.getClass().getName()
            + ", like a virgin HistogramRawTestData, but it's a "
            + measurements.getClass().getName(),
        measurements.getClass() == typeProbeData.getClass());

    for (int j = 0; j < measurements.size(); ++j) {
      hist.enter(measurements.get(j));
    }

    LoggedDiscreteCDF result = new LoggedDiscreteCDF();
    int[] percentiles = new int[data.getPercentiles().size()];

    for (int j = 0; j < data.getPercentiles().size(); ++j) {
      percentiles[j] = data.getPercentiles().get(j);
    }

    result.setCDF(hist, percentiles, data.getScale());

    return result;
  }
}
