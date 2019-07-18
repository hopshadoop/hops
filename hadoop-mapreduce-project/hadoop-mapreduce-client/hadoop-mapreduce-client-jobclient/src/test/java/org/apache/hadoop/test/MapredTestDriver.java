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

package org.apache.hadoop.test;

import org.apache.hadoop.fs.DFSCIOTest;
import org.apache.hadoop.fs.DistributedFSCheck;
import org.apache.hadoop.fs.JHLogAnalyzer;
import org.apache.hadoop.fs.TestDFSIO;
import org.apache.hadoop.fs.TestFileSystem;
import org.apache.hadoop.fs.loadGenerator.DataGenerator;
import org.apache.hadoop.fs.loadGenerator.LoadGenerator;
import org.apache.hadoop.fs.loadGenerator.LoadGeneratorMR;
import org.apache.hadoop.fs.loadGenerator.StructureGenerator;
import org.apache.hadoop.fs.slive.SliveTest;
import org.apache.hadoop.hdfs.NNBench;
import org.apache.hadoop.hdfs.NNBenchWithoutMR;
import org.apache.hadoop.io.FileBench;
import org.apache.hadoop.io.TestSequenceFile;
import org.apache.hadoop.mapred.BigMapOutput;
import org.apache.hadoop.mapred.GenericMRLoadGenerator;
import org.apache.hadoop.mapred.MRBench;
import org.apache.hadoop.mapred.ReliabilityTest;
import org.apache.hadoop.mapred.SortValidator;
import org.apache.hadoop.mapred.TestMapRed;
import org.apache.hadoop.mapred.TestSequenceFileInputFormat;
import org.apache.hadoop.mapred.TestTextInputFormat;
import org.apache.hadoop.mapred.ThreadedMapBenchmark;
import org.apache.hadoop.mapreduce.FailJob;
import org.apache.hadoop.mapreduce.GrowingSleepJob;
import org.apache.hadoop.mapreduce.LargeSorter;
import org.apache.hadoop.mapreduce.MiniHadoopClusterManager;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.TimelineServicePerformance;
import org.apache.hadoop.util.ProgramDriver;

/**
 * Driver for Map-reduce tests.
 *
 */
public class MapredTestDriver {

  private ProgramDriver pgd;

  public MapredTestDriver() {
    this(new ProgramDriver());
  }

  public MapredTestDriver(ProgramDriver pgd) {
    this.pgd = pgd;
    try {
      pgd.addClass("testsequencefile", TestSequenceFile.class,
      "A test for flat files of binary key value pairs.");
      pgd.addClass("threadedmapbench", ThreadedMapBenchmark.class,
          "A map/reduce benchmark that compares the performance " +
          "of maps with multiple spills over maps with 1 spill");
      pgd.addClass("mrbench", MRBench.class,
          "A map/reduce benchmark that can create many small jobs");
      pgd.addClass("mapredtest", TestMapRed.class, "A map/reduce test check.");
      pgd.addClass("testsequencefileinputformat",
          TestSequenceFileInputFormat.class,
          "A test for sequence file input format.");
      pgd.addClass("testtextinputformat", TestTextInputFormat.class,
          "A test for text input format.");
      pgd.addClass("testmapredsort", SortValidator.class,
          "A map/reduce program that validates the " +
          "map-reduce framework's sort.");
      pgd.addClass("testbigmapoutput", BigMapOutput.class,
          "A map/reduce program that works on a very big " +
          "non-splittable file and does identity map/reduce");
      pgd.addClass("loadgen", GenericMRLoadGenerator.class,
          "Generic map/reduce load generator");
      pgd.addClass("MRReliabilityTest", ReliabilityTest.class,
          "A program that tests the reliability of the MR framework by " +
          "injecting faults/failures");
      pgd.addClass("fail", FailJob.class, "a job that always fails");
      pgd.addClass("sleep", SleepJob.class,
                   "A job that sleeps at each map and reduce task.");
      pgd.addClass("gsleep", GrowingSleepJob.class,
          "A sleep job whose mappers create 1MB buffer for every record.");
      pgd.addClass("timelineperformance", TimelineServicePerformance.class,
                   "A job that launches mappers to test timeline service " +
                   "performance.");
      pgd.addClass("nnbench", NNBench.class,
          "A benchmark that stresses the namenode w/ MR.");
      pgd.addClass("nnbenchWithoutMR", NNBenchWithoutMR.class,
          "A benchmark that stresses the namenode w/o MR.");
      pgd.addClass("testfilesystem", TestFileSystem.class,
          "A test for FileSystem read/write.");
      pgd.addClass(TestDFSIO.class.getSimpleName(), TestDFSIO.class,
          "Distributed i/o benchmark.");
      pgd.addClass("DFSCIOTest", DFSCIOTest.class, "" +
          "Distributed i/o benchmark of libhdfs.");
      pgd.addClass("DistributedFSCheck", DistributedFSCheck.class,
          "Distributed checkup of the file system consistency.");
      pgd.addClass("filebench", FileBench.class,
          "Benchmark SequenceFile(Input|Output)Format " +
          "(block,record compressed and uncompressed), " +
          "Text(Input|Output)Format (compressed and uncompressed)");
      pgd.addClass(JHLogAnalyzer.class.getSimpleName(), JHLogAnalyzer.class,
          "Job History Log analyzer.");
      pgd.addClass(SliveTest.class.getSimpleName(), SliveTest.class,
          "HDFS Stress Test and Live Data Verification.");
      pgd.addClass("minicluster", MiniHadoopClusterManager.class,
      "Single process HDFS and MR cluster.");
      pgd.addClass("largesorter", LargeSorter.class,
          "Large-Sort tester");
      pgd.addClass("NNloadGenerator", LoadGenerator.class,
              "Generate load on Namenode using NN loadgenerator run WITHOUT MR");
      pgd.addClass("NNloadGeneratorMR", LoadGeneratorMR.class,
          "Generate load on Namenode using NN loadgenerator run as MR job");
      pgd.addClass("NNstructureGenerator", StructureGenerator.class,
          "Generate the structure to be used by NNdataGenerator");
      pgd.addClass("NNdataGenerator", DataGenerator.class,
          "Generate the data to be used by NNloadGenerator");
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }

  public void run(String argv[]) {
    int exitCode = -1;
    try {
      exitCode = pgd.run(argv);
    } catch(Throwable e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }

  public static void main(String argv[]){
    new MapredTestDriver().run(argv);
  }
}

