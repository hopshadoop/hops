/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.leaderElection.experiments;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.StringTokenizer;

public class ExperimentDriver {

  private Process process;
  private static final String JAR_FILE =
      "hops-leader-election-2.0.4-alpha-jar-with-dependencies.jar";
  private static final Log LOG = LogFactory.getLog(ExperimentDriver.class);


  public static void main(String[] argv)
      throws FileNotFoundException, IOException, InterruptedException {
    String experimentFile = "experiments_descriptions.txt";

    if (argv.length != 1) {
      System.out
          .println("Please specify the file containing experiment parameters ");
      System.out.println("Trying to read defaule file " + experimentFile);
    } else {
      experimentFile = argv[0];
    }

    new ExperimentDriver().runCommands(experimentFile);
  }

  private void runCommands(String file)
      throws FileNotFoundException, IOException, InterruptedException {
    if (!new File(file).exists()) {
      LOG.error("File Does not exists");
      return;
    }

    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    while ((line = br.readLine()) != null) {
      if (line.startsWith("#") || line.trim().isEmpty()) {
        continue;
      }
      
      StringTokenizer st = new StringTokenizer(line, " ");
      int numProcesses = -1;
      String timeToRep = st.nextToken();
      int timesToRepeat = Integer.parseInt(timeToRep);
      line = line.substring(timeToRep.length(), line.length());
      String outputFileName = null;
      
      while (st.hasMoreElements()) {
        if (st.nextElement().equals("-max_processes")) {
          numProcesses = Integer.parseInt(st.nextToken());
          outputFileName = numProcesses + ".log";
          break;
        }
      }
      LOG.info("Going to repeat the experiment of " + timesToRepeat + " times");
      for (int i = 0; i < timesToRepeat; i++) { // run command 10 times
        String command = line + " -output_file_path " + outputFileName;
        LOG.info("Driver going to run command " + command);
        runCommand(command);
      }
      
      LOG.info("Going to calculate points from file " + outputFileName);
      calculateNumbers(numProcesses, outputFileName);
    }
    br.close();
  }

  private void calculateNumbers(int numProcesses, String outputFileName)
      throws FileNotFoundException, IOException {
    if (!new File(outputFileName).exists()) {
      LOG.error("File " + outputFileName + " does not exists");
      return;
    }
    String marker = "DataPoints: ";
    String line;
    DescriptiveStatistics failOverStats = new DescriptiveStatistics();
    DescriptiveStatistics tpStats = new DescriptiveStatistics();
    BufferedReader br = new BufferedReader(new FileReader(outputFileName));
    while ((line = br.readLine()) != null) {
      if (!line.startsWith(marker)) {
        continue;
      }

      boolean tpStatRecorded = false;
      String numbers = line.substring(marker.length(), line.length());
      StringTokenizer st = new StringTokenizer(numbers, ",[] ");
      while (st.hasMoreElements()) {
        double point = Double.parseDouble(st.nextToken());
        if (!tpStatRecorded) {
          tpStats.addValue(point);
          tpStatRecorded = true;
        } else {
          failOverStats.addValue(point);
        }
      }
    }
    br.close();
    writeMessageToFile(numProcesses, failOverStats, tpStats);
    
  }

  public void writeMessageToFile(int numProcesses,
      DescriptiveStatistics failOverStats, DescriptiveStatistics tpStats)
      throws IOException {
    PrintWriter out = new PrintWriter(
        new BufferedWriter(new FileWriter("summary.log", true)));
    out.println(
        numProcesses + " " + tpStats.getN() + " " + tpStats.getMin() + " " +
            tpStats.getMax() + " " + tpStats.getMean() + " " +
            tpStats.getStandardDeviation() + " " +
            (tpStats.getStandardDeviation() / Math.sqrt(tpStats.getN())) + " " +
            failOverStats.getN() + " " + failOverStats.getMin() + " " +
            failOverStats.getMax() + " " + failOverStats.getMean() + " " +
            failOverStats.getStandardDeviation() + " " +
            (failOverStats.getStandardDeviation() /
                Math.sqrt(failOverStats.getN())));
    out.close();
  }

  private void runCommand(String args)
      throws IOException, InterruptedException {

    this.process = Runtime.getRuntime().exec(makeCommand(args));
    if (process == null) {
      LOG.error("Failed to run experiment. Argv " + args);
    } else {
      new StreamGobbler(process.getInputStream()).start();
      new StreamGobbler(process.getErrorStream()).start();
      LOG.error("Process exited. Value " + process.waitFor());


    }
  }

  private String makeCommand(String args) {
    String dir = System.getProperty("user.dir");
    String jarFile =
        dir + File.separator + "target" + File.separator + JAR_FILE;
    if (!new File(jarFile).exists()) {
      jarFile =
          Experiment1.class.getProtectionDomain().getCodeSource().getLocation()
              .getPath();
    }
    String command = "java -Xmx10000m -cp " + jarFile + " Experiment1 " + args;
    LOG.error(command);
    return command;
  }

  class StreamGobbler extends Thread {

    InputStream is;

    // reads everything from is until empty. 
    StreamGobbler(InputStream is) {
      this.is = is;
    }

    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
          System.out.println(line);
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }
}
