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

import io.hops.exception.StorageException;
import io.hops.exception.StorageInitializtionException;
import io.hops.leaderElection.HdfsLeDescriptorFactory;
import io.hops.leaderElection.VarsRegister;
import io.hops.metadata.LEStorageFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.OptionHandler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class Experiment1 {

  private static final Log LOG = LogFactory.getLog(Experiment1.class);
  Configuration conf = null;
  List<LightWeightNameNode> nnList;
  @Option(name = "-time_period", usage = "Time Period")
  private int time_period = 2 * 1000;
  @Option(name = "-time_period_increment",
      usage = "Time period increment. stabilization factor")
  private long time_period_increment = 200;
  @Option(name = "-missed_hb_threshold", usage = "Missed HB Threshold")
  private int missed_hb_threshold = 2;
  @Option(name = "-ndb_jar", usage = "NDB Implementation Driver JAR Path")
  private String driver_jar =
      "/home/salman/NetbeanProjects/hop/hops-metadata-dal-impl-ndb/target/hops-metadata-dal-impl-ndb-1.1-SNAPSHOT-jar-with-dependencies.jar";
  @Option(name = "-max_processes", usage = "Max number of processes")
  private int max_processes = 20;
  @Option(name = "-process_join_wait_time",
      usage = "Process join wait time. 0 for no wait, -1 for random wait between [0, time_period), and > 1 for fixed wait")
  private int process_join_wait_time = -1;
  @Option(name = "-number_of_leaders_to_kill",
      usage = "Number of Leaders to kill")
  private int number_of_leaders_to_kill = 10;
  @Option(name = "-consider_stable_after",
      usage = "If the time_period does not change for this long then the system is considered to be stable")
  private long consider_stable_after = 10 * 1 * 1000;
  @Option(name = "-max_stabilization_wait_time",
      usage = "Maximum wait time to see if the system has stabilized. it should be > consider_stable_after")
  private long max_stabilization_wait_time = 1 * 60 * 1000;
  @Option(name = "-output_file_path", usage = "Output File")
  private String output_file_path = "results.txt";
  private final String HTTP_ADDRESS = "dummy.address.com:9999";
  private final String RPC_ADDRESS = "repc.server.ip:0000";
  private final String DRIVER_CLASS = "io.hops.metadata.ndb.NdbStorageFactory";
  private final String DFS_STORAGE_DRIVER_CONFIG_FILE = "ndb-config.properties";
  //private final List<Long> times = new ArrayList<Long>();
  private final DescriptiveStatistics stats = new DescriptiveStatistics();
  private long stable_time_period;

  public static void main(String[] argv) throws Exception {
    Experiment1 exp = new Experiment1();
    exp.runExperiment(argv);
    System.exit(0);
  }

  public void runExperiment(String[] args)
      throws StorageInitializtionException, StorageException, IOException,
      ClassNotFoundException, InterruptedException {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);

    try {
      // parse the arguments.
      parser.parseArgument(args);
      OptionHandler hd;

    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println();
      return;
    }

    init();

    writeStartMessages(args);

    startProcesses();

    waitAllJoin();

    waitForTheSystemToStabilize();

    killLeaders();

    writeResults();

    tearDown();
  }

  private void init()
      throws StorageInitializtionException, StorageException, IOException,
      ClassNotFoundException {
    LogManager.getRootLogger().setLevel(Level.INFO);
    nnList = new ArrayList<LightWeightNameNode>();
    LEStorageFactory.setConfiguration(driver_jar, DRIVER_CLASS,
        DFS_STORAGE_DRIVER_CONFIG_FILE);
    LEStorageFactory.formatStorage();
    VarsRegister.registerHdfsDefaultValues();
  }

  private void tearDown() {
    //stop all NN
    LOG.info("TearDown ... ");
    for (LightWeightNameNode nn : nnList) {
      nn.stop();
    }
  }

  private void startProcesses() throws InterruptedException, IOException {
    //create 10 NN
    Random rand = new Random(System.currentTimeMillis());
    for (int i = 0; i < max_processes; i++) {
      startAProcess();

      //0 for no wait, -1 for random wait between [0, time_period), and > 1 for fixed wait
      if (process_join_wait_time == 0) {
        continue;
      } else if (process_join_wait_time == -1) {
        Thread.sleep(rand.nextInt(time_period));
      } else if (process_join_wait_time > 0) {
        Thread.sleep(process_join_wait_time);
      } else {
        writeMessageToFile("Unsupported process wait time. Fix process args ");
        System.exit(-1);
      }
    }
  }

  private void waitAllJoin() throws InterruptedException, IOException {
    final long max_wait_time = 10 * 60 * 1000;
    long start_time = System.currentTimeMillis();
    boolean all_processes_started = false;

    while ((System.currentTimeMillis() - start_time) < max_wait_time) {
      // ask the last process how many member thinks 
      try {
        if (nnList.get(nnList.size() - 1).getActiveNameNodes().size() ==
            nnList.size()) {
          all_processes_started = true;
          break;
        }
        LOG.info(
            "Experiment. The last process does not have complete list of processes. Got " +
                nnList.get(nnList.size() - 1).getActiveNameNodes().size() +
                " expecting " + nnList.size());
        Thread.sleep(1000);

      } catch (NullPointerException e) {
        LOG.error("Null pointer error in join");
      }
    }

    if (!all_processes_started) {
      writeMessageToFile(
          "Waiting for all processes to join is taking too long ...");
      System.exit(-1);
    }
  }

  private void waitForTheSystemToStabilize()
      throws IOException, InterruptedException {
    final long start_time = System.currentTimeMillis();
    long last_time_period = -1;
    long last_time_period_change_time = -1;
    boolean system_stable = false;

    Random rand = new Random(System.currentTimeMillis());
    while ((System.currentTimeMillis() - start_time) <
        max_stabilization_wait_time) {

      // ask random node about the time_period
      long new_time_period =
          nnList.get(rand.nextInt(nnList.size())).getLeTimePeriod();
      if (last_time_period != new_time_period) {
        last_time_period = new_time_period;
        last_time_period_change_time = System.currentTimeMillis();
      } else {
        if ((System.currentTimeMillis() - last_time_period_change_time) >
            consider_stable_after) {
          writeMessageToFile("After join the system stabilized in " +
              (System.currentTimeMillis() - start_time) +
              " ms. Time period is " + last_time_period);
          stable_time_period = last_time_period;
          system_stable = true;
          break;
        }
      }
      Thread.sleep(1000);
      LOG.info(
          "Experiment. System has not yet stabilized. TP " + last_time_period +
              " since " +
              (System.currentTimeMillis() - last_time_period_change_time));
    }

    if (!system_stable) {
      writeMessageToFile("The system did not stabilize ... ");
      System.exit(-1);
    }
  }

  private void killLeaders() {
    try {
      long last_leader_kill_time = 0;
      LightWeightNameNode leader_killed = null;
      int leadersKilled = 0;
      LOG.info("Experiment. going to start killing nodes");
      while (leadersKilled < number_of_leaders_to_kill) {
        LightWeightNameNode current_leader = getCurrentLeader();

        if (leader_killed == null || (current_leader != null &&
            current_leader.getLeCurrentId() !=
                leader_killed.getLeCurrentId())) // new leader elected
        {
          if (leader_killed != null) {
            long failOverTime =
                (System.currentTimeMillis() - last_leader_kill_time);
            long failOverLowerBound = leader_killed.getLeTimePeriod();
            long failOverUpperBound =
                leader_killed.getLeTimePeriod() * (missed_hb_threshold + 1);
            if (!(failOverTime > failOverLowerBound &&
                failOverTime < failOverUpperBound)) {
              //writeMessageToFile("Leader election time does not correspond to upper and lower bounds. Lower Bound: " + failOverLowerBound + " Upper Bound: " + failOverUpperBound + " Leader Failover Time: " + failOverTime);
            }
            writeMessageToFile("New Leader Elected. Old Leader Id " +
                leader_killed.getLeCurrentId() + " new Leader Id " +
                current_leader.getLeCurrentId() + " New leader elected in " +
                (failOverTime));
            stats.addValue(failOverTime);
          }

          LOG.info("Experiment. going to start a new process");
          startAProcess();
          LOG.info("Experiment. new process started");
          writeMessageToFile("Experiment. Stopping the leader process ... Id " +
              current_leader.getLeCurrentId());
          long killstarttime = System.currentTimeMillis();
          current_leader.stop();
          while (!current_leader.getLeaderElectionInstance().isStopped()) {
            Thread.sleep(1);
          }
          last_leader_kill_time = System.currentTimeMillis();
          LOG.info("Experiment. Stopped the leader process in " +
              (last_leader_kill_time - killstarttime));
          leader_killed = current_leader;
          leadersKilled++;
        }

        if (last_leader_kill_time > 0) {
          long max_wait_for_fail_over = 5 * 60 * 1000;
          if ((System.currentTimeMillis() - last_leader_kill_time) >
              max_wait_for_fail_over) {
            writeMessageToFile("Taking very long to elect a new leader ...");
            System.exit(-1);
          }
        }
      }
    } catch (Exception e) {
      try {
        writeMessageToFile(
            "Got an exception that is not properly handled " + e);
        e.printStackTrace();
      } catch (IOException ex) {
        Logger.getLogger(Experiment1.class.getName())
            .log(java.util.logging.Level.SEVERE, null, ex);
      }
      
    }
  }

  private void writeStartMessages(String[] argv) throws IOException {
    writeMessageToFile(
        "\n\n==========================================================================");
    writeMessageToFile("Params " + Arrays.toString(argv));
    writeMessageToFile(
        "--------------------------------------------------------------------------");
  }

  private void writeResults() throws IOException {


    writeMessageToFile(
        "Experiment Finished Sucessfully. Data " + max_processes + ", " +
            stable_time_period + ", " + stats.getMin() + ", " + stats.getMax() +
            ", " + stats.getMean() + ", " + stats.getVariance() + ", " +
            stats.getStandardDeviation() + ", " +
            (stats.getStandardDeviation() / Math.sqrt(stats.getN())));
    writeMessageToFile("DataPoints: " + stable_time_period + " " +
        Arrays.toString(stats.getValues()));
  }

  private LightWeightNameNode getCurrentLeader() throws IOException {
    int leaderCount = 0;
    LightWeightNameNode leader = null;
    for (int i = nnList.size() - 1; i >= 0; i--) {
      if (nnList.get(i).isLeader()) {
        leaderCount++;
        leader = nnList.get(i);
      }
    }

    if (leaderCount > 1) {
      writeMessageToFile("Wrong number of leaders. Found " + leaderCount);
      System.exit(-1);
    } else if (leaderCount == 0) {
      // writeMessageToFile("No Leader Elected Yet." + leaderCount);
    }
    return leader;
  }

  public void startAProcess() throws IOException, InterruptedException {
    int tries = 100;
    while (tries >= 0) {
      tries--;
      try {
        LightWeightNameNode nn =
            new LightWeightNameNode(new HdfsLeDescriptorFactory(), time_period,
                missed_hb_threshold, time_period_increment, HTTP_ADDRESS,
                RPC_ADDRESS);
        nnList.add(nn);
        return;
      } catch (Throwable e) {
        LOG.warn("Could not create a process. Retrying (tries left " + tries +
            ")... Exception was  " + e.getMessage());
        e.printStackTrace();
        Random rand = new Random(System.currentTimeMillis());
        Thread.sleep(rand.nextInt(5000));
      }
    }
    writeMessageToFile("Unable to start a process. Experiment failed ...");
    System.exit(-1);
  }

  public void writeMessageToFile(String message) throws IOException {
    LOG.info(message);
    PrintWriter out = new PrintWriter(
        new BufferedWriter(new FileWriter(output_file_path, true)));
    out.println(message);
    out.close();
  }
}
