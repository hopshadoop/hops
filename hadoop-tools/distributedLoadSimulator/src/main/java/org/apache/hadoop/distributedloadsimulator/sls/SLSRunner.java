/*
 * Copyright 2015 Apache Software Foundation.
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
package org.apache.hadoop.distributedloadsimulator.sls;

/**
 *
 * @author sri
 */
import io.hops.metadata.util.RMStorageFactory;
import io.hops.metadata.util.YarnAPIStorageFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import static java.lang.Thread.sleep;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.distributedloadsimulator.sls.appmaster.AMSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.appmaster.MRAMSimulator;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.distributedloadsimulator.sls.utils.SLSUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.distributedloadsimulator.sls.conf.SLSConfiguration;
import org.apache.hadoop.distributedloadsimulator.sls.nodemanager.NMSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.TaskRunner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Priority;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

public class SLSRunner implements AMNMCommonObject {

  private ResourceManager rm;
  private static final TaskRunner nodeRunner = new TaskRunner();
  private static final TaskRunner applicationRunner = new TaskRunner();
  private final String[] inputTraces;
  private final Configuration conf;
  private final Map<String, Integer> queueAppNumMap;

  // NM simulator
  private static HashMap<NodeId, NMSimulator> nmMap;
  private int nmMemoryMB, nmVCores;
  private int containerMemoryMB;
  private final String nodeFile;

  // AM simulator
  private int AM_ID;
  private Map<String, AMSimulator> amMap;
  private final Set<String> trackedApps;
  private final Map<String, Class> amClassMap;
  private static AtomicInteger remainingApps = new AtomicInteger(0);

  // metrics
  private final String metricsOutputDir;
  private final boolean printSimulation;
  private boolean yarnNode = false;
  private AtomicBoolean firstAMRegistration = new AtomicBoolean(false);
  private static boolean distributedmode;
  private final boolean loadsimulatormode;
  private static boolean stopAppSimulation = false;
  private static final boolean calculationDone = false;
  private boolean isNMRegisterationDone = false;

  // other simulation information
  private int numNMs, numRacks, numAMs, numTasks;
  private long maxRuntime;
  public final static Map<String, Object> simulateInfoMap
          = new HashMap<String, Object>();

  // logger
  public final static Logger LOG = Logger.getLogger(SLSRunner.class);

  private final String rmAddress;
  private int numberOfRT = 0;
  private final String[] rtAddresses = new String[5]; // this is kind of fixed,
  ResourceTracker[] resourceTrackers = new ResourceTracker[5];

  private int totalJobRunningTimeSec = 0;
  protected YarnClient rmClient;

  private static float hbResponsePercentage;
  private String listOfRMIIpAddress = null;
  Map<String, AMNMCommonObject> remoteConnections
          = new HashMap<String, AMNMCommonObject>();

  private static long firstHBTimeStamp = 0;
  private static boolean isFirstBeat = true;
  private boolean isLeader = false;
  private long simulationDuration;
  int nmHeartbeatInterval;
  
  public SLSRunner(String inputTraces[], String nodeFile,
          String outputDir, Set<String> trackedApps,
          boolean printsimulation, boolean yarnNodeDeployment,
          boolean distributedMode,
          boolean loadSimMode, String resourceTrackerAddress,
          String resourceManagerAddress,
          String rmiAddress, boolean isLeader, long simulationDuration)
          throws IOException, ClassNotFoundException {
    this.rm = null;
    this.isLeader = isLeader;
    this.simulationDuration = simulationDuration;
    this.yarnNode = yarnNodeDeployment;
    distributedmode = distributedMode;
    this.loadsimulatormode = loadSimMode;
    if (resourceTrackerAddress.split(",").length == 1) { // so we only have one RT
      this.rtAddresses[0] = resourceTrackerAddress;
      this.numberOfRT = 1;
    } else {
      for (int i = 0; i < resourceTrackerAddress.split(",").length; ++i) {
        rtAddresses[i] = resourceTrackerAddress.split(",")[i];
      }
      this.numberOfRT = resourceTrackerAddress.split(",").length;
    }
    this.rmAddress = resourceManagerAddress;
    this.inputTraces = inputTraces.clone();
    this.nodeFile = nodeFile;
    this.trackedApps = trackedApps;
    this.printSimulation = printsimulation;
    metricsOutputDir = outputDir;
    this.listOfRMIIpAddress = rmiAddress;

    nmMap = new HashMap<NodeId, NMSimulator>();
    queueAppNumMap = new HashMap<String, Integer>();
    amMap = new HashMap<String, AMSimulator>();
    amClassMap = new HashMap<String, Class>();

    // runner configuration
    conf = new Configuration();
    conf.addResource("sls-runner.xml");
    // runner
    int poolSize = conf.getInt(SLSConfiguration.NM_RUNNER_POOL_SIZE,
            SLSConfiguration.NM_RUNNER_POOL_SIZE_DEFAULT);
    SLSRunner.nodeRunner.setQueueSize(poolSize);
    SLSRunner.applicationRunner.setQueueSize(poolSize);
    // <AMType, Class> map
    for (Map.Entry e : conf) {
      String key = e.getKey().toString();
      if (key.startsWith(SLSConfiguration.AM_TYPE)) {
        String amType = key.substring(SLSConfiguration.AM_TYPE.length());
        amClassMap.put(amType, Class.forName(conf.get(key)));
      }
    }

    containerMemoryMB = conf.getInt(SLSConfiguration.CONTAINER_MEMORY_MB,
            SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
  }

  private String[] getRMIAddress() {
    return this.listOfRMIIpAddress.split(",");
  }

  public void initializeYarnClientForAMSimulation() {
    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.setStrings(YarnConfiguration.RM_ADDRESS, rmAddress);
    rmClient = YarnClient.createYarnClient();
    rmClient.init(yarnConf);
    rmClient.start();
  }

  public static void measureFirstBeat() {
    if (isFirstBeat) {
      firstHBTimeStamp = System.currentTimeMillis();
      isFirstBeat = false;
    }
  }
  long lastMonitoring = 0;

  public void startHbMonitorThread() {
    LOG.info("start Heartbeat monitor");
    Thread hbExperimentalMonitoring = new Thread() {
      @Override
      public void run() {
        while (true) {
          try {
            sleep(5000);
          } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(SLSRunner.class.getName()).log(
                    Level.SEVERE, null, ex);
          }
          int totalHb = 0;
          int trueTotalHb = 0;
          for (NMSimulator nm : nmMap.values()) {
            totalHb += nm.getTotalHeartBeat();
            trueTotalHb += nm.getTotalTrueHeartBeat();
          }
          if (totalHb != 0) {
            float hbExperimentailResponsePercentage = (float) ((trueTotalHb
                    - lastLocalSCHB) * 100) / (totalHb - lastLocalRTHB);
            float runningTime = ((float) (System.currentTimeMillis()
                    - lastMonitoring));
            float numberOfIdealHb = ((float) nmMap.size() / nmHeartbeatInterval) * runningTime;
            float idealHbPer = (float) ((totalHb - lastLocalRTHB) * 100)
                    / numberOfIdealHb;
            float trueHb = (float) ((trueTotalHb - lastLocalSCHB) * 100)
                    / numberOfIdealHb;
            LOG.info("HeartBeat Monitor I :" + idealHbPer + " \t Tr : " + trueHb
                    + "\t Ex : " + hbExperimentailResponsePercentage
                    + "\t TotHB : " + (totalHb - lastLocalRTHB) + "\t TrHB : "
                    + (trueTotalHb - lastLocalSCHB) + "\t clusterUsage : "
                    + lastClusterUsage);
          }
          lastMonitoring = System.currentTimeMillis();
          lastLocalRTHB = totalHb;
          lastLocalSCHB = trueTotalHb;
        }
      }
    };
    hbExperimentalMonitoring.start();
  }

  public void start() throws Exception {

    if (loadsimulatormode) {
      // here we only need to start the load and send rt and scheduler
      startNM();
      // this sleep is important, it is possible where registeration time is fater than the simulator starting time :(. so lets give
      // some time to other instance to start
      Thread.sleep(3000);
      getAllRemoteConnections();
      initializeYarnClientForAMSimulation();
      for (AMNMCommonObject remoteCon : remoteConnections.values()) {
        while (!remoteCon.isNMRegisterationDone()) {
          Thread.sleep(1000);
        }
      }

      // start application masters
      if (!stopAppSimulation) {
        LOG.info(
                "Starting the applicatoin simulator from ApplicationMaster traces");
        startAMFromSLSTraces();
      }
      numAMs = amMap.size();
      remainingApps.set(numAMs);
      // this method will be used for only experimental purpose. Every 5 sec , it will print the hb handled percentage
      //just to get some idea about the experiment.
      startHbMonitorThread();
    } else if (distributedmode) {
      // before start the rm , let rm to read and get to know about number of applications
      startAMFromSLSTraces();
      startRM();
      ((ResourceSchedulerWrapper) rm.getResourceScheduler())
              .setQueueSet(this.queueAppNumMap.keySet());
      ((ResourceSchedulerWrapper) rm.getResourceScheduler())
              .setTrackedAppSet(this.trackedApps);
    }
    printSimulationInfo();
    nodeRunner.start();
    applicationRunner.start();
  }

  private void startRM() throws IOException, ClassNotFoundException {
    Configuration rmConf = new YarnConfiguration();

    rmConf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    rmConf.setBoolean(YarnConfiguration.HOPS_DISTRIBUTED_RT_ENABLED, true);
    rmConf.setBoolean(YarnConfiguration.HOPS_NDB_EVENT_STREAMING_ENABLED, true);
    rmConf.setBoolean(YarnConfiguration.HOPS_NDB_RT_EVENT_STREAMING_ENABLED,
            true);
    rmConf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
    LOG.info(
            "HOP :: Load simulator is starting resource manager in distributed mode ######################### ");

    YarnAPIStorageFactory.setConfiguration(rmConf);
    RMStorageFactory.setConfiguration(rmConf);

    String schedulerClass = rmConf.get(YarnConfiguration.RM_SCHEDULER);
    rmConf.set(SLSConfiguration.RM_SCHEDULER, schedulerClass);
    rmConf.set(YarnConfiguration.RM_SCHEDULER,
            ResourceSchedulerWrapper.class.getName());
    rmConf.set(SLSConfiguration.METRICS_OUTPUT_DIR, metricsOutputDir);
    rm = new ResourceManager();
    rm.init(rmConf);
    rm.start();
  }

  private void getAllRemoteConnections() {
    String[] listOfIp = getRMIAddress();
    Registry remoteRegistry = null;
    for (String rmiIp : listOfIp) {
      while (true) {
        try {
          remoteRegistry = LocateRegistry.getRegistry(rmiIp);
          AMNMCommonObject remoteConnection = (AMNMCommonObject) remoteRegistry.
                  lookup("AMNMCommonObject");
          remoteConnections.put(rmiIp, remoteConnection);
          break;
        } catch (RemoteException ex) {
          LOG.error(ex, ex);
        } catch (NotBoundException ex) {
          LOG.error(ex, ex);
        }
      }
    }

  }

  private void startNM() throws YarnException, IOException {
    // nm configuration
    // 38GB
    nmMemoryMB = conf.getInt(SLSConfiguration.NM_MEMORY_MB,
            SLSConfiguration.NM_MEMORY_MB_DEFAULT);
    nmVCores = conf.getInt(SLSConfiguration.NM_VCORES,
            SLSConfiguration.NM_VCORES_DEFAULT);
    nmHeartbeatInterval = conf.getInt(
            SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS,
            SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    // nm information (fetch from topology file, or from sls/rumen json file)
    Set<String> nodeSet = new HashSet<String>();
    if (nodeFile.isEmpty()) {
      for (String inputTrace : inputTraces) {
        nodeSet.addAll(SLSUtils.parseNodesFromSLSTrace(inputTrace));
      }

    } else {
      nodeSet.addAll(SLSUtils.parseNodesFromNodeFile(nodeFile));
    }

    Configuration rtConf = new YarnConfiguration();
    int rtPort = rtConf.getPort(YarnConfiguration.RM_RESOURCE_TRACKER_PORT,
            YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_PORT);

    for (int i = 0; i < numberOfRT; ++i) {
      rtConf.setStrings(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
              rtAddresses[i]);
      resourceTrackers[i] = (ResourceTracker) RpcClientFactoryPBImpl.get().
              getClient(ResourceTracker.class, 1, rtConf.getSocketAddr(
                              YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_ADDRESS,
                              rtPort), rtConf);
    }
    // create NM simulators
    int counter = 0;
    Random random = new Random();
    Set<String> rackSet = new HashSet<String>();
    for (String hostName : nodeSet) {
      ++counter;
      // we randomize the heartbeat start time from zero to 1 interval
      LOG.info("Init nm: " + hostName + " (" + counter + ")");
      NMSimulator nm = new NMSimulator();
      nm.init(hostName, nmMemoryMB, nmVCores,
              random.nextInt(nmHeartbeatInterval), nmHeartbeatInterval, rm,
              resourceTrackers[counter % numberOfRT]);

      nmMap.put(nm.getNode().getNodeID(), nm);
      nodeRunner.schedule(nm);
      rackSet.add(nm.getNode().getRackName());

    }
    numRacks = rackSet.size();
    numNMs = nmMap.size();
    isNMRegisterationDone = true;
  }

  /**
   * parse workload information from sls trace files
   */
  @SuppressWarnings("unchecked")
  private void startAMFromSLSTraces() throws IOException, Exception {
    // parse from sls traces
    int heartbeatInterval = conf.getInt(
            SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
            SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    for (String inputTrace : inputTraces) {
      Reader input = new FileReader(inputTrace);
      try {
        Iterator<Map> i = mapper.readValues(jsonF.createJsonParser(input),
                Map.class);
        while (i.hasNext()) {
          Map jsonJob = i.next();

          long jobStartTime = Long.parseLong(
                  jsonJob.get("job.start.ms").toString());
          long jobFinishTime = Long.parseLong(
                  jsonJob.get("job.end.ms").toString());

          String user = (String) jsonJob.get("job.user");
          if (user == null) {
            user = "default";
          }
          String queue = jsonJob.get("job.queue.name").toString();

          String oldAppId = jsonJob.get("job.id").toString();
          totalJobRunningTimeSec = (int) jobFinishTime / 1000;// every time we update the time, so final time is total time
          int queueSize = queueAppNumMap.containsKey(queue)
                  ? queueAppNumMap.get(queue) : 0;
          queueSize++;
          queueAppNumMap.put(queue, queueSize);
          // tasks
          List tasks = (List) jsonJob.get("job.tasks");
          if (tasks == null || tasks.isEmpty()) {
            continue;
          }

          // create a new AM
          // appMastersList.add(new AppMasterParameter(queue, inputTrace, AM_ID++, rmAddress, rmiAddress));
          // if it is yarn node, don't execute applications
          String amType = jsonJob.get("am.type").toString();
          AMSimulator amSim = (AMSimulator) ReflectionUtils.newInstance(
                  amClassMap.get(amType), new Configuration(conf));
          if (amSim != null) {
            amSim.init(AM_ID++, heartbeatInterval, tasks, rm, this,
                    jobStartTime, jobFinishTime, user, queue, false, oldAppId,
                    getRMIAddress(), rmClient, new Configuration(conf));

            applicationRunner.schedule(amSim);
            maxRuntime = Math.max(maxRuntime, jobFinishTime);
            amMap.put(oldAppId, amSim);
            LOG.info("scheduled " + amMap.size());
          }
        }
      } finally {
        input.close();
      }
    }
    numAMs = amMap.size();
    remainingApps.set(numAMs);
  }

  private void printSimulationInfo() {
    if (printSimulation) {
      // node
      LOG.info("------------------------------------");
      LOG.info(MessageFormat.format("# nodes = {0}, # racks = {1}, capacity "
              + "of each node {2} MB memory and {3} vcores.",
              numNMs, numRacks, nmMemoryMB, nmVCores));
      LOG.info("------------------------------------");
      // job
      LOG.info(MessageFormat.format("# applications = {0}, # total "
              + "tasks = {1}, average # tasks per application = {2}",
              numAMs, numTasks, (int) (Math.ceil((numTasks + 0.0) / numAMs))));
      LOG.info("JobId\tQueue\tAMType\tDuration\t#Tasks");
      for (Map.Entry<String, AMSimulator> entry : amMap.entrySet()) {
        AMSimulator am = entry.getValue();
        LOG.info(entry.getKey() + "\t" + am.getQueue() + "\t" + am.getAMType()
                + "\t" + am.getDuration() + "\t" + am.getNumTasks());
      }
      LOG.info("------------------------------------");
      // queue
      LOG.info(MessageFormat.format("number of queues = {0}  average "
              + "number of apps = {1}", queueAppNumMap.size(),
              (int) (Math.ceil((numAMs + 0.0) / queueAppNumMap.size()))));
      LOG.info("------------------------------------");
      // runtime
      LOG.info(MessageFormat.format("estimated simulation time is {0}"
              + " seconds", (long) (Math.ceil(maxRuntime / 1000.0))));
      LOG.info("------------------------------------");
    }
    // package these information in the simulateInfoMap used by other places
    simulateInfoMap.put("Number of racks", numRacks);
    simulateInfoMap.put("Number of nodes", numNMs);
    simulateInfoMap.put("Node memory (MB)", nmMemoryMB);
    simulateInfoMap.put("Node VCores", nmVCores);
    simulateInfoMap.put("Number of applications", numAMs);
    simulateInfoMap.put("Number of tasks", numTasks);
    simulateInfoMap.put("Average tasks per applicaion",
            (int) (Math.ceil((numTasks + 0.0) / numAMs)));
    simulateInfoMap.put("Number of queues", queueAppNumMap.size());
    simulateInfoMap.put("Average applications per queue",
            (int) (Math.ceil((numAMs + 0.0) / queueAppNumMap.size())));
    simulateInfoMap.put("Estimated simulate time (s)",
            (long) (Math.ceil(maxRuntime / 1000.0)));
  }

  public HashMap<NodeId, NMSimulator> getNmMap() {
    return nmMap;
  }

  public static TaskRunner getApplicationRunner() {
    return applicationRunner;
  }

  public static TaskRunner getNodeRunner() {
    return nodeRunner;
  }

  public static void main(String args[]) throws Exception {
    Options options = new Options();
    options.addOption("inputsls", true, "input sls files");
    options.addOption("nodes", true, "input topology");
    options.addOption("output", true, "output directory");
    options.addOption("trackjobs", true,
            "jobs to be tracked during simulating");
    options.addOption("printsimulation", false,
            "print out simulation information");
    options.addOption("yarnnode", false, "taking boolean to enable rt mode");
    options.addOption("distributedmode", false,
            "taking boolean to enable scheduler mode");
    options.addOption("loadsimulatormode", false,
            "taking boolean to enable load simulator mode");
    options.addOption("rtaddress", true, "Resourcetracker address");
    options.addOption("rmaddress", true,
            "Resourcemanager  address for appmaster");
    options.addOption("parallelsimulator", false,
            "this is a boolean value to check whether to enable parallel simulator or not");
    options.addOption("rmiaddress", true,
            "Run a simulator on distributed mode, so we need rmi address");
    options.addOption("stopappsimulation", false,
            "we can stop the application simulation");
    options.addOption("isLeader", false, "leading slsRunner for the measurer");
    options.addOption("simulationDuration", true,
            "duration of the simulation only needed by the leader");

    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);

    String inputSLS = cmd.getOptionValue("inputsls");
    String output = cmd.getOptionValue("output");
    String rtAddress = cmd.getOptionValue("rtaddress"); // we are expecting the multiple rt, so input should be comma seperated
    String rmAddress = cmd.getOptionValue("rmaddress");
    String rmiAddress = "127.0.0.1";
    boolean isLeader = cmd.hasOption("isLeader");
    System.out.println(isLeader);
    long simulationDuration = 0;
    if (isLeader) {
      System.out.println(cmd.getOptionValue("simulationDuration"));
      simulationDuration = Long.parseLong(cmd.getOptionValue(
              "simulationDuration")) * 1000;
    }
    if ((inputSLS == null) || output == null) {
      System.err.println();
      System.err.println("ERROR: Missing input or output file");
      System.err.println();
      System.err.println("Options: -inputsls FILE,FILE... "
              + "-output FILE [-nodes FILE] [-trackjobs JobId,JobId...] "
              + "[-printsimulation]" + "[-distributedrt]");
      System.err.println();
      System.exit(1);
    }

    File outputFile = new File(output);
    if (!outputFile.exists()
            && !outputFile.mkdirs()) {
      System.err.println("ERROR: Cannot create output directory "
              + outputFile.getAbsolutePath());
      System.exit(1);
    }

    Set<String> trackedJobSet = new HashSet<String>();
    if (cmd.hasOption("trackjobs")) {
      String trackjobs = cmd.getOptionValue("trackjobs");
      String jobIds[] = trackjobs.split(",");
      trackedJobSet.addAll(Arrays.asList(jobIds));
    }

    String nodeFile = cmd.hasOption("nodes") ? cmd.getOptionValue("nodes") : "";

    String inputFiles[] = inputSLS.split(",");
    if (cmd.hasOption("stopappsimulation")) {
      stopAppSimulation = true;
      LOG.warn("Application simulation is disabled!!!!!!");
    }
    if (cmd.hasOption("parallelsimulator")) {
      //  then we need rmi address
      rmiAddress = cmd.getOptionValue("rmiaddress"); // currently we support only two simulator in parallel
    }
    SLSRunner sls = new SLSRunner(inputFiles, nodeFile, output,
            trackedJobSet, cmd.hasOption("printsimulation"), cmd.hasOption(
                    "yarnnode"), cmd.hasOption("distributedmode"), cmd.
            hasOption("loadsimulatormode"), rtAddress, rmAddress, rmiAddress,
            isLeader, simulationDuration
    );
    if (!cmd.hasOption("distributedmode")) {
      try {
        AMNMCommonObject stub = (AMNMCommonObject) UnicastRemoteObject.
                exportObject(sls, 0);
        // Bind the remote object's stub in the registry
        Registry registry = LocateRegistry.getRegistry();
        registry.bind("AMNMCommonObject", stub);
        LOG.info("HOP ::  SLS RMI Server ready on default RMI port ");
        sls.start();
      } catch (Exception e) {
        System.err.println("Server exception: " + e.toString());
        e.printStackTrace();
      }
    } else {
      sls.start();
    }
  }

  @Override
  public boolean isNodeExist(String nodeId) throws RemoteException {
    NodeId nId = ConverterUtils.toNodeId(nodeId);
    if (nmMap.containsKey(nId)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void addNewContainer(String containerId, String nodeId,
          String httpAddress,
          int memory, int vcores, int priority, long lifeTimeMS) throws
          RemoteException {
    Container container
            = BuilderUtils.newContainer(ConverterUtils.
                    toContainerId(containerId),
                    ConverterUtils.toNodeId(nodeId), httpAddress,
                    Resources.createResource(memory, vcores),
                    Priority.create(priority), null);

    // this we can move to thread queue to increase the performance, so we don't need to wait
    nmMap.get(container.getNodeId())
            .addNewContainer(container, lifeTimeMS);
  }

  @Override
  public void cleanupContainer(String containerId, String nodeId) throws
          RemoteException {
    nmMap.get(ConverterUtils.toNodeId(nodeId))
            .cleanupContainer(ConverterUtils.toContainerId(containerId));
  }

  @Override
  public int finishedApplicationsCount() {
    return remainingApps.get();
  }

  long simulationStart = 0;

  @Override
  public void registerApplicationTimeStamp() {
    if (!firstAMRegistration.getAndSet(true)) {
      simulationStart = System.currentTimeMillis();
      startMeasures = simulationStart;
      if (isLeader) {
        new Thread(new Measurer(simulationDuration, this)).start();
      }
      LOG.info("Application_initial_registeration_time : " + simulationStart);
    }
  }

  @Override
  public boolean isNMRegisterationDone() {
    return isNMRegisterationDone;
  }

  @Override
  public void decreseApplicationCount(String applicationId, boolean failed)
          throws RemoteException {

    if (!yarnNode) {
      int val = remainingApps.decrementAndGet();
      LOG.info("SLS decrease finished application - application count : " + val
              + " " + applicationId);

      if (failed) {
        appNotAllocated.incrementAndGet();
      }

      if (remainingApps.get() == 0) {
        this.simulationFinished();
        for (AMNMCommonObject remoteCon : remoteConnections.values()) {
          remoteCon.simulationFinished();
        }
        LOG.info("Distributed_Simulator_shutting_down_time : " + System.
                currentTimeMillis());

        System.exit(0);
      }
    }
  }

  @Override
  public int[] getHandledHeartBeats() {
    int hb[] = {0, 0};
    for (NMSimulator nm : nmMap.values()) {
      hb[0] += nm.getTotalHeartBeat();
      hb[1] += nm.getTotalTrueHeartBeat();

    }
    return hb;
  }

  @Override
  public int getNumberNodeManager() {
    return nmMap.size();
  }

  AtomicInteger nbFinished = new AtomicInteger(0);

  @Override
  public void simulationFinished() throws RemoteException {
    int finished = nbFinished.incrementAndGet();
    LOG.info("finish simulation " + finished);
    if (finished == getRMIAddress().length + 1) {
      computAndPrintStats();
      System.exit(0);
    }
  }

  private synchronized void computAndPrintStats() throws RemoteException {
    LOG.info("comput and print stats");
    long simulationDuration = System.currentTimeMillis() - startMeasures;
    int hb[] = this.getHandledHeartBeats();
    String rtHbDetail = "this: " + hb[0] + ", ";
    String scHbDetail = "this: " + hb[1] + ", ";
    hb[0] -= initialHB[0];
    hb[1] -= initialHB[1];
    int nbNM = this.getNumberNodeManager();
    int nbApplicationWaitTime = this.getNBApplicationMasterWaitTime();
    long totalApplicationWaitTime = this.getApplicationMasterWaitTime();
    int nbContainers = this.getNBContainers();
    long totalContainerAllocationWaitTime = this.
            getContainerAllocationWaitTime();
    long totalContainerStartTime = this.getContainerStartWaitTime();

    for (String conId : remoteConnections.keySet()) {
      AMNMCommonObject remoteCon = remoteConnections.get(conId);
      int remoteHb[] = remoteCon.getHandledHeartBeats();
      hb[0] += remoteHb[0];
      hb[1] += remoteHb[1];
      rtHbDetail = rtHbDetail + conId + ": " + remoteHb[0] + ", ";
      scHbDetail = scHbDetail + conId + ": " + remoteHb[1] + ", ";
      nbNM += remoteCon.getNumberNodeManager();
      nbApplicationWaitTime += remoteCon.getNBApplicationMasterWaitTime();
      totalApplicationWaitTime += remoteCon.getApplicationMasterWaitTime();
      nbContainers += remoteCon.getNBContainers();
      totalContainerAllocationWaitTime += remoteCon.
              getContainerAllocationWaitTime();
      totalContainerStartTime += remoteCon.getContainerStartWaitTime();
    }

    float numberOfIdealHb = ((float) nbNM / nmHeartbeatInterval) * simulationDuration;
    float rtHBRatio = (float) (hb[0] * 100) / numberOfIdealHb;
    float scHBRatio = (float) (hb[1] * 100) / numberOfIdealHb;

    float avgApplicationWaitTime = (float) totalApplicationWaitTime
            / nbApplicationWaitTime;
    float avgContainerAllocationWaitTime
            = (float) totalContainerAllocationWaitTime / nbContainers;
    float avgContainerStartTime = (float) totalContainerStartTime / nbContainers;

    Integer clusterCapacity = nmMap.size() * nmMemoryMB / containerMemoryMB;
    Integer usage = clusterUsages.poll();
    float usagePercent = (float) usage / clusterCapacity;
    float totalClusterUsage = usagePercent;
    String clusterUsageDetail = "" + usagePercent;
    int counter = 1;
    usage = clusterUsages.poll();
    while (usage != null) {
      usagePercent = (float) usage / clusterCapacity;
      totalClusterUsage += usagePercent;
      clusterUsageDetail = clusterUsageDetail + ", " + usagePercent;
      counter++;
      usage = clusterUsages.poll();
    }

    float avgClusterUsage = totalClusterUsage / counter;

    try {
      File file = new File("simulationsDuration");
      if (!file.exists()) {
        file.createNewFile();
      }
      FileWriter fileWritter = new FileWriter(file.getName(), true);
      BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
      bufferWritter.write(simulationDuration + " \t" + rtHBRatio + " ("
              + rtHbDetail + ")" + " \t"
              + scHBRatio + " (" + scHbDetail + ")" + " \t"
              + avgApplicationWaitTime + " \t"
              + avgContainerAllocationWaitTime + " \t" + avgContainerStartTime
              + " \t" + nbContainers + " \t" + avgClusterUsage + " \t"
              + appNotAllocated.get() + " \n");
      bufferWritter.close();

      file = new File("clusterUsageDetail");
      if (!file.exists()) {
        file.createNewFile();
      }
      fileWritter = new FileWriter(file.getName(), true);
      bufferWritter = new BufferedWriter(fileWritter);
      bufferWritter.write(clusterUsageDetail + "\n");
      bufferWritter.close();

    } catch (IOException e) {
      LOG.error(e);
    }

    LOG.info(
            "================== Result format:hpresponsepercentage,nmsize,amsize,totalhb,truetotalhb,totaljobrunningtieminsec ==================");
    LOG.info("Simulation: " + simulationDuration + " " + rtHBRatio + " "
            + scHBRatio);
  }

  public void finishSimulation() {
    try {
      computAndPrintStats();
    } catch (RemoteException e) {
      LOG.error(e, e);
    }
    for (AMNMCommonObject remoteCon : remoteConnections.values()) {
      try {
        remoteCon.kill();
      } catch (RemoteException e) {
        LOG.error(e, e);
      }
    }
    try {
      Thread.sleep(5000);
    } catch (InterruptedException ex) {
      java.util.logging.Logger.getLogger(SLSRunner.class.getName()).
              log(Level.SEVERE, null, ex);
    }
    System.exit(0);
  }

  public void kill() {
    new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ex) {
          java.util.logging.Logger.getLogger(SLSRunner.class.getName()).
                  log(Level.SEVERE, null, ex);
        }
        System.exit(0);
      }
    }).start();
    return;
  }

  AtomicLong totalApplicationWaitTime = new AtomicLong(0);
  AtomicInteger nbApplicationWaitTime = new AtomicInteger(0);
  AtomicLong totalContainerAllocationWaitTime = new AtomicLong(0);
  AtomicLong totalContainerStartWaitTime = new AtomicLong(0);
  AtomicInteger nbContainers = new AtomicInteger(0);
  AtomicInteger appNotAllocated = new AtomicInteger(0);
  private long startMeasures;
  int initialHB[] = {0, 0};
  int lastLocalRTHB = 0;
  int lastLocalSCHB = 0;

  public void startMeasures() {
    totalApplicationWaitTime.set(0);
    nbApplicationWaitTime.set(0);
    totalContainerAllocationWaitTime.set(0);
    totalContainerStartWaitTime.set(0);
    nbContainers.set(0);
    appNotAllocated.set(0);

    startMeasures = System.currentTimeMillis();

    LOG.info("HeartBeat Monitor reset");
    initialHB = this.getHandledHeartBeats();

    for (AMNMCommonObject remoteCon : remoteConnections.values()) {
      while (true) {
        try {
          int remoteInitialHB[] = remoteCon.getHandledHeartBeats();
          initialHB[0] += remoteInitialHB[0];
          initialHB[1] += remoteInitialHB[1];
          break;
        } catch (RemoteException e) {
          LOG.error(e, e);
        }
      }
    }
  }

  @Override
  public void addApplicationMasterWaitTime(long applicationMasterWaitTime)
          throws RemoteException {
    this.totalApplicationWaitTime.addAndGet(applicationMasterWaitTime);
    this.nbApplicationWaitTime.incrementAndGet();
  }

  public Long getApplicationMasterWaitTime() {
    return totalApplicationWaitTime.get();
  }

  public int getNBApplicationMasterWaitTime() {
    return nbApplicationWaitTime.get();
  }

  @Override
  public void addContainerAllocationWaitTime(long containerAllocationWaitTime)
          throws RemoteException {
    this.totalContainerAllocationWaitTime.addAndGet(containerAllocationWaitTime);
    this.nbContainers.incrementAndGet();
  }

  public Long getContainerAllocationWaitTime() {
    return totalContainerAllocationWaitTime.get();
  }

  public int getNBContainers() {
    return nbContainers.get();
  }

  @Override
  public void addContainerStartWaitTime(long containerStartWaitTime) throws
          RemoteException {
    this.totalContainerStartWaitTime.addAndGet(containerStartWaitTime);
  }

  public Long getContainerStartWaitTime() {
    return totalContainerStartWaitTime.get();
  }

  Queue<Integer> clusterUsages = new LinkedBlockingQueue<Integer>();
  float lastClusterUsage = 0;

  private class Measurer implements Runnable {

    final long xpDuration;
    final SLSRunner runner;

    public Measurer(long xpDuration, SLSRunner runner) {
      this.xpDuration = xpDuration;
      this.runner = runner;
    }

    public void run() {
      try {
        LOG.info("Measurer sleep for warmup: " + xpDuration / 4);
        Thread.sleep(xpDuration / 4);
        LOG.info("Measurer start measures for " + xpDuration / 2);
        runner.startMeasures();
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < xpDuration / 2) {
          long startLoop = System.currentTimeMillis();
          int clusterUsage = 0;
          for (NMSimulator nm : nmMap.values()) {
            clusterUsage += nm.getUsedResources();
          }
          clusterUsages.add(clusterUsage);
          Integer clusterCapacity = nmMap.size() * nmMemoryMB
                  / containerMemoryMB;
          lastClusterUsage = (float) clusterUsage / clusterCapacity;
          Thread.sleep(5000 - (System.currentTimeMillis() - startLoop));
        }
        LOG.info("Measurer finish measures");
        runner.finishSimulation();
      } catch (InterruptedException e) {
        LOG.error(e, e);
      }
    }
  }
}
