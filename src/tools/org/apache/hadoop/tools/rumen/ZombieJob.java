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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * {@link ZombieJob} is a layer above {@link LoggedJob} raw JSON objects.
 * 
 * Each <code>ZombieJob</code> object represents a job in job history. For
 * everything that exists in job history, contents are returned unchanged
 * faithfully. To get input splits of a non-exist task, a non-exist task
 * attempt, or an ill-formed task attempt, proper objects are made up from
 * statistical sketch.
 */
public class ZombieJob implements JobStory {

  private final LoggedJob job;

  private final Map<TaskID, LoggedTask> loggedTaskMap = new HashMap<TaskID, LoggedTask>();

  private final Map<TaskAttemptID, LoggedTaskAttempt> loggedTaskAttemptMap = new HashMap<TaskAttemptID, LoggedTaskAttempt>();

  private final Random random;

  private FileSplit[] splits;

  private final LoggedNetworkTopology topology;

  // TODO: Fix ZombieJob to initialize this correctly from observed data
  double rackLocalOverNodeLocal = 1.5;
  // TODO: Fix ZombieJob to initialize this correctly from observed data
  double rackRemoteOverNodeLocal = 3.0;

  /**
   * This constructor creates a {@link ZombieJob} with the same semantics as the
   * {@link LoggedJob} passed in this parameter
   * 
   * @param job
   *          The dead job this ZombieJob instance is based on.
   * @param topology
   *          The topology of the network where the dead job ran on.
   * @param seed
   *          Seed for the random number generator for filling in information
   *          not available from the ZombieJob.
   */
  public ZombieJob(LoggedJob job, LoggedNetworkTopology topology, long seed) {
    if (job == null || topology == null) {
      throw new IllegalArgumentException("job or topology is null");
    }
    this.job = job;
    this.topology = topology;
    random = new Random(seed);
  }

  /**
   * This constructor creates a {@link ZombieJob} with the same semantics as the
   * {@link LoggedJob} passed in this parameter
   * 
   * @param job
   *          The dead job this ZombieJob instance is based on.
   * @param topology
   *          The topology of the network where the dead job ran on.
   */
  public ZombieJob(LoggedJob job, LoggedNetworkTopology topology) {
    this(job, topology, System.nanoTime());
  }

  private State convertState(JobHistory.Values status) {
    if (status == JobHistory.Values.SUCCESS) {
      return State.SUCCEEDED;
    } else if (status == JobHistory.Values.FAILED) {
      return State.FAILED;
    } else if (status == JobHistory.Values.KILLED) {
      return State.KILLED;
    } else {
      throw new IllegalArgumentException("unknown status " + status);
    }
  }

  public JobConf getJobConf() {

    // TODO : add more to jobConf ?
    JobConf jobConf = new JobConf();
    jobConf.setJobName(getName());
    jobConf.setUser(getUser());
    jobConf.setNumMapTasks(getNumberMaps());
    jobConf.setNumReduceTasks(getNumberReduces());
    return jobConf;

  }

  @Override
  public InputSplit[] getInputSplits() {
    if (splits == null) {
      List<FileSplit> splitsList = new ArrayList<FileSplit>();
      Path emptyPath = new Path("/");
      for (LoggedTask mapTask : job.getMapTasks()) {
        ArrayList<LoggedLocation> locations = mapTask.getPreferredLocations();
        String[] hosts = new String[locations.size()];
        int i = 0;
        for (LoggedLocation location : locations) {
          List<String> layers = location.getLayers();
          if (layers.size() == 0) {
            continue;
          }
          String host = "";
          /*
           * for (String layer: location.getLayers()) { host +=
           * (File.separatorChar + layer); }
           */
          host = layers.get(layers.size() - 1);
          hosts[i++] = host;
        }
        long mapInputBytes = mapTask.getInputBytes();
        splitsList.add(new FileSplit(emptyPath, 0,
            ((mapInputBytes > 0) ? mapInputBytes : 0), hosts));
      }

      // If not all map tasks are in job trace, should make up some splits
      // for missing map tasks.
      int totalMaps = job.getTotalMaps();
      for (int i = splitsList.size(); i < totalMaps; i++) {
        ArrayList<String> hosts = new ArrayList<String>();
        // assume replication factor is 3.
        while (hosts.size() < 3) {
          List<LoggedNetworkTopology> racks = topology.getChildren();
          LoggedNetworkTopology rack = racks.get(random.nextInt(racks.size()));
          List<LoggedNetworkTopology> nodes = rack.getChildren();
          String host = nodes.get(random.nextInt(nodes.size())).getName();
          if (!hosts.contains(host)) {
            hosts.add(host);
          }
        }
        String[] hostsArray = hosts.toArray(new String[hosts.size()]);
        // TODO set size of a split to 0 now.
        splitsList.add(new FileSplit(emptyPath, 0, 0, hostsArray));
      }

      if (splitsList.size() == 0) {
        System.err.println(job.getMapTasks().size());
      }

      splits = splitsList.toArray(new FileSplit[splitsList.size()]);
    }
    return splits;
  }

  @Override
  public String getName() {
    String jobName = job.getJobName();
    if (jobName == null) {
      return "";
    } else {
      return jobName;
    }
  }

  @Override
  public JobID getJobID() {
    return JobID.forName(getLoggedJob().getJobID());
  }

  @Override
  public int getNumberMaps() {
    return job.getTotalMaps();
  }

  @Override
  public int getNumberReduces() {
    return job.getTotalReduces();
  }

  @Override
  public long getSubmissionTime() {
    return job.getSubmitTime() - job.getRelativeTime();
  }

  /**
   * Mask the job ID part in a {@link TaskID}.
   * 
   * @param taskId
   *          raw {@link TaskID} read from trace
   * @return masked {@link TaskID} with empty {@link JobID}.
   */
  private TaskID maskTaskID(TaskID taskId) {
    JobID jobId = new JobID();
    TaskType taskType = taskId.getTaskType();
    return new TaskID(jobId, taskType, taskId.getId());
  }

  /**
   * Mask the job ID part in a {@link TaskAttemptID}.
   * 
   * @param attemptId
   *          raw {@link TaskAttemptID} read from trace
   * @return masked {@link TaskAttemptID} with empty {@link JobID}.
   */
  private TaskAttemptID maskAttemptID(TaskAttemptID attemptId) {
    JobID jobId = new JobID();
    TaskType taskType = attemptId.getTaskType();
    TaskID taskId = attemptId.getTaskID();
    return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), taskType,
        taskId.getId(), attemptId.getId());
  }

  /**
   * Build task mapping and task attempt mapping, to be later used to find
   * information of a particular {@link TaskID} or {@link TaskAttemptID}.
   */
  private void buildMaps() {
    for (LoggedTask map : job.getMapTasks()) {
      loggedTaskMap.put(maskTaskID(TaskID.forName(map.taskID)), map);

      for (LoggedTaskAttempt mapAttempt : map.getAttempts()) {
        TaskAttemptID id = TaskAttemptID.forName(mapAttempt.getAttemptID());
        loggedTaskAttemptMap.put(maskAttemptID(id), mapAttempt);
      }
    }
    for (LoggedTask reduce : job.getReduceTasks()) {
      loggedTaskMap.put(maskTaskID(TaskID.forName(reduce.taskID)), reduce);

      for (LoggedTaskAttempt reduceAttempt : reduce.getAttempts()) {
        TaskAttemptID id = TaskAttemptID.forName(reduceAttempt.getAttemptID());
        loggedTaskAttemptMap.put(maskAttemptID(id), reduceAttempt);
      }
    }

    // do not care about "other" tasks, "setup" or "clean"
  }

  @Override
  public String getUser() {
    return job.getUser();
  }

  /**
   * Get the underlining {@link LoggedJob} object read directly from the trace.
   * This is mainly for debugging.
   * 
   * @return the underlining {@link LoggedJob} object
   */
  public LoggedJob getLoggedJob() {
    return job;
  }

  /**
   * Get a {@link TaskAttemptInfo} with a {@link TaskAttemptID} associated with
   * taskType, taskNumber, and taskAttemptNumber. This function does not care
   * about locality, and follows the following decision logic: 1. Make up a
   * {@link TaskAttemptInfo} if the task attempt is missing in trace, 2. Make up
   * a {@link TaskAttemptInfo} if the task attempt has a KILLED final status in
   * trace, 3. Otherwise (final state is SUCCEEDED or FAILED), construct the
   * {@link TaskAttemptInfo} from the trace.
   */
  public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType, int taskNumber,
      int taskAttemptNumber) {
    // does not care about locality. assume default locality is NODE_LOCAL.
    // But if both task and task attempt exist in trace, use logged locality.
    int locality = 0;
    LoggedTask loggedTask = getLoggedTask(taskType, taskNumber);
    if (loggedTask == null) {
      // TODO insert parameters
      TaskInfo taskInfo = new TaskInfo(0, 0, 0, 0, 0);
      return makeUpInfo(taskType, taskInfo, taskAttemptNumber, locality);
    }

    LoggedTaskAttempt loggedAttempt = getLoggedTaskAttempt(taskType,
        taskNumber, taskAttemptNumber);
    if (loggedAttempt == null) {
      // Task exists, but attempt is missing.
      TaskInfo taskInfo = getTaskInfo(loggedTask);
      return makeUpInfo(taskType, taskInfo, taskAttemptNumber, locality);
    } else {
      // Task and TaskAttempt both exist.
      try {
        return getInfo(loggedTask, loggedAttempt);
      } catch (IllegalArgumentException e) {
        if (e.getMessage().startsWith("status cannot be")) {
          TaskInfo taskInfo = getTaskInfo(loggedTask);
          return makeUpInfo(taskType, taskInfo, taskAttemptNumber, locality);
        } else {
          throw e;
        }
      }
    }
  }

  @Override
  public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {
    return getTaskInfo(getLoggedTask(taskType, taskNumber));
  }

  /**
   * Get a {@link TaskAttemptInfo} with a {@link TaskAttemptID} associated with
   * taskType, taskNumber, and taskAttemptNumber. This function considers
   * locality, and follows the following decision logic: 1. Make up a
   * {@link TaskAttemptInfo} if the task attempt is missing in trace, 2. Make up
   * a {@link TaskAttemptInfo} if the task attempt has a KILLED final status in
   * trace, 3. If final state is FAILED, construct a {@link TaskAttemptInfo}
   * from the trace, without considering locality. 4. If final state is
   * SUCCEEDED, construct a {@link TaskAttemptInfo} from the trace, with runtime
   * scaled according to locality in simulation and locality in trace.
   */
  @Override
  public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(int taskNumber,
      int taskAttemptNumber, int locality) {
    TaskType taskType = TaskType.MAP;
    LoggedTask loggedTask = getLoggedTask(taskType, taskNumber);
    if (loggedTask == null) {
      // TODO insert parameters
      TaskInfo taskInfo = new TaskInfo(0, 0, 0, 0, 0);
      return makeUpInfo(taskType, taskInfo, taskAttemptNumber, locality);
    }

    LoggedTaskAttempt loggedAttempt = getLoggedTaskAttempt(taskType,
        taskNumber, taskAttemptNumber);
    if (loggedAttempt == null) {
      // Task exists, but attempt is missing.
      TaskInfo taskInfo = getTaskInfo(loggedTask);
      return makeUpInfo(taskType, taskInfo, taskAttemptNumber, locality);
    } else {
      // Task and TaskAttempt both exist.
      if (loggedAttempt.getResult() == Values.KILLED) {
        TaskInfo taskInfo = getTaskInfo(loggedTask);
        return makeUpInfo(taskType, taskInfo, taskAttemptNumber, locality);
      } else if (loggedAttempt.getResult() == Values.FAILED) {
        // FAILED attempt is not affected by locality
        // XXX however, made-up FAILED attempts ARE affected by locality, since
        // statistics are present for attempts of different locality.
        return getInfo(loggedTask, loggedAttempt);
      } else if (loggedAttempt.getResult() == Values.SUCCESS) {
        int loggedLocality = getLocality(loggedTask, loggedAttempt);
        if (locality == loggedLocality) {
          return getInfo(loggedTask, loggedAttempt);
        } else {
          // attempt succeeded in trace. It is scheduled in simulation with
          // a different locality.
          return scaleInfo(loggedTask, loggedAttempt, locality, loggedLocality,
              rackLocalOverNodeLocal, rackRemoteOverNodeLocal);
        }
      } else {
        throw new IllegalArgumentException(
            "attempt result is not SUCCEEDED, FAILED or KILLED: "
                + loggedAttempt.getResult());
      }
    }
  }

  private TaskAttemptInfo scaleInfo(LoggedTask loggedTask,
      LoggedTaskAttempt loggedAttempt, int locality, int loggedLocality,
      double rackLocalOverNodeLocal, double rackRemoteOverNodeLocal) {
    TaskInfo taskInfo = getTaskInfo(loggedTask);
    double[] factors = new double[] { 1.0, rackLocalOverNodeLocal,
        rackRemoteOverNodeLocal };
    double scaleFactor = factors[locality] / factors[loggedLocality];
    State state = convertState(loggedAttempt.getResult());
    if (loggedTask.getTaskType() == Values.MAP) {
      long taskTime = 0;
      if (loggedAttempt.getStartTime() == 0) {
        taskTime = makeUpMapRuntime(state, locality);
      } else {
        taskTime = loggedAttempt.getFinishTime() - loggedAttempt.getStartTime();
      }
      taskTime *= scaleFactor;
      return new MapTaskAttemptInfo(state, taskInfo, taskTime);
    } else if (loggedTask.getTaskType() == Values.REDUCE) {
      /*
       * long shuffleTime = (loggedAttempt.getShuffleFinished() -
       * loggedAttempt.getStartTime()); long sortTime =
       * (loggedAttempt.getSortFinished() - loggedAttempt.getShuffleFinished());
       * long reduceTime = (loggedAttempt.getFinishTime() -
       * loggedAttempt.getSortFinished()); reduceTime *= scaleFactor; return new
       * ReduceTaskAttemptInfo(convertState(loggedAttempt.getResult()),
       * taskInfo, shuffleTime, sortTime, reduceTime);
       */
      throw new IllegalArgumentException("taskType cannot be REDUCE");
    } else {
      throw new IllegalArgumentException("taskType is neither MAP nor REDUCE: "
          + loggedTask.getTaskType());
    }
  }

  private int getLocality(LoggedTask loggedTask, LoggedTaskAttempt loggedAttempt) {
    ParsedHost host = getParsedHost(loggedAttempt.getHostName());
    int distance = 2;
    for (LoggedLocation location : loggedTask.getPreferredLocations()) {
      ParsedHost dataNode = new ParsedHost(location);
      distance = Math.min(distance, host.distance(dataNode));
    }
    return distance;
  }

  private ParsedHost getParsedHost(String hostName) {
    try {
      return new ParsedHost(hostName);
    } catch (IllegalArgumentException e) {
      // look up the host in topology, return a correct hostname
      for (LoggedNetworkTopology rack : topology.getChildren()) {
        for (LoggedNetworkTopology host : rack.getChildren()) {
          if (host.getName().equals(hostName)) {
            return new ParsedHost(
                new String[] { rack.getName(), host.getName() });
          }
        }
      }
      return new ParsedHost(new String[] { "default-rack", hostName });
    }
  }

  private TaskAttemptInfo getInfo(LoggedTask loggedTask,
      LoggedTaskAttempt loggedAttempt) {
    TaskInfo taskInfo = getTaskInfo(loggedTask);
    State state = convertState(loggedAttempt.getResult());
    if (loggedTask.getTaskType() == Values.MAP) {
      long taskTime;
      if (loggedAttempt.getStartTime() == 0) {
        int locality = getLocality(loggedTask, loggedAttempt);
        taskTime = makeUpMapRuntime(state, locality);
      } else {
        taskTime = loggedAttempt.getFinishTime() - loggedAttempt.getStartTime();
      }
      if (taskTime < 0) {
        throw new IllegalStateException(loggedAttempt.getAttemptID()
            + " taskTime<0: " + taskTime);
      }
      return new MapTaskAttemptInfo(state, taskInfo, taskTime);
    } else if (loggedTask.getTaskType() == Values.REDUCE) {
      long startTime = loggedAttempt.getStartTime();
      long mergeDone = loggedAttempt.getSortFinished();
      long shuffleDone = loggedAttempt.getShuffleFinished();
      long finishTime = loggedAttempt.getFinishTime();
      if (startTime <= 0 || startTime >= finishTime) {
        // have seen startTime>finishTime.
        // haven't seen reduce task with startTime=0 ever. But if this happens,
        // make up a reduceTime with no shuffle/merge.
        long reduceTime = makeUpReduceRuntime(state);
        return new ReduceTaskAttemptInfo(state, taskInfo, 0, 0, reduceTime);
      } else {
        if (shuffleDone <= 0) {
          shuffleDone = startTime;
        }
        if (mergeDone <= 0) {
          mergeDone = finishTime;
        }
        long shuffleTime = shuffleDone - startTime;
        long mergeTime = mergeDone - shuffleDone;
        long reduceTime = finishTime - mergeDone;
        if (reduceTime < 0) {
          throw new IllegalStateException(loggedAttempt.getAttemptID()
              + " reduceTime<0: " + reduceTime);
        }
        return new ReduceTaskAttemptInfo(state, taskInfo, shuffleTime,
            mergeTime, reduceTime);
      }
    } else {
      throw new IllegalArgumentException("taskType is neither MAP nor REDUCE: "
          + loggedTask.getTaskType());
    }
  }

  private TaskInfo getTaskInfo(LoggedTask loggedTask) {
    // TODO insert maxMemory
    TaskInfo taskInfo = new TaskInfo(loggedTask.getInputBytes(),
        (int) (loggedTask.getInputBytes() / loggedTask.getInputRecords()),
        loggedTask.getOutputBytes(),
        (int) (loggedTask.getOutputBytes() / loggedTask.getOutputRecords()), 0);
    return taskInfo;
  }

  private TaskAttemptInfo makeUpInfo(TaskType taskType, TaskInfo taskInfo,
      int taskAttemptNumber, int locality) {
    if (taskType == TaskType.MAP) {
      State state = State.SUCCEEDED;
      long runtime = 0;

      // make up state
      state = makeUpState(taskAttemptNumber, job.getMapperTriesToSucceed());
      runtime = makeUpMapRuntime(state, locality);

      TaskAttemptInfo tai = new MapTaskAttemptInfo(state, taskInfo, runtime);
      return tai;
    } else if (taskType == TaskType.REDUCE) {
      State state = State.SUCCEEDED;
      long shuffleTime = 0;
      long sortTime = 0;
      long reduceTime = 0;

      // TODO make up state
      // state = makeUpState(taskAttemptNumber, job.getReducerTriesToSucceed());
      reduceTime = makeUpReduceRuntime(state);
      TaskAttemptInfo tai = new ReduceTaskAttemptInfo(state, taskInfo,
          shuffleTime, sortTime, reduceTime);
      return tai;
    }

    throw new IllegalArgumentException("taskType is neither MAP nor REDUCE: "
        + taskType);
  }

  private long makeUpReduceRuntime(State state) {
    long reduceTime = 0;
    for (int i = 0; i < 5; i++) {
      reduceTime = doMakeUpReduceRuntime(state);
      if (reduceTime >= 0) {
        return reduceTime;
      }
    }
    return 0;
    // throw new IllegalStateException(" made-up reduceTime<0: " + reduceTime);
  }

  private long doMakeUpReduceRuntime(State state) {
    long reduceTime;
    try {
      if (state == State.SUCCEEDED) {
        reduceTime = makeUpRuntime(job.getSuccessfulReduceAttemptCDF());
      } else if (state == State.FAILED) {
        reduceTime = makeUpRuntime(job.getFailedReduceAttemptCDF());
      } else {
        throw new IllegalArgumentException(
            "state is neither SUCCEEDED nor FAILED: " + state);
      }
      return reduceTime;
    } catch (IllegalArgumentException e) {
      if (e.getMessage().startsWith("no value to use to make up runtime")) {
        return 0;
      }
      throw e;
    }
  }

  private long makeUpMapRuntime(State state, int locality) {
    long runtime;
    // make up runtime
    if (state == State.SUCCEEDED) {
      // XXX MapCDFs is a ArrayList of 4 possible groups: distance=0, 1, 2, and
      // the last group is "distance cannot be determined". All pig jobs
      // would have only the 4th group, and pig tasks usually do not have
      // any locality, so this group should count as "distance=2".
      // However, setup/cleanup tasks are also counted in the 4th group.
      // These tasks do not make sense.
      try {
        runtime = makeUpRuntime(job.getSuccessfulMapAttemptCDFs().get(locality));
      } catch (IllegalArgumentException e) {
        if (e.getMessage() == "no value to use to make up runtime") {
          runtime = makeUpRuntime(job.getSuccessfulMapAttemptCDFs());
        } else {
          throw e;
        }
      }
    } else if (state == State.FAILED) {
      try {
        runtime = makeUpRuntime(job.getFailedMapAttemptCDFs().get(locality));
      } catch (IllegalArgumentException e) {
        if (e.getMessage() == "no value to use to make up runtime") {
          runtime = makeUpRuntime(job.getFailedMapAttemptCDFs());
        } else {
          throw e;
        }
      }
    } else {
      throw new IllegalArgumentException(
          "state is neither SUCCEEDED nor FAILED: " + state);
    }
    return runtime;
  }

  private long makeUpRuntime(ArrayList<LoggedDiscreteCDF> mapAttemptCDFs) {
    int total = 0;
    for (LoggedDiscreteCDF cdf : mapAttemptCDFs) {
      total += cdf.getNumberValues();
    }
    if (total == 0) {
      // consider no value to use to make up runtime.
      return 0;
      // throw new IllegalStateException("No task attempt statistics");
    }
    int index = random.nextInt(total);
    for (LoggedDiscreteCDF cdf : mapAttemptCDFs) {
      if (index >= cdf.getNumberValues()) {
        index -= cdf.getNumberValues();
      } else {
        if (index < 0) {
          throw new IllegalStateException("application error");
        }
        return makeUpRuntime(cdf);
      }
    }
    throw new IllegalStateException("not possible to get here");
  }

  private long makeUpRuntime(LoggedDiscreteCDF loggedDiscreteCDF) {
    ArrayList<LoggedSingleRelativeRanking> rankings = new ArrayList<LoggedSingleRelativeRanking>(
        loggedDiscreteCDF.getRankings());
    if (loggedDiscreteCDF.getNumberValues() == 0) {
      throw new IllegalArgumentException("no value to use to make up runtime");
    }

    LoggedSingleRelativeRanking ranking = new LoggedSingleRelativeRanking();
    ranking.setDatum(loggedDiscreteCDF.getMaximum());
    ranking.setRelativeRanking(1.0);
    rankings.add(ranking);

    ranking = new LoggedSingleRelativeRanking();
    ranking.setDatum(loggedDiscreteCDF.getMinimum());
    ranking.setRelativeRanking(0.0);
    rankings.add(0, ranking);

    double r = random.nextDouble();
    LoggedSingleRelativeRanking prevRanking = rankings.get(0);
    for (LoggedSingleRelativeRanking ranking2 : rankings) {
      double r2 = ranking2.getRelativeRanking();
      if (r < r2) {
        double r1 = prevRanking.getRelativeRanking();
        double f1 = prevRanking.getDatum();
        double f2 = ranking2.getDatum();
        double runtime = (r - r1) / (r2 - r1) * (f2 - f1) + f1;
        return (long) runtime;
      }
      prevRanking = ranking2;
    }

    return rankings.get(rankings.size() - 1).getDatum();
  }

  private State makeUpState(int taskAttemptNumber, double[] numAttempts) {
    if (taskAttemptNumber >= numAttempts.length - 1) {
      // always succeed
      return State.SUCCEEDED;
    } else {
      double pSucceed = numAttempts[taskAttemptNumber];
      double pFail = 0;
      for (int i = taskAttemptNumber + 1; i < numAttempts.length; i++) {
        pFail += numAttempts[i];
      }
      return (random.nextDouble() < pSucceed / (pSucceed + pFail)) ? State.SUCCEEDED
          : State.FAILED;
    }
  }

  private TaskID getMaskedTaskID(TaskType taskType, int taskNumber) {
    return new TaskID(new JobID(), taskType, taskNumber);
  }

  private LoggedTask getLoggedTask(TaskType taskType, int taskNumber) {
    if (loggedTaskMap.isEmpty()) {
      buildMaps();
    }
    return loggedTaskMap.get(getMaskedTaskID(taskType, taskNumber));
  }

  private LoggedTaskAttempt getLoggedTaskAttempt(TaskType taskType,
      int taskNumber, int taskAttemptNumber) {
    TaskAttemptID id = new TaskAttemptID(getMaskedTaskID(taskType, taskNumber),
        taskAttemptNumber);
    if (loggedTaskAttemptMap.isEmpty()) {
      buildMaps();
    }
    return loggedTaskAttemptMap.get(id);
  }
}
