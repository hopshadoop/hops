/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.conf.Configuration;
import static org.junit.Assert.*;
import java.util.*;
import java.io.IOException;

import org.apache.hadoop.security.SecurityUtil.AccessControlList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class CapacityTestUtils {
  static final Log LOG =
    LogFactory.getLog(org.apache.hadoop.mapred.CapacityTestUtils.class);


  /**
   * Test class that removes the asynchronous nature of job initialization.
   * <p/>
   * The run method is a dummy which just waits for completion. It is
   * expected that test code calls the main method, initializeJobs, directly
   * to trigger initialization.
   */
  static class ControlledJobInitializer extends
                              JobInitializationPoller.JobInitializationThread {

    boolean stopRunning;

    public ControlledJobInitializer(JobInitializationPoller p) {
      p.super();
    }

    @Override
    public void run() {
      while (!stopRunning) {
        try {
          synchronized (this) {
            this.wait();
          }
        } catch (InterruptedException ie) {
          break;
        }
      }
    }

    void stopRunning() {
      stopRunning = true;
    }
  }


  /**
   * Test class that removes the asynchronous nature of job initialization.
   * <p/>
   * The run method is a dummy which just waits for completion. It is
   * expected that test code calls the main method, selectJobsToInitialize,
   * directly to trigger initialization.
   * <p/>
   * The class also creates the test worker thread objects of type
   * ControlledJobInitializer instead of the objects of the actual class
   */
  static class ControlledInitializationPoller extends JobInitializationPoller {

    private boolean stopRunning;
    private ArrayList<ControlledJobInitializer> workers;

    public ControlledInitializationPoller(
      JobQueuesManager mgr,
      CapacitySchedulerConf rmConf,
      Set<String> queues,
      TaskTrackerManager ttm) {
      super(mgr, ttm);
    }

    @Override
    public void run() {
      // don't do anything here.
      while (!stopRunning) {
        try {
          synchronized (this) {
            this.wait();
          }
        } catch (InterruptedException ie) {
          break;
        }
      }
    }

    @Override
    JobInitializationThread createJobInitializationThread() {
      ControlledJobInitializer t = new ControlledJobInitializer(this);
      if (workers == null) {
        workers = new ArrayList<ControlledJobInitializer>();
      }
      workers.add(t);
      return t;
    }

    @Override
    void selectJobsToInitialize() {
      super.cleanUpInitializedJobsList();
      super.selectJobsToInitialize();
      for (ControlledJobInitializer t : workers) {
        t.initializeJobs();
      }
    }

    void stopRunning() {
      stopRunning = true;
      for (ControlledJobInitializer t : workers) {
        t.stopRunning();
        t.interrupt();
      }
    }
  }

  static class FakeClock extends CapacityTaskScheduler.Clock {
    private long time = 0;

    public void advance(long millis) {
      time += millis;
    }

    @Override
    long getTime() {
      return time;
    }
  }


  static Task checkAssignment(
    CapacityTestUtils.FakeTaskTrackerManager taskTrackerManager,
    CapacityTaskScheduler scheduler, String taskTrackerName,
    String expectedTaskString) throws IOException {
    List<Task> tasks = scheduler.assignTasks(
      taskTrackerManager.getTaskTracker(
        taskTrackerName));
    assertNotNull(expectedTaskString, tasks);
    assertEquals(expectedTaskString, 1, tasks.size());
    assertEquals(expectedTaskString, tasks.get(0).toString());
    return tasks.get(0);
  }

  static void verifyCapacity(
    FakeTaskTrackerManager taskTrackerManager,
    String expectedCapacity,
    String queue) throws IOException {
    String schedInfo = taskTrackerManager.getQueueManager().
      getSchedulerInfo(queue).toString();
    assertTrue(
      schedInfo.contains(
        "Map tasks\nCapacity: "
          + expectedCapacity + " slots"));
  }

  /*
  * Fake job in progress object used for testing the schedulers scheduling
  * decisions. The JobInProgress objects returns out FakeTaskInProgress
  * objects when assignTasks is called. If speculative maps and reduces
  * are configured then JobInProgress returns exactly one Speculative
  * map and reduce task.
  */
  static class FakeJobInProgress extends JobInProgress {

    protected FakeTaskTrackerManager taskTrackerManager;
    private int mapTaskCtr;
    private int redTaskCtr;
    private Set<TaskInProgress> mapTips =
      new HashSet<TaskInProgress>();
    private Set<TaskInProgress> reduceTips =
      new HashSet<TaskInProgress>();
    private int speculativeMapTaskCounter = 0;
    private int speculativeReduceTaskCounter = 0;

    public FakeJobInProgress(
      JobID jId, JobConf jobConf,
      FakeTaskTrackerManager taskTrackerManager, String user) {
      super(jId, jobConf, null);
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.status = new JobStatus(
        jId, 0f, 0f, JobStatus.PREP,
        jobConf.getUser(),
        jobConf.getJobName(), "", "");
      this.status.setJobPriority(JobPriority.NORMAL);
      this.status.setStartTime(startTime);
      if (null == jobConf.getQueueName()) {
        this.profile = new JobProfile(
          user, jId,
          null, null, null);
      } else {
        this.profile = new JobProfile(
          user, jId,
          null, null, null, jobConf.getQueueName());
      }
      mapTaskCtr = 0;
      redTaskCtr = 0;
    }

    @Override
    public synchronized void initTasks() throws IOException {
      getStatus().setRunState(JobStatus.RUNNING);
    }

    @Override
    public Task obtainNewMapTask(
      final TaskTrackerStatus tts, int clusterSize,
      int ignored) throws IOException {
      boolean areAllMapsRunning = (mapTaskCtr == numMapTasks);
      if (areAllMapsRunning) {
        if (!getJobConf().getMapSpeculativeExecution() ||
          speculativeMapTasks > 0) {
          return null;
        }
      }
      TaskAttemptID attemptId = getTaskAttemptID(true, areAllMapsRunning);
      Task task = new MapTask(
        "", attemptId, 0, "", new BytesWritable(),
        super.numSlotsPerMap) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningMapTasks++;
      // create a fake TIP and keep track of it
      FakeTaskInProgress mapTip = new FakeTaskInProgress(
        getJobID(),
        getJobConf(), task, true, this);
      mapTip.taskStatus.setRunState(TaskStatus.State.RUNNING);
      if (areAllMapsRunning) {
        speculativeMapTasks++;
        //you have scheduled a speculative map. Now set all tips in the
        //map tips not to have speculative task.
        for (TaskInProgress t : mapTips) {
          if (t instanceof FakeTaskInProgress) {
            FakeTaskInProgress mt = (FakeTaskInProgress) t;
            mt.hasSpeculativeMap = false;
          }
        }
      } else {
        //add only non-speculative tips.
        mapTips.add(mapTip);
        //add the tips to the JobInProgress TIPS
        maps = mapTips.toArray(new TaskInProgress[mapTips.size()]);
      }
      return task;
    }

    @Override
    public Task obtainNewReduceTask(
      final TaskTrackerStatus tts,
      int clusterSize, int ignored) throws IOException {
      boolean areAllReducesRunning = (redTaskCtr == numReduceTasks);
      if (areAllReducesRunning) {
        if (!getJobConf().getReduceSpeculativeExecution() ||
          speculativeReduceTasks > 0) {
          return null;
        }
      }
      TaskAttemptID attemptId = getTaskAttemptID(false, areAllReducesRunning);
      Task task = new ReduceTask(
        "", attemptId, 0, 10,
        super.numSlotsPerReduce) {
        @Override
        public String toString() {
          return String.format("%s on %s", getTaskID(), tts.getTrackerName());
        }
      };
      taskTrackerManager.startTask(tts.getTrackerName(), task);
      runningReduceTasks++;
      // create a fake TIP and keep track of it
      FakeTaskInProgress reduceTip = new FakeTaskInProgress(
        getJobID(),
        getJobConf(), task, false, this);
      reduceTip.taskStatus.setRunState(TaskStatus.State.RUNNING);
      if (areAllReducesRunning) {
        speculativeReduceTasks++;
        //you have scheduled a speculative map. Now set all tips in the
        //map tips not to have speculative task.
        for (TaskInProgress t : reduceTips) {
          if (t instanceof FakeTaskInProgress) {
            FakeTaskInProgress rt = (FakeTaskInProgress) t;
            rt.hasSpeculativeReduce = false;
          }
        }
      } else {
        //add only non-speculative tips.
        reduceTips.add(reduceTip);
        //add the tips to the JobInProgress TIPS
        reduces = reduceTips.toArray(new TaskInProgress[reduceTips.size()]);
      }
      return task;
    }

    public void mapTaskFinished() {
      runningMapTasks--;
      finishedMapTasks++;
    }

    public void reduceTaskFinished() {
      runningReduceTasks--;
      finishedReduceTasks++;
    }

    private TaskAttemptID getTaskAttemptID(
      boolean isMap, boolean isSpeculative) {
      JobID jobId = getJobID();
      TaskType t = TaskType.REDUCE;
      if (isMap) {
        t = TaskType.MAP;
      }
      if (!isSpeculative) {
        return new TaskAttemptID(
          jobId.getJtIdentifier(), jobId.getId(), t,
          (isMap) ? ++mapTaskCtr : ++redTaskCtr, 0);
      } else {
        return new TaskAttemptID(
          jobId.getJtIdentifier(), jobId.getId(), t,
          (isMap) ? mapTaskCtr : redTaskCtr, 1);
      }
    }

    @Override
    Set<TaskInProgress> getNonLocalRunningMaps() {
      return (Set<TaskInProgress>) mapTips;
    }

    @Override
    Set<TaskInProgress> getRunningReduces() {
      return (Set<TaskInProgress>) reduceTips;
    }

  }

  static class FakeFailingJobInProgress extends FakeJobInProgress {

    public FakeFailingJobInProgress(
      JobID id, JobConf jobConf,
      FakeTaskTrackerManager taskTrackerManager, String user) {
      super(id, jobConf, taskTrackerManager, user);
    }

    @Override
    public synchronized void initTasks() throws IOException {
      throw new IOException("Failed Initalization");
    }

    @Override
    synchronized void fail() {
      this.status.setRunState(JobStatus.FAILED);
    }
  }

  static class FakeTaskInProgress extends TaskInProgress {
    private boolean isMap;
    private FakeJobInProgress fakeJob;
    private TreeMap<TaskAttemptID, String> activeTasks;
    private TaskStatus taskStatus;
    boolean hasSpeculativeMap;
    boolean hasSpeculativeReduce;

    FakeTaskInProgress(
      JobID jId, JobConf jobConf, Task t,
      boolean isMap, FakeJobInProgress job) {
      super(jId, "", new JobClient.RawSplit(), null, jobConf, job, 0, 1);
      this.isMap = isMap;
      this.fakeJob = job;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      activeTasks.put(t.getTaskID(), "tt");
      // create a fake status for a task that is running for a bit
      this.taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setProgress(0.5f);
      taskStatus.setRunState(TaskStatus.State.RUNNING);
      if (jobConf.getMapSpeculativeExecution()) {
        //resetting of the hasSpeculativeMap is done
        //when speculative map is scheduled by the job.
        hasSpeculativeMap = true;
      }
      if (jobConf.getReduceSpeculativeExecution()) {
        //resetting of the hasSpeculativeReduce is done
        //when speculative reduce is scheduled by the job.
        hasSpeculativeReduce = true;
      }
    }

    @Override
    TreeMap<TaskAttemptID, String> getActiveTasks() {
      return activeTasks;
    }

    @Override
    public TaskStatus getTaskStatus(TaskAttemptID taskid) {
      // return a status for a task that has run a bit
      return taskStatus;
    }

    @Override
    boolean killTask(TaskAttemptID taskId, boolean shouldFail) {
      if (isMap) {
        fakeJob.mapTaskFinished();
      } else {
        fakeJob.reduceTaskFinished();
      }
      return true;
    }

    @Override
      /*
      *hasSpeculativeMap and hasSpeculativeReduce is reset by FakeJobInProgress
      *after the speculative tip has been scheduled.
      */
    boolean canBeSpeculated(long currentTime) {
      if (isMap && hasSpeculativeMap) {
        return fakeJob.getJobConf().getMapSpeculativeExecution();
      }
      if (!isMap && hasSpeculativeReduce) {
        return fakeJob.getJobConf().getReduceSpeculativeExecution();
      }
      return false;
    }

    @Override
    public boolean isRunning() {
      return !activeTasks.isEmpty();
    }

  }

  static class FakeQueueManager extends QueueManager {
    private Set<String> queueNames = null;
    private static final AccessControlList allEnabledAcl =
      new AccessControlList("*");

    FakeQueueManager(Configuration conf) {
      super(conf);
    }

    void setQueues(Set<String> queueNames) {
      this.queueNames = queueNames;

      // sync up queues with the parent class.
      Queue[] queues = new Queue[queueNames.size()];
      int i = 0;
      for (String queueName : queueNames) {
        HashMap<String, AccessControlList> aclsMap
          = new HashMap<String, AccessControlList>();
        for (Queue.QueueOperation oper : Queue.QueueOperation.values()) {
          String key = QueueManager.toFullPropertyName(
            queueName,
            oper.getAclName());
          aclsMap.put(key, allEnabledAcl);
        }
        queues[i++] = new Queue(queueName, aclsMap, Queue.QueueState.RUNNING);
      }
      super.setQueues(queues);
    }

    public synchronized Set<String> getLeafQueueNames() {
      return queueNames;
    }
  }

  static class FakeTaskTrackerManager implements TaskTrackerManager {
    int maps = 0;
    int reduces = 0;
    int maxMapTasksPerTracker = 2;
    int maxReduceTasksPerTracker = 1;
    long ttExpiryInterval = 10 * 60 * 1000L; // default interval
    List<JobInProgressListener> listeners =
      new ArrayList<JobInProgressListener>();

    FakeQueueManager qm = null;

    private Map<String, TaskTracker> trackers =
      new HashMap<String, TaskTracker>();
    private Map<String, TaskStatus> taskStatuses =
      new HashMap<String, TaskStatus>();
    private Map<JobID, JobInProgress> jobs =
      new HashMap<JobID, JobInProgress>();

    public FakeTaskTrackerManager() {
      this(2, 2, 1);
    }

    public FakeTaskTrackerManager(
      int numTaskTrackers,
      int maxMapTasksPerTracker, int maxReduceTasksPerTracker) {
    Configuration cfn = new Configuration();
    cfn.set("mapred.queue.names","default");
    qm = new FakeQueueManager(cfn);
      this.maxMapTasksPerTracker = maxMapTasksPerTracker;
      this.maxReduceTasksPerTracker = maxReduceTasksPerTracker;
      for (int i = 1; i < numTaskTrackers + 1; i++) {
        String ttName = "tt" + i;
        TaskTracker tt = new TaskTracker(ttName);
        tt.setStatus(
          new TaskTrackerStatus(
            ttName, ttName + ".host", i,
            new ArrayList<TaskStatus>(), 0,
            maxMapTasksPerTracker,
            maxReduceTasksPerTracker));
        trackers.put(ttName, tt);
      }
    }

    public void addTaskTracker(String ttName) {
      TaskTracker tt = new TaskTracker(ttName);
      tt.setStatus(
        new TaskTrackerStatus(
          ttName, ttName + ".host", 1,
          new ArrayList<TaskStatus>(), 0,
          maxMapTasksPerTracker,
          maxReduceTasksPerTracker));
      trackers.put(ttName, tt);
    }

    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();
      return new ClusterStatus(
        numTrackers, 0,
        ttExpiryInterval, maps, reduces,
        numTrackers * maxMapTasksPerTracker,
        numTrackers * maxReduceTasksPerTracker,
        JobTracker.State.RUNNING);
    }

    public int getNumberOfUniqueHosts() {
      return 0;
    }

    public int getNextHeartbeatInterval() {
      return MRConstants.HEARTBEAT_INTERVAL_MIN;
    }

    @Override
    public void killJob(JobID jobid) throws IOException {
      JobInProgress job = jobs.get(jobid);
      finalizeJob(job, JobStatus.KILLED);
      job.kill();
    }

    public void initJob(JobInProgress jip) {
      try {
        JobStatus oldStatus = (JobStatus) jip.getStatus().clone();
        jip.initTasks();
        if (jip.isJobEmpty()) {
          completeEmptyJob(jip);
        } else if (!jip.isSetupCleanupRequired()) {
          jip.completeSetup();
        }
        JobStatus newStatus = (JobStatus) jip.getStatus().clone();
        JobStatusChangeEvent event = new JobStatusChangeEvent(
          jip,
          JobStatusChangeEvent.EventType.RUN_STATE_CHANGED, oldStatus,
          newStatus);
        for (JobInProgressListener listener : listeners) {
          listener.jobUpdated(event);
        }
      } catch (Exception ioe) {
        failJob(jip);
      }
    }

    private synchronized void completeEmptyJob(JobInProgress jip) {
      jip.completeEmptyJob();
    }

    public synchronized void failJob(JobInProgress jip) {
      JobStatus oldStatus = (JobStatus) jip.getStatus().clone();
      jip.fail();
      JobStatus newStatus = (JobStatus) jip.getStatus().clone();
      JobStatusChangeEvent event = new JobStatusChangeEvent(
        jip,
        JobStatusChangeEvent.EventType.RUN_STATE_CHANGED, oldStatus, newStatus);
      for (JobInProgressListener listener : listeners) {
        listener.jobUpdated(event);
      }
    }

    public void removeJob(JobID jobid) {
      jobs.remove(jobid);
    }

    @Override
    public JobInProgress getJob(JobID jobid) {
      return jobs.get(jobid);
    }

    Collection<JobInProgress> getJobs() {
      return jobs.values();
    }

    public Collection<TaskTrackerStatus> taskTrackers() {
      List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
      for (TaskTracker tt : trackers.values()) {
        statuses.add(tt.getStatus());
      }
      return statuses;
    }


    public void addJobInProgressListener(JobInProgressListener listener) {
      listeners.add(listener);
    }

    public void removeJobInProgressListener(JobInProgressListener listener) {
      listeners.remove(listener);
    }

    public void submitJob(JobInProgress job) throws IOException {
      jobs.put(job.getJobID(), job);
      for (JobInProgressListener listener : listeners) {
        listener.jobAdded(job);
      }
    }

    public TaskTracker getTaskTracker(String trackerID) {
      return trackers.get(trackerID);
    }

    public void startTask(String taskTrackerName, final Task t) {
      if (t.isMapTask()) {
        maps++;
      } else {
        reduces++;
      }
      TaskStatus status = new TaskStatus() {
        @Override
        public TaskAttemptID getTaskID() {
          return t.getTaskID();
        }

        @Override
        public boolean getIsMap() {
          return t.isMapTask();
        }

        @Override
        public int getNumSlots() {
          return t.getNumSlotsRequired();
        }

        @Override
        public void addFetchFailedMap(TaskAttemptID mapTaskId) {
          
        }
      };
      taskStatuses.put(t.getTaskID().toString(), status);
      status.setRunState(TaskStatus.State.RUNNING);
      trackers.get(taskTrackerName).getStatus().getTaskReports().add(status);
    }

    public void finishTask(
      String tipId,
      FakeJobInProgress j) {
      TaskStatus status = taskStatuses.get(tipId);
      if (status.getIsMap()) {
        maps--;
        j.mapTaskFinished();
      } else {
        reduces--;
        j.reduceTaskFinished();
      }
      status.setRunState(TaskStatus.State.SUCCEEDED);
    }

    void finalizeJob(FakeJobInProgress fjob) {
      finalizeJob(fjob, JobStatus.SUCCEEDED);
    }

    void finalizeJob(JobInProgress fjob, int state) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus) fjob.getStatus().clone();
      fjob.getStatus().setRunState(state);
      JobStatus newStatus = (JobStatus) fjob.getStatus().clone();
      JobStatusChangeEvent event =
        new JobStatusChangeEvent(
          fjob, JobStatusChangeEvent.EventType.RUN_STATE_CHANGED, oldStatus,
          newStatus);
      for (JobInProgressListener listener : listeners) {
        listener.jobUpdated(event);
      }
    }

    public void setPriority(FakeJobInProgress fjob, JobPriority priority) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus) fjob.getStatus().clone();
      fjob.setPriority(priority);
      JobStatus newStatus = (JobStatus) fjob.getStatus().clone();
      JobStatusChangeEvent event =
        new JobStatusChangeEvent(
          fjob, JobStatusChangeEvent.EventType.PRIORITY_CHANGED, oldStatus,
          newStatus);
      for (JobInProgressListener listener : listeners) {
        listener.jobUpdated(event);
      }
    }

    public void setStartTime(FakeJobInProgress fjob, long start) {
      // take a snapshot of the status before changing it
      JobStatus oldStatus = (JobStatus) fjob.getStatus().clone();

      fjob.startTime = start; // change the start time of the job
      fjob.status.setStartTime(start); // change the start time of the jobstatus

      JobStatus newStatus = (JobStatus) fjob.getStatus().clone();

      JobStatusChangeEvent event =
        new JobStatusChangeEvent(
          fjob, JobStatusChangeEvent.EventType.START_TIME_CHANGED, oldStatus,
          newStatus);
      for (JobInProgressListener listener : listeners) {
        listener.jobUpdated(event);
      }
    }

    void addQueues(String[] arr) {
      Set<String> queues = new HashSet<String>();
      for (String s : arr) {

        queues.add(s);
      }
      qm.setQueues(queues);
    }

    public QueueManager getQueueManager() {
      return qm;
    }

    @Override
    public boolean killTask(TaskAttemptID taskid, boolean shouldFail) {
      return true;
    }
  }// represents a fake queue configuration info

  static class FakeQueueInfo {
    String queueName;
    float capacity;
    boolean supportsPrio;
    int ulMin;

    public FakeQueueInfo(
      String queueName, float capacity, boolean supportsPrio, int ulMin) {
      this.queueName = queueName;
      this.capacity = capacity;
      this.supportsPrio = supportsPrio;
      this.ulMin = ulMin;
    }
  }

  static class FakeResourceManagerConf extends CapacitySchedulerConf {

    // map of queue names to queue info
    private Map<String, FakeQueueInfo> queueMap =
      new LinkedHashMap<String, FakeQueueInfo>();
    String firstQueue;


    void setFakeQueues(List<FakeQueueInfo> queues, QueueManager qManager) {
      Properties p = new Properties();
      for (FakeQueueInfo q : queues) {
        p.setProperty("capacity",q.capacity+"");
        p.setProperty("supports-priority",q.supportsPrio+"");
        p.setProperty("minimum-user-limit-percent",q.ulMin+"");
        qManager.getQueue(q.queueName).setProperties(p);
        queueMap.put(q.queueName, q);
      }
      firstQueue = new String(queues.get(0).queueName);
    }

    public synchronized Set<String> getQueues() {
      return queueMap.keySet();
    }

    /*public synchronized String getFirstQueue() {
      return firstQueue;
    }*/

    public float getCapacity(String queue) {
      if (queueMap.get(queue).capacity == -1) {
        return -1;
      }
      return queueMap.get(queue).capacity;
    }

    public float getMaxCapacity(String queue) {
      //There is no support for the old testcase for
      //maxCapacity.

      //MaxCapacity testcases are part of TestContainerQueue
      return -1;
    }

    public int getMaxMapCap(String queue) {
      return -1;
    }

    public int getMaxReduceCap(String queue) {
      return -1;
    }


    public int getMinimumUserLimitPercent(String queue) {
      return queueMap.get(queue).ulMin;
    }

    /**
     * Gets the maximum number of jobs which are allowed to initialize in the
     * job queue.
     *
     * @param queue queue name.
     * @return maximum number of jobs allowed to be initialized per user.
     * @throws IllegalArgumentException if maximum number of users is negative
     *                                  or zero.
     */
    @Override
    public int getMaxJobsPerUserToInitialize(String queue) {
      return 2;
    }

    public boolean isPrioritySupported(String queue) {
      return queueMap.get(queue).supportsPrio;
    }

    @Override
    public long getSleepInterval() {
      return 1;
    }

    @Override
    public int getMaxWorkerThreads() {
      return 1;
    }
  }
}
