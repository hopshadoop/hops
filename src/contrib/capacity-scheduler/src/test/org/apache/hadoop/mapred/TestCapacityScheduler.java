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

package org.apache.hadoop.mapred;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import static org.apache.hadoop.mapred.CapacityTestUtils.*;

import java.io.IOException;
import java.util.*;

public class TestCapacityScheduler extends TestCase {

  static final Log LOG =
    LogFactory.getLog(org.apache.hadoop.mapred.TestCapacityScheduler.class);

  private static int jobCounter;

  private ControlledInitializationPoller controlledInitializationPoller;


  protected JobConf conf;
  protected CapacityTaskScheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private FakeResourceManagerConf resConf;
  private FakeClock clock;

  @Override
  protected void setUp() {
    setUp(2, 2, 1);
  }

  private void setUp(
    int numTaskTrackers, int numMapTasksPerTracker,
    int numReduceTasksPerTracker) {
    jobCounter = 0;
    taskTrackerManager =
      new FakeTaskTrackerManager(
        numTaskTrackers, numMapTasksPerTracker,
        numReduceTasksPerTracker);
    clock = new FakeClock();
    scheduler = new CapacityTaskScheduler(clock);
    scheduler.setTaskTrackerManager(taskTrackerManager);

    conf = new JobConf();
    // Don't let the JobInitializationPoller come in our way.
    conf.set("mapred.queue.names","default");
    resConf = new FakeResourceManagerConf();
    controlledInitializationPoller = new ControlledInitializationPoller(
      scheduler.jobQueuesManager,
      resConf,
      resConf.getQueues(), taskTrackerManager);
    scheduler.setInitializationPoller(controlledInitializationPoller);
    scheduler.setConf(conf);
    //by default disable speculative execution.
    conf.setMapSpeculativeExecution(false);
    conf.setReduceSpeculativeExecution(false);
  }

  @Override
  protected void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.terminate();
    }
  }

  private FakeJobInProgress submitJob(int state, JobConf jobConf)
    throws IOException {
    FakeJobInProgress job =
      new FakeJobInProgress(
        new JobID("test", ++jobCounter),
        (jobConf == null ? new JobConf(conf) : jobConf), taskTrackerManager,
        jobConf.getUser());
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    return job;
  }

  private FakeJobInProgress submitJobAndInit(int state, JobConf jobConf)
    throws IOException {
    FakeJobInProgress j = submitJob(state, jobConf);
    taskTrackerManager.initJob(j);
    return j;
  }

  private FakeJobInProgress submitJob(
    int state, int maps, int reduces,
    String queue, String user) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    if (queue != null) {
      jobConf.setQueueName(queue);
    }
    jobConf.setUser(user);
    return submitJob(state, jobConf);
  }

  // Submit a job and update the listeners
  private FakeJobInProgress submitJobAndInit(
    int state, int maps, int reduces,
    String queue, String user)
    throws IOException {
    FakeJobInProgress j = submitJob(state, maps, reduces, queue, user);
    taskTrackerManager.initJob(j);
    return j;
  }

  /**
   * Test the max map limit.
   *
   * @throws IOException
   */
  public void testMaxMapCap() throws IOException {
    this.setUp(4, 1, 1);
    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, false, 1));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext().getMapTSC().setMaxTaskLimit(2);
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext().getReduceTSC().setMaxTaskLimit(-1);


    //submit the Job
    FakeJobInProgress fjob1 =
      submitJob(JobStatus.PREP, 3, 1, "default", "user");

    taskTrackerManager.initJob(fjob1);

    List<Task> task1 = scheduler.assignTasks(tracker("tt1"));
    List<Task> task2 = scheduler.assignTasks(tracker("tt2"));

    //Once the 2 tasks are running the third assigment should be reduce.
    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0001_r_000001_0 on tt3");
    //This should fail.
    List<Task> task4 = scheduler.assignTasks(tracker("tt4"));
    assertNull(task4);
    //Now complete the task 1.
    // complete the job
    taskTrackerManager.finishTask(
      task1.get(0).getTaskID().toString(),
      fjob1);
    //We have completed the tt1 task which was a map task so we expect one map
    //task to be picked up
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0001_m_000003_0 on tt4");
  }

  /**
   * Test max reduce limit
   *
   * @throws IOException
   */
  public void testMaxReduceCap() throws IOException {
    this.setUp(4, 1, 1);
    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, false, 1));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext().getMapTSC().setMaxTaskLimit(-1);
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext().getReduceTSC().setMaxTaskLimit(2);


    //submit the Job
    FakeJobInProgress fjob1 =
      submitJob(JobStatus.PREP, 1, 3, "default", "user");

    taskTrackerManager.initJob(fjob1);

    List<Task> task1 = scheduler.assignTasks(tracker("tt1"));
    List<Task> task2 = scheduler.assignTasks(tracker("tt2"));
    List<Task> task3 = scheduler.assignTasks(tracker("tt3"));

    //This should fail. 1 map, 2 reduces , we have reached the limit.
    List<Task> task4 = scheduler.assignTasks(tracker("tt4"));
    assertNull(task4);
    //Now complete the task 1 i.e map task.
    // complete the job
    taskTrackerManager.finishTask(
      task1.get(0).getTaskID().toString(),
      fjob1);

    //This should still fail as only map task is done
    task4 = scheduler.assignTasks(tracker("tt4"));
    assertNull(task4);

    //Complete the reduce task
    taskTrackerManager.finishTask(
      task2.get(0).getTaskID().toString(), fjob1);

    //One reduce is done hence assign the new reduce.
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0001_r_000003_0 on tt4");
  }

  // test job run-state change
  public void testJobRunStateChange() throws IOException {
    // start the scheduler
    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 1));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit the job
    FakeJobInProgress fjob1 =
      submitJob(JobStatus.PREP, 1, 0, "default", "user");

    FakeJobInProgress fjob2 =
      submitJob(JobStatus.PREP, 1, 0, "default", "user");

    // test if changing the job priority/start-time works as expected in the 
    // waiting queue
    testJobOrderChange(fjob1, fjob2, true);

    // Init the jobs
    // simulate the case where the job with a lower priority becomes running 
    // first (may be because of the setup tasks).

    // init the lower ranked job first
    taskTrackerManager.initJob(fjob2);

    // init the higher ordered job later
    taskTrackerManager.initJob(fjob1);

    // check if the jobs are missing from the waiting queue
    // The jobs are not removed from waiting queue until they are scheduled 
    assertEquals(
      "Waiting queue is garbled on job init", 2,
      scheduler.jobQueuesManager.getJobQueue("default").getWaitingJobs()
        .size());

    // test if changing the job priority/start-time works as expected in the 
    // running queue
    testJobOrderChange(fjob1, fjob2, false);

    // schedule a task
    List<Task> tasks = scheduler.assignTasks(tracker("tt1"));

    // complete the job
    taskTrackerManager.finishTask(
      tasks.get(0).getTaskID().toString(),
      fjob1);

    // mark the job as complete
    taskTrackerManager.finalizeJob(fjob1);

    Collection<JobInProgress> rqueue =
      scheduler.jobQueuesManager.getJobQueue("default").getRunningJobs();

    // check if the job is removed from the scheduler
    assertFalse(
      "Scheduler contains completed job",
      rqueue.contains(fjob1));

    // check if the running queue size is correct
    assertEquals(
      "Job finish garbles the queue",
      1, rqueue.size());

  }

  // test if the queue reflects the changes
  private void testJobOrderChange(
    FakeJobInProgress fjob1,
    FakeJobInProgress fjob2,
    boolean waiting) {
    String queueName = waiting ? "waiting" : "running";

    // check if the jobs in the queue are the right order
    JobInProgress[] jobs = getJobsInQueue(waiting);
    assertTrue(
      queueName + " queue doesnt contain job #1 in right order",
      jobs[0].getJobID().equals(fjob1.getJobID()));
    assertTrue(
      queueName + " queue doesnt contain job #2 in right order",
      jobs[1].getJobID().equals(fjob2.getJobID()));

    // I. Check the start-time change
    // Change job2 start-time and check if job2 bumps up in the queue 
    taskTrackerManager.setStartTime(fjob2, fjob1.startTime - 1);

    jobs = getJobsInQueue(waiting);
    assertTrue(
      "Start time change didnt not work as expected for job #2 in "
        + queueName + " queue",
      jobs[0].getJobID().equals(fjob2.getJobID()));
    assertTrue(
      "Start time change didnt not work as expected for job #1 in"
        + queueName + " queue",
      jobs[1].getJobID().equals(fjob1.getJobID()));

    // check if the queue is fine
    assertEquals(
      "Start-time change garbled the " + queueName + " queue",
      2, jobs.length);

    // II. Change job priority change
    // Bump up job1's priority and make sure job1 bumps up in the queue
    taskTrackerManager.setPriority(fjob1, JobPriority.HIGH);

    // Check if the priority changes are reflected
    jobs = getJobsInQueue(waiting);
    assertTrue(
      "Priority change didnt not work as expected for job #1 in "
        + queueName + " queue",
      jobs[0].getJobID().equals(fjob1.getJobID()));
    assertTrue(
      "Priority change didnt not work as expected for job #2 in "
        + queueName + " queue",
      jobs[1].getJobID().equals(fjob2.getJobID()));

    // check if the queue is fine
    assertEquals(
      "Priority change has garbled the " + queueName + " queue",
      2, jobs.length);

    // reset the queue state back to normal
    taskTrackerManager.setStartTime(fjob1, fjob2.startTime - 1);
    taskTrackerManager.setPriority(fjob1, JobPriority.NORMAL);
  }

  private JobInProgress[] getJobsInQueue(boolean waiting) {
    Collection<JobInProgress> queue =
      waiting
        ? scheduler.jobQueuesManager.getJobQueue("default").getWaitingJobs()
        : scheduler.jobQueuesManager.getJobQueue("default").getRunningJobs();
    return queue.toArray(new JobInProgress[0]);
  }

  // tests if tasks can be assinged when there are multiple jobs from a same
  // user
  public void testJobFinished() throws Exception {
    taskTrackerManager.addQueues(new String[]{"default"});

    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit 2 jobs
    FakeJobInProgress j1 = submitJobAndInit(
      JobStatus.PREP, 3, 0, "default", "u1");
    FakeJobInProgress j2 = submitJobAndInit(
      JobStatus.PREP, 3, 0, "default", "u1");

    // I. Check multiple assignments with running tasks within job
    // ask for a task from first job
    Task t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    //  ask for another task from the first job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");

    // complete tasks
    taskTrackerManager.finishTask("attempt_test_0001_m_000001_0", j1);
    taskTrackerManager.finishTask("attempt_test_0001_m_000002_0", j1);

    // II. Check multiple assignments with running tasks across jobs
    // ask for a task from first job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000003_0 on tt1");

    //  ask for a task from the second job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");

    // complete tasks
    taskTrackerManager.finishTask("attempt_test_0002_m_000001_0", j2);
    taskTrackerManager.finishTask("attempt_test_0001_m_000003_0", j1);

    // III. Check multiple assignments with completed tasks across jobs
    // ask for a task from the second job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000002_0 on tt1");

    // complete task
    taskTrackerManager.finishTask("attempt_test_0002_m_000002_0", j2);

    // IV. Check assignment with completed job
    // finish first job
    scheduler.jobQueuesManager.getJobQueue(j1).jobCompleted(j1);

    // ask for another task from the second job
    // if tasks can be assigned then the structures are properly updated 
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000003_0 on tt1");

    // complete task
    taskTrackerManager.finishTask("attempt_test_0002_m_000003_0", j2);
  }

  // basic tests, should be able to submit to queues
  public void testSubmitToQueues() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job with no queue specified. It should be accepted
    // and given to the default queue. 
    JobInProgress j = submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");

    // when we ask for a task, we should get one, from the job submitted
    Task t;
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    // submit another job, to a different queue
    j = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // now when we get a task, it should be from the second job
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000001_0 on tt2");
  }

  public void testGetJobs() throws Exception {
    // need only one queue
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    HashMap<String, ArrayList<FakeJobInProgress>> subJobsList =
      submitJobs(1, 4, "default");

    JobQueuesManager mgr = scheduler.jobQueuesManager;

    while (mgr.getJobQueue("default").getWaitingJobs().size() < 4) {
      Thread.sleep(1);
    }
    //Raise status change events for jobs submitted.
    raiseStatusChangeEvents(mgr);
    Collection<JobInProgress> jobs = scheduler.getJobs("default");

    assertTrue(
      "Number of jobs returned by scheduler is wrong"
      , jobs.size() == 4);

    assertTrue(
      "Submitted jobs and Returned jobs are not same",
      subJobsList.get("u1").containsAll(jobs));
  }

  //Basic test to test capacity allocation across the queues which have no
  //capacity configured.

  public void testCapacityAllocationToQueues() throws Exception {
    String[] qs = {"default", "qAZ1", "qAZ2", "qAZ3", "qAZ4"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 25.0f, true, 25));
    queues.add(new FakeQueueInfo("qAZ1", -1.0f, true, 25));
    queues.add(new FakeQueueInfo("qAZ2", -1.0f, true, 25));
    queues.add(new FakeQueueInfo("qAZ3", -1.0f, true, 25));
    queues.add(new FakeQueueInfo("qAZ4", -1.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    JobQueuesManager jqm = scheduler.jobQueuesManager;
    assertEquals(18.75f, jqm.getJobQueue("qAZ1").qsc.getCapacityPercent());
    assertEquals(18.75f, jqm.getJobQueue("qAZ2").qsc.getCapacityPercent());
    assertEquals(18.75f, jqm.getJobQueue("qAZ3").qsc.getCapacityPercent());
    assertEquals(18.75f, jqm.getJobQueue("qAZ4").qsc.getCapacityPercent());
  }

  // Tests how capacity is computed and assignment of tasks done
  // on the basis of the capacity.
  public void testCapacityBasedAllocation() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    // set the capacity % as 10%, so that capacity will be zero initially as 
    // the cluster capacity increase slowly.
    queues.add(new FakeQueueInfo("default", 10.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 90.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job to the default queue
    submitJobAndInit(JobStatus.PREP, 10, 0, "default", "u1");

    // submit a job to the second queue
    submitJobAndInit(JobStatus.PREP, 10, 0, "q2", "u1");

    // job from q2 runs first because it has some non-zero capacity.
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    verifyCapacity(taskTrackerManager, "0", "default");
    verifyCapacity(taskTrackerManager, "3", "q2");

    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt3");
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000002_0 on tt2");
    verifyCapacity(taskTrackerManager, "0", "default");
    verifyCapacity(taskTrackerManager, "5", "q2");

    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt4");
    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0002_m_000003_0 on tt3");
    verifyCapacity(taskTrackerManager, "0", "default");
    verifyCapacity(taskTrackerManager, "7", "q2");

    // add another tt to increase tt slots
    taskTrackerManager.addTaskTracker("tt5");
    // now job from default should run, as it is furthest away
    // in terms of runningMaps / capacity.
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0001_m_000001_0 on tt4");
    verifyCapacity(taskTrackerManager, "1", "default");
    verifyCapacity(taskTrackerManager, "9", "q2");
  }

  // test capacity transfer
  public void testCapacityTransfer() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task 
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    // I should get another map task. 
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");
    // Now we're at full capacity for maps. If I ask for another map task,
    // I should get a map task from the default queue's capacity. 
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000003_0 on tt2");
    // and another
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000004_0 on tt2");
  }

  /**
   * Creates a queue with max task limit of 2
   * submit 1 job in the queue which is high ram(2 slots) . As 2 slots are
   * given to high ram job and are reserved , no other tasks are accepted .
   *
   * @throws IOException
   */
  public void testHighMemoryBlockingWithMaxLimit()
    throws IOException {

    // 2 map and 1 reduce slots
    taskTrackerManager = new FakeTaskTrackerManager(2, 2, 1);

    taskTrackerManager.addQueues(new String[]{"defaultXYZM"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("defaultXYZM", 100.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
      2 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
      1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext()
      .getMapTSC().setMaxTaskLimit(2);


    // The situation :  Submit 2 jobs with high memory map task
    //Set the max limit for queue to 2 ,
    // try submitting more map tasks to the queue , it should not happen

    LOG.debug(
      "Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("defaultXYZM");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug(
      "Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("defaultXYZM");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // first, a map from j1 will run this is a high memory job so it would
    // occupy the 2 slots
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");

    checkOccupiedSlots("defaultXYZM", TaskType.MAP, 1, 2, 100.0f, 1, 1);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);

    // at this point, the scheduler tries to schedule another map from j1.
    // there isn't enough space. The second job's reduce should be scheduled.
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000001_0 on tt1");

    checkOccupiedSlots("defaultXYZM", TaskType.MAP, 1, 2, 100.0f, 1, 1);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L);

    //at this point , the scheduler tries to schedule another map from j2 for
    //another task tracker.
    // This should not happen as all the map slots are taken
    //by the first task itself.hence reduce task from the second job is given

    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_r_000002_0 on tt2");
  }

  /**
   * test if user limits automatically adjust to max map or reduce limit
   */
  public void testUserLimitsWithMaxLimits() throws Exception {
    setUp(4, 4, 4);
    // set up some queues
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 50));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext().getMapTSC().setMaxTaskLimit(2);
    scheduler.getRoot().getChildren().get(0).getQueueSchedulingContext().getReduceTSC().setMaxTaskLimit(2);


    // submit a job
    FakeJobInProgress fjob1 =
      submitJobAndInit(JobStatus.PREP, 10, 10, "default", "u1");
    FakeJobInProgress fjob2 =
      submitJobAndInit(JobStatus.PREP, 10, 10, "default", "u2");

    // for queue 'default', the capacity for maps is 2.
    // But the max map limit is 2
    // hence user should be getting not more than 1 as it is the 50%.
    Task t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");

    //Now we should get the task from the other job. As the
    //first user has reached his max map limit.

    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000001_0 on tt2");

    //Now we are done with map limit , now if we ask for task we should
    // get reduce from 1st job
    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0001_r_000001_0 on tt3");
    // Now we're at full capacity for maps. 1 done with reduces for job 1 so
    // now we should get 1 reduces for job 2
    Task t4 = checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0002_r_000001_0 on tt4");

    taskTrackerManager.finishTask(
      t1.getTaskID().toString(),
      fjob1);

    //tt1 completed the task so we have 1 map slot for u1
    // we are assigning the 2nd map task from fjob1
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");

    taskTrackerManager.finishTask(
      t4.getTaskID().toString(),
      fjob2);
    //tt4 completed the task , so we have 1 reduce slot for u2
    //we are assigning the 2nd reduce from fjob2
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0002_r_000002_0 on tt4");

  }


  // test user limits
  public void testUserLimits() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task 
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    // Submit another job, from a different user
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    // Now if I ask for a map task, it should come from the second job 
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    // Now we're at full capacity for maps. If I ask for another map task,
    // I should get a map task from the default queue's capacity. 
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000002_0 on tt2");
    // and another
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000002_0 on tt2");
  }

  // test user limits when a 2nd job is submitted much after first job 
  public void testUserLimits2() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task 
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    // since we're the only job, we get another map
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");
    // Submit another job, from a different user
    submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    // Now if I ask for a map task, it should come from the second job 
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000001_0 on tt2");
    // and another
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000002_0 on tt2");
  }

  // test user limits when a 2nd job is submitted much after first job 
  // and we need to wait for first job's task to complete
  public void testUserLimits3() throws Exception {
    // set up some queues
    String[] qs = {"default", "q2"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a job  
    FakeJobInProgress j1 = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u1");
    // for queue 'q2', the capacity for maps is 2. Since we're the only user,
    // we should get a task 
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    // since we're the only job, we get another map
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");
    // we get two more maps from 'default queue'
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000004_0 on tt2");
    // Submit another job, from a different user
    FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP, 10, 10, "q2", "u2");
    // one of the task finishes
    taskTrackerManager.finishTask("attempt_test_0001_m_000001_0", j1);
    // Now if I ask for a map task, it should come from the second job 
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    // another task from job1 finishes, another new task to job2
    taskTrackerManager.finishTask("attempt_test_0001_m_000002_0", j1);
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000002_0 on tt1");
    // now we have equal number of tasks from each job. Whichever job's
    // task finishes, that job gets a new task
    taskTrackerManager.finishTask("attempt_test_0001_m_000003_0", j1);
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000005_0 on tt2");
    taskTrackerManager.finishTask("attempt_test_0002_m_000001_0", j2);
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000003_0 on tt1");
  }

  // test user limits with many users, more slots
  public void testUserLimits4() throws Exception {
    // set up one queue, with 10 slots
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();
    // add some more TTs 
    taskTrackerManager.addTaskTracker("tt3");
    taskTrackerManager.addTaskTracker("tt4");
    taskTrackerManager.addTaskTracker("tt5");

    // u1 submits job
    FakeJobInProgress j1 = submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    // it gets the first 5 slots
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000003_0 on tt2");
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000004_0 on tt2");
    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0001_m_000005_0 on tt3");
    // u2 submits job with 4 slots
    FakeJobInProgress j2 = submitJobAndInit(JobStatus.PREP, 4, 4, null, "u2");
    // u2 should get next 4 slots
    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0002_m_000001_0 on tt3");
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0002_m_000002_0 on tt4");
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0002_m_000003_0 on tt4");
    checkAssignment(
      taskTrackerManager, scheduler, "tt5",
      "attempt_test_0002_m_000004_0 on tt5");
    // last slot should go to u1, since u2 has no more tasks
    checkAssignment(
      taskTrackerManager, scheduler, "tt5",
      "attempt_test_0001_m_000006_0 on tt5");
    // u1 finishes a task
    taskTrackerManager.finishTask("attempt_test_0001_m_000006_0", j1);
    // u1 submits a few more jobs 
    // All the jobs are inited when submitted
    // because of addition of Eager Job Initializer all jobs in this
    //case would e initialised.
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u1");
    // u2 also submits a job
    submitJobAndInit(JobStatus.PREP, 10, 10, null, "u2");
    // now u3 submits a job
    submitJobAndInit(JobStatus.PREP, 2, 2, null, "u3");
    // next slot should go to u3, even though u2 has an earlier job, since
    // user limits have changed and u1/u2 are over limits
    checkAssignment(
      taskTrackerManager, scheduler, "tt5",
      "attempt_test_0007_m_000001_0 on tt5");
    // some other task finishes and u3 gets it
    taskTrackerManager.finishTask("attempt_test_0002_m_000004_0", j1);
    checkAssignment(
      taskTrackerManager, scheduler, "tt5",
      "attempt_test_0007_m_000002_0 on tt5");
    // now, u2 finishes a task
    taskTrackerManager.finishTask("attempt_test_0002_m_000002_0", j1);
    // next slot will go to u1, since u3 has nothing to run and u1's job is 
    // first in the queue
    checkAssignment(
      taskTrackerManager, scheduler, "tt4",
      "attempt_test_0001_m_000007_0 on tt4");
  }

  /**
   * Test to verify that high memory jobs hit user limits faster than any normal
   * job.
   *
   * @throws IOException
   */
  public void testUserLimitsForHighMemoryJobs()
    throws IOException {
    taskTrackerManager = new FakeTaskTrackerManager(1, 10, 10);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 50));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // Submit one normal job to the other queue.
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setUser("u1");
    jConf.setQueueName("default");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug(
      "Submit one high memory(2GB maps, 2GB reduces) job of "
        + "6 map and 6 reduce tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setQueueName("default");
    jConf.setUser("u2");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // Verify that normal job takes 3 task assignments to hit user limits
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000002_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000003_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000003_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000004_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000004_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000005_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000005_0 on tt1");
    // u1 has 5 map slots and 5 reduce slots. u2 has none. So u1's user limits
    // are hit. So u2 should get slots

    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000002_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000002_0 on tt1");

    // u1 has 5 map slots and 5 reduce slots. u2 has 4 map slots and 4 reduce
    // slots. Because of high memory tasks, giving u2 another task would
    // overflow limits. So, no more tasks should be given to anyone.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  /*
   * Following is the testing strategy for testing scheduling information.
   * - start capacity scheduler with two queues.
   * - check the scheduling information with respect to the configuration
   * which was used to configure the queues.
   * - Submit 5 jobs to a queue.
   * - Check the waiting jobs count, it should be 5.
   * - Then run initializationPoller()
   * - Check once again the waiting queue, it should be 5 jobs again.
   * - Then raise status change events.
   * - Assign one task to a task tracker. (Map)
   * - Check waiting job count, it should be 4 now and used map (%) = 100
   * - Assign another one task (Reduce)
   * - Check waiting job count, it should be 4 now and used map (%) = 100
   * and used reduce (%) = 100
   * - finish the job and then check the used percentage it should go
   * back to zero
   * - Then pick an initialized job but not scheduled job and fail it.
   * - Run the poller
   * - Check the waiting job count should now be 3.
   * - Now fail a job which has not been initialized at all.
   * - Run the poller, so that it can clean up the job queue.
   * - Check the count, the waiting job count should be 2.
   * - Now raise status change events to move the initialized jobs which
   * should be two in count to running queue.
   * - Then schedule a map of the job in running queue.
   * - Run the poller because the poller is responsible for waiting
   * jobs count. Check the count, it should be using 100% map and one
   * waiting job
   * - fail the running job.
   * - Check the count, it should be now one waiting job and zero running
   * tasks
   */

  public void testSchedulingInformation() throws Exception {
    String[] qs = {"default", "q2"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 25));
    queues.add(new FakeQueueInfo("q2", 50.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    scheduler.assignTasks(tracker("tt1")); // heartbeat
    scheduler.assignTasks(tracker("tt2")); // heartbeat
    int totalMaps = taskTrackerManager.getClusterStatus().getMaxMapTasks();
    int totalReduces =
      taskTrackerManager.getClusterStatus().getMaxReduceTasks();
    QueueManager queueManager = scheduler.taskTrackerManager.getQueueManager();
    String schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    String schedulingInfo2 =
      queueManager.getJobQueueInfo("q2").getSchedulingInfo();

    String[] infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    assertEquals(infoStrings[0], "Queue configuration");
    assertEquals(infoStrings[1], "Capacity Percentage: 50.0%");
    assertEquals(infoStrings[2], "User Limit: 25%");
    assertEquals(infoStrings[3], "Priority Supported: YES");
    assertEquals(infoStrings[4], "-------------");
    assertEquals(infoStrings[5], "Map tasks");
    assertEquals(
      infoStrings[6], "Capacity: " + totalMaps * 50 / 100
        + " slots");
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[9], "-------------");
    assertEquals(infoStrings[10], "Reduce tasks");
    assertEquals(
      infoStrings[11], "Capacity: " + totalReduces * 50 / 100
        + " slots");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[14], "-------------");
    assertEquals(infoStrings[15], "Job info");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 0");
    assertEquals(infoStrings[17], "Number of users who have submitted jobs: 0");

    assertEquals(schedulingInfo, schedulingInfo2);

    //Testing with actual job submission.
    ArrayList<FakeJobInProgress> userJobs =
      submitJobs(1, 5, "default").get("u1");
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");

    //waiting job should be equal to number of jobs submitted.
    assertEquals(infoStrings.length, 18);
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 5");
    assertEquals(infoStrings[17], "Number of users who have submitted jobs: 1");

    //Initalize the jobs but don't raise events
    controlledInitializationPoller.selectJobsToInitialize();

    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    //should be previous value as nothing is scheduled because no events
    //has been raised after initialization.
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 5");

    //Raise status change event so that jobs can move to running queue.
    raiseStatusChangeEvents(scheduler.jobQueuesManager);
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q2");
    //assign one job
    Task t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    //Initalize extra job.
    controlledInitializationPoller.selectJobsToInitialize();

    //Get scheduling information, now the number of waiting job should have
    //changed to 4 as one is scheduled and has become running.
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 20);
    assertEquals(infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[14], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[15], "Running tasks: 0");
    assertEquals(infoStrings[18], "Number of Waiting Jobs: 4");

    //assign a reduce task
    Task t2 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 22);
    assertEquals(infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[14], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[15], "Running tasks: 1");
    assertEquals(infoStrings[16], "Active users:");
    assertEquals(infoStrings[17], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[20], "Number of Waiting Jobs: 4");

    //Complete the job and check the running tasks count
    FakeJobInProgress u1j1 = userJobs.get(0);
    taskTrackerManager.finishTask(t1.getTaskID().toString(), u1j1);
    taskTrackerManager.finishTask(t2.getTaskID().toString(), u1j1);
    taskTrackerManager.finalizeJob(u1j1);

    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 4");

    //Fail a job which is initialized but not scheduled and check the count.
    FakeJobInProgress u1j2 = userJobs.get(1);
    assertTrue(
      "User1 job 2 not initalized ",
      u1j2.getStatus().getRunState() == JobStatus.RUNNING);
    taskTrackerManager.finalizeJob(u1j2, JobStatus.FAILED);
    //Run initializer to clean up failed jobs
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    //should be previous value as nothing is scheduled because no events
    //has been raised after initialization.
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 3");

    //Fail a job which is not initialized but is in the waiting queue.
    FakeJobInProgress u1j5 = userJobs.get(4);
    assertFalse(
      "User1 job 5 initalized ",
      u1j5.getStatus().getRunState() == JobStatus.RUNNING);

    taskTrackerManager.finalizeJob(u1j5, JobStatus.FAILED);
    //run initializer to clean up failed job
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    //should be previous value as nothing is scheduled because no events
    //has been raised after initialization.
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[12], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[13], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 2");

    //Raise status change events as none of the intialized jobs would be
    //in running queue as we just failed the second job which was initialized
    //and completed the first one.
    raiseStatusChangeEvents(scheduler.jobQueuesManager);
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q2");

    //Now schedule a map should be job3 of the user as job1 succeeded job2
    //failed and now job3 is running
    t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0003_m_000001_0 on tt1");
    FakeJobInProgress u1j3 = userJobs.get(2);
    assertTrue(
      "User Job 3 not running ",
      u1j3.getStatus().getRunState() == JobStatus.RUNNING);

    //now the running count of map should be one and waiting jobs should be
    //one. run the poller as it is responsible for waiting count
    controlledInitializationPoller.selectJobsToInitialize();
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 20);
    assertEquals(infoStrings[7], "Used capacity: 1 (100.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 1");
    assertEquals(infoStrings[9], "Active users:");
    assertEquals(infoStrings[10], "User 'u1': 1 (100.0% of used capacity)");
    assertEquals(infoStrings[18], "Number of Waiting Jobs: 1");

    //Fail the executing job
    taskTrackerManager.finalizeJob(u1j3, JobStatus.FAILED);
    // make sure we update our stats
    scheduler.updateContextInfoForTests();
    //Now running counts should become zero
    schedulingInfo =
      queueManager.getJobQueueInfo("default").getSchedulingInfo();
    infoStrings = schedulingInfo.split("\n");
    assertEquals(infoStrings.length, 18);
    assertEquals(infoStrings[7], "Used capacity: 0 (0.0% of Capacity)");
    assertEquals(infoStrings[8], "Running tasks: 0");
    assertEquals(infoStrings[16], "Number of Waiting Jobs: 1");
  }

  /**
   * Test to verify that highMemoryJobs are scheduled like all other jobs when
   * memory-based scheduling is not enabled.
   *
   * @throws IOException
   */
  public void testDisabledMemoryBasedScheduling()
    throws IOException {

    LOG.debug("Starting the scheduler.");
    taskTrackerManager = new FakeTaskTrackerManager(1, 1, 1);

    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // memory-based scheduling disabled by default.
    scheduler.start();

    LOG.debug(
      "Submit one high memory job of 1 3GB map task "
        + "and 1 1GB reduce task.");
    JobConf jConf = new JobConf();
    jConf.setMemoryForMapTask(3 * 1024L); // 3GB
    jConf.setMemoryForReduceTask(1 * 1024L); // 1 GB
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    submitJobAndInit(JobStatus.RUNNING, jConf);

    // assert that all tasks are launched even though they transgress the
    // scheduling limits.

    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");
  }

  /**
   * Test reverting HADOOP-4979. If there is a high-mem job, we should now look
   * at reduce jobs (if map tasks are high-mem) or vice-versa.
   *
   * @throws IOException
   */
  public void testHighMemoryBlockingAcrossTaskTypes()
    throws IOException {

    // 2 map and 1 reduce slots
    taskTrackerManager = new FakeTaskTrackerManager(1, 2, 1);

    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
      2 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
      1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // The situation : Two jobs in the queue. First job with only maps and no
    // reduces and is a high memory job. Second job is a normal job with both
    // maps and reduces.
    // First job cannot run for want of memory for maps. In this case, second
    // job's reduces should run.

    LOG.debug(
      "Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug(
      "Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // first, a map from j1 will run
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    // Total 2 map slots should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);

    // at this point, the scheduler tries to schedule another map from j1. 
    // there isn't enough space. The second job's reduce should be scheduled.
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000001_0 on tt1");
    // Total 1 reduce slot should be accounted for.
    checkOccupiedSlots(
      "default", TaskType.REDUCE, 1, 1,
      100.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 1 * 1024L);
  }

  /**
   * Test blocking of cluster for lack of memory.
   *
   * @throws IOException
   */
  public void testClusterBlockingForLackOfMemory()
    throws IOException {

    LOG.debug("Starting the scheduler.");
    taskTrackerManager = new FakeTaskTrackerManager(2, 2, 2);

    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    taskTrackerManager.addQueues(new String[]{"default"});
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal jobs 1GB maps/reduces. 2GB limit on maps/reduces
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
      2 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
      2 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug(
      "Submit one normal memory(1GB maps/reduces) job of "
        + "1 map, 1 reduce tasks.");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // Fill the second tt with this job.
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000001_0 on tt2");
    // Total 1 map slot should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 25.0f);
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
        1, 1, 0, 0, 0, 0),
      (String) job1.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt2", 1 * 1024L, 0L);
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_r_000001_0 on tt2");
    // Total 1 map slot should be accounted for.
    checkOccupiedSlots(
      "default", TaskType.REDUCE, 1, 1,
      25.0f);
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
        1, 1, 0, 1, 1, 0),
      (String) job1.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt2", 1 * 1024L, 1 * 1024L);

    LOG.debug(
      "Submit one high memory(2GB maps/reduces) job of "
        + "2 map, 2 reduce tasks.");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    // Total 3 map slots should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 3, 75.0f);
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
        1, 2, 0, 0, 0, 0),
      (String) job2.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 0L);

    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000001_0 on tt1");
    // Total 3 reduce slots should be accounted for.
    checkOccupiedSlots(
      "default", TaskType.REDUCE, 1, 3,
      75.0f);
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
        1, 2, 0, 1, 2, 0),
      (String) job2.getSchedulingInfo());
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 2 * 1024L);

    LOG.debug(
      "Submit one normal memory(1GB maps/reduces) job of "
        + "1 map, 0 reduce tasks.");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(1);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job3 = submitJobAndInit(JobStatus.PREP, jConf);

    // Job2 cannot fit on tt1. So tt1 is reserved for a map slot of job2
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt1")));

    // reserved tasktrackers contribute to occupied slots for maps.
    checkOccupiedSlots("default", TaskType.MAP, 1, 5, 125.0f);
    // occupied slots for reduces remain unchanged as tt1 is not reserved for
    // reduces.
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 3, 75.0f);
    checkMemReservedForTasksOnTT("tt1", 2 * 1024L, 2 * 1024L);
    checkMemReservedForTasksOnTT("tt2", 1 * 1024L, 1 * 1024L);
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
        1, 2, 2, 1, 2, 0),
      (String) job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING,
        0, 0, 0, 0, 0, 0),
      (String) job3.getSchedulingInfo());

    // One reservation is already done for job2. So job3 should go ahead.
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0003_m_000001_0 on tt2");
  }

  /**
   * Testcase to verify fix for a NPE (HADOOP-5641), when memory based
   * scheduling is enabled and jobs are retired from memory when tasks
   * are still active on some Tasktrackers.
   *
   * @throws IOException
   */
  public void testMemoryMatchingWithRetiredJobs() throws IOException {
    // create a cluster with a single node.
    LOG.debug("Starting cluster with 1 tasktracker, 2 map and 2 reduce slots");
    taskTrackerManager = new FakeTaskTrackerManager(1, 2, 2);

    // create scheduler
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    taskTrackerManager.addQueues(new String[]{"default"});
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    LOG.debug("Assume TT has 2GB for maps and 2GB for reduces");
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY,
      2 * 1024L);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 512);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY,
      2 * 1024L);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 512);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    // submit a normal job
    LOG.debug("Submitting a normal job with 2 maps and 2 reduces");
    JobConf jConf = new JobConf();
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setMemoryForMapTask(512);
    jConf.setMemoryForReduceTask(512);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // 1st cycle - 1 map gets assigned.
    Task t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    // Total 1 map slot should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 50.0f);
    checkMemReservedForTasksOnTT("tt1", 512L, 0L);

    // 1st cycle of reduces - 1 reduce gets assigned.
    Task t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");
    // Total 1 reduce slot should be accounted for.
    checkOccupiedSlots(
      "default", TaskType.REDUCE, 1, 1,
      50.0f);
    checkMemReservedForTasksOnTT("tt1", 512L, 512L);

    // kill this job !
    taskTrackerManager.killJob(job1.getJobID());
    // No more map/reduce slots should be accounted for.
    checkOccupiedSlots("default", TaskType.MAP, 0, 0, 0.0f);
    checkOccupiedSlots(
      "default", TaskType.REDUCE, 0, 0,
      0.0f);

    // retire the job
    taskTrackerManager.removeJob(job1.getJobID());

    // submit another job.
    LOG.debug("Submitting another normal job with 2 maps and 2 reduces");
    jConf = new JobConf();
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setMemoryForMapTask(512);
    jConf.setMemoryForReduceTask(512);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // since with HADOOP-5964, we don't rely on a job conf to get
    // the memory occupied, scheduling should be able to work correctly.
    t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    checkOccupiedSlots("default", TaskType.MAP, 1, 1, 50);
    checkMemReservedForTasksOnTT("tt1", 1024L, 512L);

    // assign a reduce now.
    t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000001_0 on tt1");
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 1, 50);
    checkMemReservedForTasksOnTT("tt1", 1024L, 1024L);

    // now, no more can be assigned because all the slots are blocked.
    assertNull(scheduler.assignTasks(tracker("tt1")));

    // finish the tasks on the tracker.
    taskTrackerManager.finishTask(t.getTaskID().toString(), job1);
    taskTrackerManager.finishTask(t1.getTaskID().toString(), job1);

    // now a new task can be assigned.
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000002_0 on tt1");
    checkOccupiedSlots("default", TaskType.MAP, 1, 2, 100.0f);
    // memory used will change because of the finished task above.
    checkMemReservedForTasksOnTT("tt1", 1024L, 512L);

    // reduce can be assigned.
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000002_0 on tt1");
    checkOccupiedSlots("default", TaskType.REDUCE, 1, 2, 100.0f);
    checkMemReservedForTasksOnTT("tt1", 1024L, 1024L);
  }

  /*
   * Test cases for Job Initialization poller.
   */

  /*
  * This test verifies that the correct number of jobs for
  * correct number of users is initialized.
  * It also verifies that as jobs of users complete, new jobs
  * from the correct users are initialized.
  */

  public void testJobInitialization() throws Exception {
    // set up the scheduler
    String[] qs = {"default"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller initPoller = scheduler.getInitializationPoller();

    // submit 4 jobs each for 3 users.
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs = submitJobs(
      3,
      4, "default");

    // get the jobs submitted.
    ArrayList<FakeJobInProgress> u1Jobs = userJobs.get("u1");
    ArrayList<FakeJobInProgress> u2Jobs = userJobs.get("u2");
    ArrayList<FakeJobInProgress> u3Jobs = userJobs.get("u3");

    // reference to the initializedJobs data structure
    // changes are reflected in the set as they are made by the poller
    Set<JobID> initializedJobs = initPoller.getInitializedJobList();

    // we should have 12 (3 x 4) jobs in the job queue
    assertEquals(mgr.getJobQueue("default").getWaitingJobs().size(), 12);

    // run one poller iteration.
    controlledInitializationPoller.selectJobsToInitialize();

    // the poller should initialize 6 jobs
    // 3 users and 2 jobs from each
    assertEquals(initializedJobs.size(), 6);

    assertTrue(
      "Initialized jobs didnt contain the user1 job 1",
      initializedJobs.contains(u1Jobs.get(0).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user1 job 2",
      initializedJobs.contains(u1Jobs.get(1).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user2 job 1",
      initializedJobs.contains(u2Jobs.get(0).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user2 job 2",
      initializedJobs.contains(u2Jobs.get(1).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user3 job 1",
      initializedJobs.contains(u3Jobs.get(0).getJobID()));
    assertTrue(
      "Initialized jobs didnt contain the user3 job 2",
      initializedJobs.contains(u3Jobs.get(1).getJobID()));

    // now submit one more job from another user.
    FakeJobInProgress u4j1 =
      submitJob(JobStatus.PREP, 1, 1, "default", "u4");

    // run the poller again.
    controlledInitializationPoller.selectJobsToInitialize();

    // since no jobs have started running, there should be no
    // change to the initialized jobs.
    assertEquals(initializedJobs.size(), 6);
    assertFalse(
      "Initialized jobs contains user 4 jobs",
      initializedJobs.contains(u4j1.getJobID()));

    // This event simulates raising the event on completion of setup task
    // and moves the job to the running list for the scheduler to pick up.
    raiseStatusChangeEvents(mgr);

    // get some tasks assigned.
    Task t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    Task t2 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");
    Task t3 = checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000001_0 on tt2");
    Task t4 = checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_r_000001_0 on tt2");
    taskTrackerManager.finishTask(
      t1.getTaskID().toString(), u1Jobs.get(
        0));
    taskTrackerManager.finishTask(
      t2.getTaskID().toString(), u1Jobs.get(
        0));
    taskTrackerManager.finishTask(
      t3.getTaskID().toString(), u1Jobs.get(
        1));
    taskTrackerManager.finishTask(
      t4.getTaskID().toString(), u1Jobs.get(
        1));

    // as some jobs have running tasks, the poller will now
    // pick up new jobs to initialize.
    controlledInitializationPoller.selectJobsToInitialize();

    // count should still be the same
    assertEquals(initializedJobs.size(), 6);

    // new jobs that have got into the list
    assertTrue(initializedJobs.contains(u1Jobs.get(2).getJobID()));
    assertTrue(initializedJobs.contains(u1Jobs.get(3).getJobID()));
    raiseStatusChangeEvents(mgr);

    // the first two jobs are done, no longer in the initialized list.
    assertFalse(
      "Initialized jobs contains the user1 job 1",
      initializedJobs.contains(u1Jobs.get(0).getJobID()));
    assertFalse(
      "Initialized jobs contains the user1 job 2",
      initializedJobs.contains(u1Jobs.get(1).getJobID()));

    // finish one more job
    t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0003_m_000001_0 on tt1");
    t2 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0003_r_000001_0 on tt1");
    taskTrackerManager.finishTask(
      t1.getTaskID().toString(), u1Jobs.get(
        2));
    taskTrackerManager.finishTask(
      t2.getTaskID().toString(), u1Jobs.get(
        2));

    // no new jobs should be picked up, because max user limit
    // is still 3.
    controlledInitializationPoller.selectJobsToInitialize();

    assertEquals(initializedJobs.size(), 5);

    // run 1 more jobs.. 
    t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0004_m_000001_0 on tt1");
    t1 = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0004_r_000001_0 on tt1");
    taskTrackerManager.finishTask(
      t1.getTaskID().toString(), u1Jobs.get(
        3));
    taskTrackerManager.finishTask(
      t2.getTaskID().toString(), u1Jobs.get(
        3));

    // Now initialised jobs should contain user 4's job, as
    // user 1's jobs are all done and the number of users is
    // below the limit
    controlledInitializationPoller.selectJobsToInitialize();
    assertEquals(initializedJobs.size(), 5);
    assertTrue(initializedJobs.contains(u4j1.getJobID()));

    controlledInitializationPoller.stopRunning();
  }

  /*
   * testHighPriorityJobInitialization() shows behaviour when high priority job
   * is submitted into a queue and how initialisation happens for the same.
   */
  public void testHighPriorityJobInitialization() throws Exception {
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobInitializationPoller initPoller = scheduler.getInitializationPoller();
    Set<JobID> initializedJobsList = initPoller.getInitializedJobList();

    // submit 3 jobs for 3 users
    submitJobs(3, 3, "default");
    controlledInitializationPoller.selectJobsToInitialize();
    assertEquals(initializedJobsList.size(), 6);

    // submit 2 job for a different user. one of them will be made high priority
    FakeJobInProgress u4j1 = submitJob(JobStatus.PREP, 1, 1, "default", "u4");
    FakeJobInProgress u4j2 = submitJob(JobStatus.PREP, 1, 1, "default", "u4");

    controlledInitializationPoller.selectJobsToInitialize();

    // shouldn't change
    assertEquals(initializedJobsList.size(), 6);

    assertFalse(
      "Contains U4J1 high priority job ",
      initializedJobsList.contains(u4j1.getJobID()));
    assertFalse(
      "Contains U4J2 Normal priority job ",
      initializedJobsList.contains(u4j2.getJobID()));

    // change priority of one job
    taskTrackerManager.setPriority(u4j1, JobPriority.VERY_HIGH);

    controlledInitializationPoller.selectJobsToInitialize();

    // the high priority job should get initialized, but not the
    // low priority job from u4, as we have already exceeded the
    // limit.
    assertEquals(initializedJobsList.size(), 7);
    assertTrue(
      "Does not contain U4J1 high priority job ",
      initializedJobsList.contains(u4j1.getJobID()));
    assertFalse(
      "Contains U4J2 Normal priority job ",
      initializedJobsList.contains(u4j2.getJobID()));
    controlledInitializationPoller.stopRunning();
  }

  public void testJobMovement() throws Exception {
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;

    // check proper running job movement and completion
    checkRunningJobMovementAndCompletion();

    // check failed running job movement
    checkFailedRunningJobMovement();

    // Check job movement of failed initalized job
    checkFailedInitializedJobMovement();

    // Check failed waiting job movement
    checkFailedWaitingJobMovement();
  }

  public void testStartWithoutDefaultQueueConfigured() throws Exception {
    //configure a single queue which is not default queue
    String[] qs = {"q1"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("q1", 100.0f, true, 100));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    //Start the scheduler.
    scheduler.start();
    //Submit a job and wait till it completes
    FakeJobInProgress job =
      submitJob(JobStatus.PREP, 1, 1, "q1", "u1");
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(scheduler.jobQueuesManager, "q1");
    Task t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");
  }

  public void testFailedJobInitalizations() throws Exception {
    String[] qs = {"default"};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;

    //Submit a job whose initialization would fail always.
    FakeJobInProgress job =
      new FakeFailingJobInProgress(
        new JobID("test", ++jobCounter),
        new JobConf(), taskTrackerManager, "u1");
    job.getStatus().setRunState(JobStatus.PREP);
    taskTrackerManager.submitJob(job);
    //check if job is present in waiting list.
    assertEquals(
      "Waiting job list does not contain submitted job",
      1, mgr.getJobQueue("default").getWaitingJobCount());
    assertTrue(
      "Waiting job does not contain submitted job",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));
    //initialization should fail now.
    controlledInitializationPoller.selectJobsToInitialize();
    //Check if the job has been properly cleaned up.
    assertEquals(
      "Waiting job list contains submitted job",
      0, mgr.getJobQueue("default").getWaitingJobCount());
    assertFalse(
      "Waiting job contains submitted job",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));
    assertFalse(
      "Waiting job contains submitted job",
      mgr.getJobQueue("default").getRunningJobs().contains(job));
  }

  /**
   * Test case deals with normal jobs which have speculative maps and reduce.
   * Following is test executed
   * <ol>
   * <li>Submit one job with speculative maps and reduce.</li>
   * <li>Submit another job with no speculative execution.</li>
   * <li>Observe that all tasks from first job get scheduled, speculative
   * and normal tasks</li>
   * <li>Finish all the first jobs tasks second jobs tasks get scheduled.</li>
   * </ol>
   *
   * @throws IOException
   */
  public void testSpeculativeTaskScheduling() throws IOException {
    String[] qs = {"default"};
    taskTrackerManager = new FakeTaskTrackerManager(2, 1, 1);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 100));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobConf conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setMapSpeculativeExecution(true);
    conf.setReduceSpeculativeExecution(true);
    //Submit a job which would have one speculative map and one speculative
    //reduce.
    FakeJobInProgress fjob1 = submitJob(JobStatus.PREP, conf);

    conf = new JobConf();
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    //Submit a job which has no speculative map or reduce.
    FakeJobInProgress fjob2 = submitJob(JobStatus.PREP, conf);

    //Ask the poller to initalize all the submitted job and raise status
    //change event.
    controlledInitializationPoller.selectJobsToInitialize();
    raiseStatusChangeEvents(mgr);

    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    assertTrue(
      "Pending maps of job1 greater than zero",
      (fjob1.pendingMaps() == 0));
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000001_1 on tt2");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");
    assertTrue(
      "Pending reduces of job2 greater than zero",
      (fjob1.pendingReduces() == 0));
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_r_000001_1 on tt2");

    taskTrackerManager.finishTask("attempt_test_0001_m_000001_0", fjob1);
    taskTrackerManager.finishTask("attempt_test_0001_m_000001_1", fjob1);
    taskTrackerManager.finishTask("attempt_test_0001_r_000001_0", fjob1);
    taskTrackerManager.finishTask("attempt_test_0001_r_000001_1", fjob1);
    taskTrackerManager.finalizeJob(fjob1);

    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000001_0 on tt1");
    taskTrackerManager.finishTask("attempt_test_0002_m_000001_0", fjob2);
    taskTrackerManager.finishTask("attempt_test_0002_r_000001_0", fjob2);
    taskTrackerManager.finalizeJob(fjob2);
  }

  /**
   * Test to verify that TTs are reserved for high memory jobs, but only till a
   * TT is reserved for each of the pending task.
   *
   * @throws IOException
   */
  public void testTTReservingWithHighMemoryJobs()
    throws IOException {
    // 3 taskTrackers, 2 map and 0 reduce slots on each TT
    taskTrackerManager = new FakeTaskTrackerManager(3, 2, 0);

    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    scheduler.setTaskTrackerManager(taskTrackerManager);
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug(
      "Submit a regular memory(1GB vmem maps/reduces) job of "
        + "3 map/red tasks");
    JobConf jConf = new JobConf(conf);
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(3);
    jConf.setNumReduceTasks(3);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // assign one map task of job1 on all the TTs
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000002_0 on tt2");
    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0001_m_000003_0 on tt3");
    scheduler.updateContextInfoForTests();

    LOG.info(job1.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 3, 3, 0, 0,
        0, 0), (String) job1.getSchedulingInfo());

    LOG.debug(
      "Submit one high memory(2GB maps, 0MB reduces) job of "
        + "2 map tasks");
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(0);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(0);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    LOG.debug(
      "Submit another regular memory(1GB vmem maps/reduces) job of "
        + "2 map/red tasks");
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(2);
    jConf.setNumReduceTasks(2);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job3 = submitJobAndInit(JobStatus.PREP, jConf);

    // Job2, a high memory job cannot be accommodated on a any TT. But with each
    // trip to the scheduler, each of the TT should be reserved by job2.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 2, 0,
        0, 0), (String) job2.getSchedulingInfo());

    assertNull(scheduler.assignTasks(tracker("tt2")));
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    // Job2 has only 2 pending tasks. So no more reservations. Job3 should get
    // slots on tt3. tt1 and tt2 should not be assigned any slots with the
    // reservation stats intact.
    assertNull(scheduler.assignTasks(tracker("tt1")));
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    assertNull(scheduler.assignTasks(tracker("tt2")));
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    checkAssignment(
      taskTrackerManager, scheduler, "tt3",
      "attempt_test_0003_m_000001_0 on tt3");
    scheduler.updateContextInfoForTests();
    LOG.info(job2.getSchedulingInfo());
    assertEquals(
      String.format(
        TaskSchedulingContext.JOB_SCHEDULING_INFO_FORMAT_STRING, 0, 0, 4, 0,
        0, 0), (String) job2.getSchedulingInfo());

    // No more tasks there in job3 also
    assertNull(scheduler.assignTasks(tracker("tt3")));
  }

  /**
   * Test to verify that queue ordering is based on the number of slots occupied
   * and hence to verify that presence of high memory jobs is reflected properly
   * while determining used capacities of queues and hence the queue ordering.
   *
   * @throws IOException
   */
  public void testQueueOrdering()
    throws IOException {
    taskTrackerManager = new FakeTaskTrackerManager(2, 6, 6);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    String[] qs = {"default", "q1"};
    String[] reversedQs = {qs[1], qs[0]};
    taskTrackerManager.addQueues(qs);
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 50.0f, true, 100));
    queues.add(new FakeQueueInfo("q1", 50.0f, true, 100));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    // enabled memory-based scheduling
    // Normal job in the cluster would be 1GB maps/reduces
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_MAP_MEMORY_MB_PROPERTY, 2 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAP_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_MAX_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.getConf().setLong(
      JobTracker.MAPRED_CLUSTER_REDUCE_MEMORY_MB_PROPERTY, 1 * 1024);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    LOG.debug(
      "Submit one high memory(2GB maps, 2GB reduces) job of "
        + "6 map and 6 reduce tasks");
    JobConf jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(2 * 1024);
    jConf.setMemoryForReduceTask(2 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setQueueName("default");
    jConf.setUser("u1");
    FakeJobInProgress job1 = submitJobAndInit(JobStatus.PREP, jConf);

    // Submit a normal job to the other queue.
    jConf = new JobConf(conf);
    jConf.setMemoryForMapTask(1 * 1024);
    jConf.setMemoryForReduceTask(1 * 1024);
    jConf.setNumMapTasks(6);
    jConf.setNumReduceTasks(6);
    jConf.setUser("u1");
    jConf.setQueueName("q1");
    FakeJobInProgress job2 = submitJobAndInit(JobStatus.PREP, jConf);

    // Map 1 of high memory job
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    checkQueuesOrder(
      qs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 1 of high memory job
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");
    checkQueuesOrder(
      qs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Map 1 of normal job
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000001_0 on tt1");
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 1 of normal job
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000001_0 on tt1");
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Map 2 of normal job
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_m_000002_0 on tt1");
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 2 of normal job
    checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0002_r_000002_0 on tt1");
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Now both the queues are equally served. But the comparator doesn't change
    // the order if queues are equally served.

    // Map 3 of normal job
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000003_0 on tt2");
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 3 of normal job
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_r_000003_0 on tt2");
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Map 2 of high memory job
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_m_000002_0 on tt2");
    checkQueuesOrder(
      qs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 2 of high memory job
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0001_r_000002_0 on tt2");
    checkQueuesOrder(
      qs, scheduler
        .getOrderedQueues(TaskType.REDUCE));

    // Map 4 of normal job
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_m_000004_0 on tt2");
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.MAP));

    // Reduce 4 of normal job
    checkAssignment(
      taskTrackerManager, scheduler, "tt2",
      "attempt_test_0002_r_000004_0 on tt2");
    checkQueuesOrder(
      reversedQs, scheduler
        .getOrderedQueues(TaskType.REDUCE));
  }

  private void checkRunningJobMovementAndCompletion() throws IOException {

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller p = scheduler.getInitializationPoller();
    // submit a job
    FakeJobInProgress job =
      submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    controlledInitializationPoller.selectJobsToInitialize();

    assertEquals(p.getInitializedJobList().size(), 1);

    // make it running.
    raiseStatusChangeEvents(mgr);

    // it should be there in both the queues.
    assertTrue(
      "Job not present in Job Queue",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));
    assertTrue(
      "Job not present in Running Queue",
      mgr.getJobQueue("default").getRunningJobs().contains(job));

    // assign a task
    Task t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_m_000001_0 on tt1");
    t = checkAssignment(
      taskTrackerManager, scheduler, "tt1",
      "attempt_test_0001_r_000001_0 on tt1");

    controlledInitializationPoller.selectJobsToInitialize();

    // now this task should be removed from the initialized list.
    assertTrue(p.getInitializedJobList().isEmpty());

    // the job should also be removed from the job queue as tasks
    // are scheduled
    assertFalse(
      "Job present in Job Queue",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));

    // complete tasks and job
    taskTrackerManager.finishTask("attempt_test_0001_m_000001_0", job);
    taskTrackerManager.finishTask("attempt_test_0001_r_000001_0", job);
    taskTrackerManager.finalizeJob(job);

    // make sure it is removed from the run queue
    assertFalse(
      "Job present in running queue",
      mgr.getJobQueue("default").getRunningJobs().contains(job));
  }

  private void checkFailedRunningJobMovement() throws IOException {

    JobQueuesManager mgr = scheduler.jobQueuesManager;

    //submit a job and initalized the same
    FakeJobInProgress job =
      submitJobAndInit(JobStatus.RUNNING, 1, 1, "default", "u1");

    //check if the job is present in running queue.
    assertTrue(
      "Running jobs list does not contain submitted job",
      mgr.getJobQueue("default").getRunningJobs().contains(job));

    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);

    //check if the job is properly removed from running queue.
    assertFalse(
      "Running jobs list does not contain submitted job",
      mgr.getJobQueue("default").getRunningJobs().contains(job));

  }

  private void checkFailedInitializedJobMovement() throws IOException {

    JobQueuesManager mgr = scheduler.jobQueuesManager;
    JobInitializationPoller p = scheduler.getInitializationPoller();

    //submit a job
    FakeJobInProgress job = submitJob(JobStatus.PREP, 1, 1, "default", "u1");
    //Initialize the job
    p.selectJobsToInitialize();
    //Don't raise the status change event.

    //check in waiting and initialized jobs list.
    assertTrue(
      "Waiting jobs list does not contain the job",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));

    assertTrue(
      "Initialized job does not contain the job",
      p.getInitializedJobList().contains(job.getJobID()));

    //fail the initalized job
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);

    //Check if the job is present in waiting queue
    assertFalse(
      "Waiting jobs list contains failed job",
      mgr.getJobQueue("default").getWaitingJobs().contains(job));

    //run the poller to do the cleanup
    p.selectJobsToInitialize();

    //check for failed job in the initialized job list
    assertFalse(
      "Initialized jobs  contains failed job",
      p.getInitializedJobList().contains(job.getJobID()));
  }

  private void checkFailedWaitingJobMovement() throws IOException {
    JobQueuesManager mgr = scheduler.jobQueuesManager;
    // submit a job
    FakeJobInProgress job = submitJob(
      JobStatus.PREP, 1, 1, "default",
      "u1");

    // check in waiting and initialized jobs list.
    assertTrue(
      "Waiting jobs list does not contain the job", mgr
        .getJobQueue("default").getWaitingJobs().contains(job));
    // fail the waiting job
    taskTrackerManager.finalizeJob(job, JobStatus.KILLED);

    // Check if the job is present in waiting queue
    assertFalse(
      "Waiting jobs list contains failed job", mgr
        .getJobQueue("default").getWaitingJobs().contains(job));
  }

  private void raiseStatusChangeEvents(JobQueuesManager mgr) {
    raiseStatusChangeEvents(mgr, "default");
  }

  private void raiseStatusChangeEvents(JobQueuesManager mgr, String queueName) {
    Collection<JobInProgress> jips = mgr.getJobQueue(queueName)
      .getWaitingJobs();
    for (JobInProgress jip : jips) {
      if (jip.getStatus().getRunState() == JobStatus.RUNNING) {
        JobStatusChangeEvent evt = new JobStatusChangeEvent(
          jip,
          EventType.RUN_STATE_CHANGED, jip.getStatus());
        mgr.jobUpdated(evt);
      }
    }
  }

  private HashMap<String, ArrayList<FakeJobInProgress>> submitJobs(
    int numberOfUsers, int numberOfJobsPerUser, String queue)
    throws Exception {
    HashMap<String, ArrayList<FakeJobInProgress>> userJobs =
      new HashMap<String, ArrayList<FakeJobInProgress>>();
    for (int i = 1; i <= numberOfUsers; i++) {
      String user = String.valueOf("u" + i);
      ArrayList<FakeJobInProgress> jips = new ArrayList<FakeJobInProgress>();
      for (int j = 1; j <= numberOfJobsPerUser; j++) {
        jips.add(submitJob(JobStatus.PREP, 1, 1, queue, user));
      }
      userJobs.put(user, jips);
    }
    return userJobs;
  }


  protected TaskTracker tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }

  /**
   * Get the amount of memory that is reserved for tasks on the taskTracker and
   * verify that it matches what is expected.
   *
   * @param taskTracker
   * @param expectedMemForMapsOnTT
   * @param expectedMemForReducesOnTT
   */
  private void checkMemReservedForTasksOnTT(
    String taskTracker,
    Long expectedMemForMapsOnTT, Long expectedMemForReducesOnTT) {
    Long observedMemForMapsOnTT =
      scheduler.memoryMatcher.getMemReservedForTasks(
        tracker(taskTracker).getStatus(),
        TaskType.MAP);
    Long observedMemForReducesOnTT =
      scheduler.memoryMatcher.getMemReservedForTasks(
        tracker(taskTracker).getStatus(),
        TaskType.REDUCE);
    if (expectedMemForMapsOnTT == null) {
      assertTrue(observedMemForMapsOnTT == null);
    } else {
      assertTrue(observedMemForMapsOnTT.equals(expectedMemForMapsOnTT));
    }
    if (expectedMemForReducesOnTT == null) {
      assertTrue(observedMemForReducesOnTT == null);
    } else {
      assertTrue(observedMemForReducesOnTT.equals(expectedMemForReducesOnTT));
    }
  }

  /**
   * Verify the number of slots of type 'type' from the queue 'queue'.
   * incrMapIndex and incrReduceIndex are set , when expected output string is
   * changed.these values can be set if the index of
   * "Used capacity: %d (%.1f%% of Capacity)"
   * is changed.
   *
   * @param queue
   * @param type
   * @param numActiveUsers               in the queue at present.
   * @param expectedOccupiedSlots
   * @param expectedOccupiedSlotsPercent
   * @param incrMapIndex
   * @param incrReduceIndex
   */
  private void checkOccupiedSlots(
    String queue,
    TaskType type, int numActiveUsers,
    int expectedOccupiedSlots, float expectedOccupiedSlotsPercent,
    int incrMapIndex
    , int incrReduceIndex
  ) {
    scheduler.updateContextInfoForTests();
    QueueManager queueManager = scheduler.taskTrackerManager.getQueueManager();
    String schedulingInfo =
      queueManager.getJobQueueInfo(queue).getSchedulingInfo();
    String[] infoStrings = schedulingInfo.split("\n");
    int index = -1;
    if (type.equals(TaskType.MAP)) {
      index = 7 + incrMapIndex;
    } else if (type.equals(TaskType.REDUCE)) {
      index =
        (numActiveUsers == 0 ? 12 : 13 + numActiveUsers) + incrReduceIndex;
    }
    LOG.info(infoStrings[index]);
    assertEquals(
      String.format(
        "Used capacity: %d (%.1f%% of Capacity)",
        expectedOccupiedSlots, expectedOccupiedSlotsPercent),
      infoStrings[index]);
  }

  /**
   * @param queue
   * @param type
   * @param numActiveUsers
   * @param expectedOccupiedSlots
   * @param expectedOccupiedSlotsPercent
   */
  private void checkOccupiedSlots(
    String queue,
    TaskType type, int numActiveUsers,
    int expectedOccupiedSlots, float expectedOccupiedSlotsPercent
  ) {
    checkOccupiedSlots(
      queue, type, numActiveUsers, expectedOccupiedSlots,
      expectedOccupiedSlotsPercent, 0, 0);
  }

  private void checkQueuesOrder(
    String[] expectedOrder, String[] observedOrder) {
    assertTrue(
      "Observed and expected queues are not of same length.",
      expectedOrder.length == observedOrder.length);
    int i = 0;
    for (String expectedQ : expectedOrder) {
      assertTrue(
        "Observed and expected queues are not in the same order. "
          + "Differ at index " + i + ". Got " + observedOrder[i]
          + " instead of " + expectedQ, expectedQ.equals(observedOrder[i]));
      i++;
    }
  }

  public void testDeprecatedMemoryValues() throws IOException {
    // 2 map and 1 reduce slots
    taskTrackerManager.addQueues(new String[]{"default"});
    ArrayList<FakeQueueInfo> queues = new ArrayList<FakeQueueInfo>();
    queues.add(new FakeQueueInfo("default", 100.0f, true, 25));
    resConf.setFakeQueues(queues, taskTrackerManager.getQueueManager()
    );
    JobConf conf = (JobConf) (scheduler.getConf());
    conf.set(
      JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY, String.valueOf(
        1024 * 1024 * 3));
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.setResourceManagerConf(resConf);
    scheduler.start();

    assertEquals(MemoryMatcher.getLimitMaxMemForMapSlot(), 3);
    assertEquals(MemoryMatcher.getLimitMaxMemForReduceSlot(), 3);
  }
}
