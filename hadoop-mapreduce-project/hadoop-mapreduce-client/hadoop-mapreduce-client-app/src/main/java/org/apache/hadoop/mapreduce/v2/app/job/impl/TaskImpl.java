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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.Avataar;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobMapTaskRescheduledEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptKilledEvent;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerFailedEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of Task interface.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class TaskImpl implements Task, EventHandler<TaskEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(TaskImpl.class);
  private static final String SPECULATION = "Speculation: ";

  protected final JobConf conf;
  protected final Path jobFile;
  protected final int partition;
  protected final TaskAttemptListener taskAttemptListener;
  protected final EventHandler eventHandler;
  private final TaskId taskId;
  private Map<TaskAttemptId, TaskAttempt> attempts;
  private final int maxAttempts;
  protected final Clock clock;
  private final Lock readLock;
  private final Lock writeLock;
  private final MRAppMetrics metrics;
  protected final AppContext appContext;
  private long scheduledTime;
  
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected boolean encryptedShuffle;
  protected Credentials credentials;
  protected Token<JobTokenIdentifier> jobToken;
  
  //should be set to one which comes first
  //saying COMMIT_PENDING
  private TaskAttemptId commitAttempt;

  private TaskAttemptId successfulAttempt;

  private final Set<TaskAttemptId> failedAttempts;
  // Track the finished attempts - successful, failed and killed
  private final Set<TaskAttemptId> finishedAttempts;
  // counts the number of attempts that are either running or in a state where
  //  they will come to be running when they get a Container
  private final Set<TaskAttemptId> inProgressAttempts;

  private boolean historyTaskStartGenerated = false;
  // Launch time reported in history events.
  private long launchTime;
  
  private static final SingleArcTransition<TaskImpl, TaskEvent> 
     ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();
  private static final SingleArcTransition<TaskImpl, TaskEvent> 
     KILL_TRANSITION = new KillTransition();

  private static final StateMachineFactory
               <TaskImpl, TaskStateInternal, TaskEventType, TaskEvent> 
            stateMachineFactory 
           = new StateMachineFactory<TaskImpl, TaskStateInternal, TaskEventType, TaskEvent>
               (TaskStateInternal.NEW)

    // define the state machine of Task

    // Transitions from NEW state
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.SCHEDULED, 
        TaskEventType.T_SCHEDULE, new InitialScheduleTransition())
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.KILLED, 
        TaskEventType.T_KILL, new KillNewTransition())
    .addTransition(TaskStateInternal.NEW,
        EnumSet.of(TaskStateInternal.FAILED,
                   TaskStateInternal.KILLED,
                   TaskStateInternal.RUNNING,
                   TaskStateInternal.SUCCEEDED),
        TaskEventType.T_RECOVER, new RecoverTransition())

    // Transitions from SCHEDULED state
      //when the first attempt is launched, the task state is set to RUNNING
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.RUNNING, 
         TaskEventType.T_ATTEMPT_LAUNCHED, new LaunchTransition())
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.KILL_WAIT, 
         TaskEventType.T_KILL, KILL_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.SCHEDULED, 
         TaskEventType.T_ATTEMPT_KILLED, ATTEMPT_KILLED_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED, 
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.FAILED), 
        TaskEventType.T_ATTEMPT_FAILED, 
        new AttemptFailedTransition())
 
    // Transitions from RUNNING state
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_LAUNCHED) //more attempts may start later
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_COMMIT_PENDING,
        new AttemptCommitPendingTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ADD_SPEC_ATTEMPT, new RedundantScheduleTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.SUCCEEDED, 
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_KILLED,
        ATTEMPT_KILLED_TRANSITION)
    .addTransition(TaskStateInternal.RUNNING, 
        EnumSet.of(TaskStateInternal.RUNNING, TaskStateInternal.FAILED), 
        TaskEventType.T_ATTEMPT_FAILED,
        new AttemptFailedTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.KILL_WAIT, 
        TaskEventType.T_KILL, KILL_TRANSITION)

    // Transitions from KILL_WAIT state
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_KILLED,
        new KillWaitAttemptKilledTransition())
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new KillWaitAttemptSucceededTransition())
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_FAILED,
        new KillWaitAttemptFailedTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.KILL_WAIT,
        TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskEventType.T_KILL,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_ATTEMPT_COMMIT_PENDING,
            TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from SUCCEEDED state
    .addTransition(TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED, new RetroactiveFailureTransition())
    .addTransition(TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED),
        TaskEventType.T_ATTEMPT_KILLED, new RetroactiveKilledTransition())
    .addTransition(TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededAtSucceededTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_ATTEMPT_COMMIT_PENDING,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_KILL))

    // Transitions from FAILED state        
    .addTransition(TaskStateInternal.FAILED, TaskStateInternal.FAILED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_ADD_SPEC_ATTEMPT,
                   TaskEventType.T_ATTEMPT_COMMIT_PENDING,
                   TaskEventType.T_ATTEMPT_FAILED,
                   TaskEventType.T_ATTEMPT_KILLED,
                   TaskEventType.T_ATTEMPT_LAUNCHED,
                   TaskEventType.T_ATTEMPT_SUCCEEDED))

    // Transitions from KILLED state
    // There could be a race condition where TaskImpl might receive
    // T_ATTEMPT_SUCCEEDED followed by T_ATTEMPTED_KILLED for the same attempt.
    // a. The task is in KILL_WAIT.
    // b. Before TA transitions to SUCCEEDED state, Task sends TA_KILL event.
    // c. TA transitions to SUCCEEDED state and thus send T_ATTEMPT_SUCCEEDED
    //    to the task. The task transitions to KILLED state.
    // d. TA processes TA_KILL event and sends T_ATTEMPT_KILLED to the task.
    .addTransition(TaskStateInternal.KILLED, TaskStateInternal.KILLED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_SCHEDULE,
                   TaskEventType.T_ATTEMPT_KILLED,
                   TaskEventType.T_ADD_SPEC_ATTEMPT))

    // create the topology tables
    .installTopology();

  private final StateMachine<TaskStateInternal, TaskEventType, TaskEvent>
    stateMachine;

  // By default, the next TaskAttempt number is zero. Changes during recovery  
  protected int nextAttemptNumber = 0;

  // For sorting task attempts by completion time
  private static final Comparator<TaskAttemptInfo> TA_INFO_COMPARATOR =
      new Comparator<TaskAttemptInfo>() {
        @Override
        public int compare(TaskAttemptInfo a, TaskAttemptInfo b) {
          long diff = a.getFinishTime() - b.getFinishTime();
          return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
        }
      };

  @Override
  public TaskState getState() {
    readLock.lock();
    try {
      return getExternalState(getInternalState());
    } finally {
      readLock.unlock();
    }
  }

  public TaskImpl(JobId jobId, TaskType taskType, int partition,
      EventHandler eventHandler, Path remoteJobConfFile, JobConf conf,
      TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      int appAttemptId, MRAppMetrics metrics, AppContext appContext) {
    this.conf = conf;
    this.clock = clock;
    this.jobFile = remoteJobConfFile;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    this.attempts = Collections.emptyMap();
    this.finishedAttempts = new HashSet<TaskAttemptId>(2);
    this.failedAttempts = new HashSet<TaskAttemptId>(2);
    this.inProgressAttempts = new HashSet<TaskAttemptId>(2);
    // This overridable method call is okay in a constructor because we
    //  have a convention that none of the overrides depends on any
    //  fields that need initialization.
    maxAttempts = getMaxAttempts();
    taskId = MRBuilderUtils.newTaskId(jobId, partition, taskType);
    this.partition = partition;
    this.taskAttemptListener = taskAttemptListener;
    this.eventHandler = eventHandler;
    this.credentials = credentials;
    this.jobToken = jobToken;
    this.metrics = metrics;
    this.appContext = appContext;
    this.encryptedShuffle = conf.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
                                            MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT);

    // This "this leak" is okay because the retained pointer is in an
    //  instance variable.
    stateMachine = stateMachineFactory.make(this);

    // All the new TaskAttemptIDs are generated based on MR
    // ApplicationAttemptID so that attempts from previous lives don't
    // over-step the current one. This assumes that a task won't have more
    // than 1000 attempts in its single generation, which is very reasonable.
    nextAttemptNumber = (appAttemptId - 1) * 1000;
  }

  @Override
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    readLock.lock();

    try {
      if (attempts.size() <= 1) {
        return attempts;
      }
      
      Map<TaskAttemptId, TaskAttempt> result
          = new LinkedHashMap<TaskAttemptId, TaskAttempt>();
      result.putAll(attempts);

      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttempt getAttempt(TaskAttemptId attemptID) {
    readLock.lock();
    try {
      return attempts.get(attemptID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskId getID() {
    return taskId;
  }

  @Override
  public boolean isFinished() {
    readLock.lock();
    try {
     // TODO: Use stateMachine level method?
      return (getInternalState() == TaskStateInternal.SUCCEEDED ||
              getInternalState() == TaskStateInternal.FAILED ||
              getInternalState() == TaskStateInternal.KILLED);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskReport getReport() {
    TaskReport report = recordFactory.newRecordInstance(TaskReport.class);
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      report.setTaskId(taskId);
      report.setStartTime(getLaunchTime());
      report.setFinishTime(getFinishTime());
      report.setTaskState(getState());
      report.setProgress(bestAttempt == null ? 0f : bestAttempt.getProgress());
      report.setStatus(bestAttempt == null
          ? ""
          : bestAttempt.getReport().getStateString());

      for (TaskAttempt attempt : attempts.values()) {
        if (TaskAttemptState.RUNNING.equals(attempt.getState())) {
          report.addRunningAttempt(attempt.getID());
        }
      }

      report.setSuccessfulAttempt(successfulAttempt);
      
      for (TaskAttempt att : attempts.values()) {
        String prefix = "AttemptID:" + att.getID() + " Info:";
        for (CharSequence cs : att.getDiagnostics()) {
          report.addDiagnostics(prefix + cs);
          
        }
      }

      // Add a copy of counters as the last step so that their lifetime on heap
      // is as small as possible.
      report.setCounters(TypeConverter.toYarn(bestAttempt == null
          ? TaskAttemptImpl.EMPTY_COUNTERS
          : bestAttempt.getCounters()));

      return report;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Counters getCounters() {
    Counters counters = null;
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      if (bestAttempt != null) {
        counters = bestAttempt.getCounters();
      } else {
        counters = TaskAttemptImpl.EMPTY_COUNTERS;
//        counters.groups = new HashMap<CharSequence, CounterGroup>();
      }
      return counters;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public float getProgress() {
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      if (bestAttempt == null) {
        return 0f;
      }
      return bestAttempt.getProgress();
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public TaskStateInternal getInternalState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  private static TaskState getExternalState(TaskStateInternal smState) {
    if (smState == TaskStateInternal.KILL_WAIT) {
      return TaskState.KILLED;
    } else {
      return TaskState.valueOf(smState.name());
    }
  }

  //this is always called in read/write lock
  private long getLaunchTime() {
    long taskLaunchTime = 0;
    boolean launchTimeSet = false;
    for (TaskAttempt at : attempts.values()) {
      // select the least launch time of all attempts
      long attemptLaunchTime = at.getLaunchTime();
      if (attemptLaunchTime != 0 && !launchTimeSet) {
        // For the first non-zero launch time
        launchTimeSet = true;
        taskLaunchTime = attemptLaunchTime;
      } else if (attemptLaunchTime != 0 && taskLaunchTime > attemptLaunchTime) {
        taskLaunchTime = attemptLaunchTime;
      }
    }
    if (!launchTimeSet) {
      return this.scheduledTime;
    }
    return taskLaunchTime;
  }

  //this is always called in read/write lock
  //TODO Verify behaviour is Task is killed (no finished attempt)
  private long getFinishTime() {
    if (!isFinished()) {
      return 0;
    }
    long finishTime = 0;
    for (TaskAttempt at : attempts.values()) {
      //select the max finish time of all attempts
      if (finishTime < at.getFinishTime()) {
        finishTime = at.getFinishTime();
      }
    }
    return finishTime;
  }

  private long getFinishTime(TaskAttemptId taId) {
    if (taId == null) {
      return clock.getTime();
    }
    long finishTime = 0;
    for (TaskAttempt at : attempts.values()) {
      //select the max finish time of all attempts
      if (at.getID().equals(taId)) {
        return at.getFinishTime();
      }
    }
    return finishTime;
  }
  
  private TaskStateInternal finished(TaskStateInternal finalState) {
    if (getInternalState() == TaskStateInternal.RUNNING) {
      metrics.endRunningTask(this);
    }
    return finalState;
  }

  //select the nextAttemptNumber with best progress
  // always called inside the Read Lock
  private TaskAttempt selectBestAttempt() {
    if (successfulAttempt != null) {
      return attempts.get(successfulAttempt);
    }

    float progress = 0f;
    TaskAttempt result = null;
    for (TaskAttempt at : attempts.values()) {
      switch (at.getState()) {
      
      // ignore all failed task attempts
      case FAILED: 
      case KILLED:
        continue;      
      }      
      if (result == null) {
        result = at; //The first time around
      }
      // calculate the best progress
      float attemptProgress = at.getProgress();
      if (attemptProgress > progress) {
        result = at;
        progress = attemptProgress;
      }
    }
    return result;
  }

  @Override
  public boolean canCommit(TaskAttemptId taskAttemptID) {
    readLock.lock();
    boolean canCommit = false;
    try {
      if (commitAttempt != null) {
        canCommit = taskAttemptID.equals(commitAttempt);
        LOG.info("Result of canCommit for " + taskAttemptID + ":" + canCommit);
      }
    } finally {
      readLock.unlock();
    }
    return canCommit;
  }

  protected abstract TaskAttemptImpl createAttempt();

  // No override of this method may require that the subclass be initialized.
  protected abstract int getMaxAttempts();

  protected TaskAttempt getSuccessfulAttempt() {
    readLock.lock();
    try {
      if (null == successfulAttempt) {
        return null;
      }
      return attempts.get(successfulAttempt);
    } finally {
      readLock.unlock();
    }
  }

  // This is always called in the Write Lock
  private void addAndScheduleAttempt(Avataar avataar) {
    addAndScheduleAttempt(avataar, false);
  }

  // This is always called in the Write Lock
  private void addAndScheduleAttempt(Avataar avataar, boolean reschedule) {
    TaskAttempt attempt = addAttempt(avataar);
    inProgressAttempts.add(attempt.getID());
    //schedule the nextAttemptNumber
    if (failedAttempts.size() > 0 || reschedule) {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
          TaskAttemptEventType.TA_RESCHEDULE));
    } else {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
          TaskAttemptEventType.TA_SCHEDULE));
    }
  }

  private TaskAttemptImpl addAttempt(Avataar avataar) {
    TaskAttemptImpl attempt = createAttempt();
    attempt.setAvataar(avataar);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created attempt " + attempt.getID());
    }
    switch (attempts.size()) {
      case 0:
        attempts = Collections.singletonMap(attempt.getID(),
            (TaskAttempt) attempt);
        break;
        
      case 1:
        Map<TaskAttemptId, TaskAttempt> newAttempts
            = new LinkedHashMap<TaskAttemptId, TaskAttempt>(maxAttempts);
        newAttempts.putAll(attempts);
        attempts = newAttempts;
        attempts.put(attempt.getID(), attempt);
        break;

      default:
        attempts.put(attempt.getID(), attempt);
        break;
    }

    ++nextAttemptNumber;
    return attempt;
  }

  @Override
  public void handle(TaskEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskID() + " of type "
          + event.getType());
    }
    try {
      writeLock.lock();
      TaskStateInternal oldState = getInternalState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.error("Can't handle this event at current state for "
            + this.taskId, e);
        internalError(event.getType());
      }
      if (oldState != getInternalState()) {
        LOG.info(taskId + " Task Transitioned from " + oldState + " to "
            + getInternalState());
      }

    } finally {
      writeLock.unlock();
    }
  }

  protected void internalError(TaskEventType type) {
    LOG.error("Invalid event " + type + " on Task " + this.taskId);
    eventHandler.handle(new JobDiagnosticsUpdateEvent(
        this.taskId.getJobId(), "Invalid event " + type + 
        " on Task " + this.taskId));
    eventHandler.handle(new JobEvent(this.taskId.getJobId(),
        JobEventType.INTERNAL_ERROR));
  }

  // always called inside a transition, in turn inside the Write Lock
  private void handleTaskAttemptCompletion(TaskAttemptId attemptId,
      TaskAttemptCompletionEventStatus status) {
    TaskAttempt attempt = attempts.get(attemptId);
    //raise the completion event only if the container is assigned
    // to nextAttemptNumber
    if (attempt.getNodeHttpAddress() != null) {
      TaskAttemptCompletionEvent tce = recordFactory
          .newRecordInstance(TaskAttemptCompletionEvent.class);
      tce.setEventId(-1);
      String scheme = (encryptedShuffle) ? "https://" : "http://";
      tce.setMapOutputServerAddress(StringInterner.weakIntern(scheme
         + attempt.getNodeHttpAddress().split(":")[0] + ":"
         + attempt.getShufflePort()));
      tce.setStatus(status);
      tce.setAttemptId(attempt.getID());
      int runTime = 0;
      if (attempt.getFinishTime() != 0 && attempt.getLaunchTime() !=0)
        runTime = (int)(attempt.getFinishTime() - attempt.getLaunchTime());
      tce.setAttemptRunTime(runTime);
      
      //raise the event to job so that it adds the completion event to its
      //data structures
      eventHandler.handle(new JobTaskAttemptCompletedEvent(tce));
    }
  }

  private void sendTaskStartedEvent() {
    launchTime = getLaunchTime();
    TaskStartedEvent tse = new TaskStartedEvent(
        TypeConverter.fromYarn(taskId), launchTime,
        TypeConverter.fromYarn(taskId.getTaskType()),
        getSplitsAsString());
    eventHandler
        .handle(new JobHistoryEvent(taskId.getJobId(), tse));
    historyTaskStartGenerated = true;
  }

  private static TaskFinishedEvent createTaskFinishedEvent(TaskImpl task,
      TaskStateInternal taskState) {
    TaskFinishedEvent tfe =
      new TaskFinishedEvent(TypeConverter.fromYarn(task.taskId),
        TypeConverter.fromYarn(task.successfulAttempt),
        task.getFinishTime(task.successfulAttempt),
        TypeConverter.fromYarn(task.taskId.getTaskType()),
        taskState.toString(), task.getCounters(), task.launchTime);
    return tfe;
  }
  
  private static TaskFailedEvent createTaskFailedEvent(TaskImpl task,
      List<String> diag, TaskStateInternal taskState, TaskAttemptId taId) {
    StringBuilder errorSb = new StringBuilder();
    if (diag != null) {
      for (String d : diag) {
        errorSb.append(", ").append(d);
      }
    }
    TaskFailedEvent taskFailedEvent = new TaskFailedEvent(
        TypeConverter.fromYarn(task.taskId),
     // Hack since getFinishTime needs isFinished to be true and that doesn't happen till after the transition.
        task.getFinishTime(taId),
        TypeConverter.fromYarn(task.getType()),
        errorSb.toString(),
        taskState.toString(),
        taId == null ? null : TypeConverter.fromYarn(taId),
        task.getCounters(), task.launchTime);
    return taskFailedEvent;
  }
  
  private static void unSucceed(TaskImpl task) {
    task.commitAttempt = null;
    task.successfulAttempt = null;
  }

  private void sendTaskSucceededEvents() {
    eventHandler.handle(new JobTaskEvent(taskId, TaskState.SUCCEEDED));
    LOG.info("Task succeeded with attempt " + successfulAttempt);
    if (historyTaskStartGenerated) {
      TaskFinishedEvent tfe = createTaskFinishedEvent(this,
          TaskStateInternal.SUCCEEDED);
      eventHandler.handle(new JobHistoryEvent(taskId.getJobId(), tfe));
    }
  }

  /**
  * @return a String representation of the splits.
  *
  * Subclasses can override this method to provide their own representations
  * of splits (if any).
  *
  */
  protected String getSplitsAsString(){
	  return "";
  }

  /**
   * Recover a completed task from a previous application attempt
   * @param taskInfo recovered info about the task
   * @param recoverTaskOutput whether to recover task outputs
   * @return state of the task after recovery
   */
  private TaskStateInternal recover(TaskInfo taskInfo,
      OutputCommitter committer, boolean recoverTaskOutput) {
    LOG.info("Recovering task " + taskId
        + " from prior app attempt, status was " + taskInfo.getTaskStatus());

    scheduledTime = taskInfo.getStartTime();
    sendTaskStartedEvent();
    Collection<TaskAttemptInfo> attemptInfos =
        taskInfo.getAllTaskAttempts().values();

    if (attemptInfos.size() > 0) {
      metrics.launchedTask(this);
    }

    // recover the attempts for this task in the order they finished
    // so task attempt completion events are ordered properly
    int savedNextAttemptNumber = nextAttemptNumber;
    ArrayList<TaskAttemptInfo> taInfos =
        new ArrayList<TaskAttemptInfo>(taskInfo.getAllTaskAttempts().values());
    Collections.sort(taInfos, TA_INFO_COMPARATOR);
    for (TaskAttemptInfo taInfo : taInfos) {
      nextAttemptNumber = taInfo.getAttemptId().getId();
      TaskAttemptImpl attempt = addAttempt(Avataar.VIRGIN);
      // handle the recovery inline so attempts complete before task does
      attempt.handle(new TaskAttemptRecoverEvent(attempt.getID(), taInfo,
          committer, recoverTaskOutput));
      finishedAttempts.add(attempt.getID());
      TaskAttemptCompletionEventStatus taces = null;
      TaskAttemptState attemptState = attempt.getState();
      switch (attemptState) {
      case FAILED:
        taces = TaskAttemptCompletionEventStatus.FAILED;
        break;
      case KILLED:
        taces = TaskAttemptCompletionEventStatus.KILLED;
        break;
      case SUCCEEDED:
        taces = TaskAttemptCompletionEventStatus.SUCCEEDED;
        break;
      default:
        throw new IllegalStateException(
            "Unexpected attempt state during recovery: " + attemptState);
      }
      if (attemptState == TaskAttemptState.FAILED) {
        failedAttempts.add(attempt.getID());
        if (failedAttempts.size() >= maxAttempts) {
          taces = TaskAttemptCompletionEventStatus.TIPFAILED;
        }
      }

      // don't clobber the successful attempt completion event
      // TODO: this shouldn't be necessary after MAPREDUCE-4330
      if (successfulAttempt == null) {
        handleTaskAttemptCompletion(attempt.getID(), taces);
        if (attemptState == TaskAttemptState.SUCCEEDED) {
          successfulAttempt = attempt.getID();
        }
      }
    }
    nextAttemptNumber = savedNextAttemptNumber;

    TaskStateInternal taskState = TaskStateInternal.valueOf(
        taskInfo.getTaskStatus());
    switch (taskState) {
    case SUCCEEDED:
      if (successfulAttempt != null) {
        sendTaskSucceededEvents();
      } else {
        LOG.info("Missing successful attempt for task " + taskId
            + ", recovering as RUNNING");
        // there must have been a fetch failure and the retry wasn't complete
        taskState = TaskStateInternal.RUNNING;
        metrics.runningTask(this);
        addAndScheduleAttempt(Avataar.VIRGIN);
      }
      break;
    case FAILED:
    case KILLED:
    {
      if (taskState == TaskStateInternal.KILLED && attemptInfos.size() == 0) {
        metrics.endWaitingTask(this);
      }
      TaskFailedEvent tfe = new TaskFailedEvent(taskInfo.getTaskId(),
          taskInfo.getFinishTime(), taskInfo.getTaskType(),
          taskInfo.getError(), taskInfo.getTaskStatus(),
          taskInfo.getFailedDueToAttemptId(), taskInfo.getCounters(),
          launchTime);
      eventHandler.handle(new JobHistoryEvent(taskId.getJobId(), tfe));
      eventHandler.handle(
          new JobTaskEvent(taskId, getExternalState(taskState)));
      break;
    }
    default:
      throw new java.lang.AssertionError("Unexpected recovered task state: "
          + taskState);
    }

    return taskState;
  }

  private static class RecoverTransition
    implements MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskRecoverEvent tre = (TaskRecoverEvent) event;
      return task.recover(tre.getTaskInfo(), tre.getOutputCommitter(),
          tre.getRecoverTaskOutput());
    }
  }

  private static class InitialScheduleTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {

    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      task.addAndScheduleAttempt(Avataar.VIRGIN);
      task.scheduledTime = task.clock.getTime();
      task.sendTaskStartedEvent();
    }
  }

  // Used when creating a new attempt while one is already running.
  //  Currently we do this for speculation.  In the future we may do this
  //  for tasks that failed in a way that might indicate application code
  //  problems, so we can take later failures in parallel and flush the
  //  job quickly when this happens.
  private static class RedundantScheduleTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {

    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      LOG.info("Scheduling a redundant attempt for task " + task.taskId);
      task.addAndScheduleAttempt(Avataar.SPECULATIVE);
    }
  }

  private static class AttemptCommitPendingTransition 
          implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent ev = (TaskTAttemptEvent) event;
      // The nextAttemptNumber is commit pending, decide on set the commitAttempt
      TaskAttemptId attemptID = ev.getTaskAttemptID();
      if (task.commitAttempt == null) {
        // TODO: validate attemptID
        task.commitAttempt = attemptID;
        LOG.info(attemptID + " given a go for committing the task output.");
      } else {
        // Don't think this can be a pluggable decision, so simply raise an
        // event for the TaskAttempt to delete its output.
        LOG.info(task.commitAttempt
            + " already given a go for committing the task output, so killing "
            + attemptID);
        task.eventHandler.handle(new TaskAttemptKillEvent(attemptID,
            SPECULATION + task.commitAttempt + " committed first!"));
      }
    }
  }

  private static class AttemptSucceededTransition 
      implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent taskTAttemptEvent = (TaskTAttemptEvent) event;
      TaskAttemptId taskAttemptId = taskTAttemptEvent.getTaskAttemptID();
      task.handleTaskAttemptCompletion(
          taskAttemptId, 
          TaskAttemptCompletionEventStatus.SUCCEEDED);
      task.finishedAttempts.add(taskAttemptId);
      task.inProgressAttempts.remove(taskAttemptId);
      task.successfulAttempt = taskAttemptId;
      task.sendTaskSucceededEvents();
      for (TaskAttempt attempt : task.attempts.values()) {
        if (attempt.getID() != task.successfulAttempt &&
            // This is okay because it can only talk us out of sending a
            //  TA_KILL message to an attempt that doesn't need one for
            //  other reasons.
            !attempt.isFinished()) {
          LOG.info("Issuing kill to other attempt " + attempt.getID());
          task.eventHandler.handle(new TaskAttemptKillEvent(attempt.getID(),
              SPECULATION + task.successfulAttempt + " succeeded first!"));
        }
      }
      task.finished(TaskStateInternal.SUCCEEDED);
    }
  }

  private static class AttemptKilledTransition implements
      SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskAttemptId taskAttemptId =
          ((TaskTAttemptEvent) event).getTaskAttemptID();
      task.handleTaskAttemptCompletion(
          taskAttemptId, 
          TaskAttemptCompletionEventStatus.KILLED);
      task.finishedAttempts.add(taskAttemptId);
      task.inProgressAttempts.remove(taskAttemptId);
      if (task.successfulAttempt == null) {
        boolean rescheduleNewAttempt = false;
        if (event instanceof TaskTAttemptKilledEvent) {
          rescheduleNewAttempt =
              ((TaskTAttemptKilledEvent)event).getRescheduleAttempt();
        }
        task.addAndScheduleAttempt(Avataar.VIRGIN, rescheduleNewAttempt);
      }
      if ((task.commitAttempt != null) && (task.commitAttempt == taskAttemptId)) {
    	task.commitAttempt = null;
      }
    }
  }


  private static class KillWaitAttemptKilledTransition implements
      MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    protected TaskStateInternal finalState = TaskStateInternal.KILLED;
    protected final TaskAttemptCompletionEventStatus taCompletionEventStatus;

    public KillWaitAttemptKilledTransition() {
      this(TaskAttemptCompletionEventStatus.KILLED);
    }

    public KillWaitAttemptKilledTransition(
        TaskAttemptCompletionEventStatus taCompletionEventStatus) {
      this.taCompletionEventStatus = taCompletionEventStatus;
    }

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskAttemptId taskAttemptId =
          ((TaskTAttemptEvent) event).getTaskAttemptID();
      task.handleTaskAttemptCompletion(taskAttemptId, taCompletionEventStatus);
      task.finishedAttempts.add(taskAttemptId);
      // check whether all attempts are finished
      if (task.finishedAttempts.size() == task.attempts.size()) {
        if (task.historyTaskStartGenerated) {
        TaskFailedEvent taskFailedEvent = createTaskFailedEvent(task, null,
              finalState, null); // TODO JH verify failedAttempt null
        task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
            taskFailedEvent)); 
        } else {
          LOG.debug("Not generating HistoryFinish event since start event not" +
          		" generated for task: " + task.getID());
        }

        task.eventHandler.handle(
            new JobTaskEvent(task.taskId, getExternalState(finalState)));
        return finalState;
      }
      return task.getInternalState();
    }
  }

  private static class KillWaitAttemptSucceededTransition extends
      KillWaitAttemptKilledTransition {
    public KillWaitAttemptSucceededTransition() {
      super(TaskAttemptCompletionEventStatus.SUCCEEDED);
    }
  }

  private static class KillWaitAttemptFailedTransition extends
      KillWaitAttemptKilledTransition {
    public KillWaitAttemptFailedTransition() {
      super(TaskAttemptCompletionEventStatus.FAILED);
    }
  }

  private static class AttemptFailedTransition implements
    MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptFailedEvent castEvent = (TaskTAttemptFailedEvent) event;
      TaskAttemptId taskAttemptId = castEvent.getTaskAttemptID();
      task.failedAttempts.add(taskAttemptId); 
      if (taskAttemptId.equals(task.commitAttempt)) {
        task.commitAttempt = null;
      }
      TaskAttempt attempt = task.attempts.get(taskAttemptId);
      if (attempt.getAssignedContainerMgrAddress() != null) {
        //container was assigned
        task.eventHandler.handle(new ContainerFailedEvent(attempt.getID(), 
            attempt.getAssignedContainerMgrAddress()));
      }
      
      task.finishedAttempts.add(taskAttemptId);
      if (!castEvent.isFastFail()
          && task.failedAttempts.size() < task.maxAttempts) {
        task.handleTaskAttemptCompletion(
            taskAttemptId, 
            TaskAttemptCompletionEventStatus.FAILED);
        // we don't need a new event if we already have a spare
        task.inProgressAttempts.remove(taskAttemptId);
        if (task.successfulAttempt == null) {
          boolean shouldAddNewAttempt = true;
          if (task.inProgressAttempts.size() > 0) {
            // if not all of the inProgressAttempts are hanging for resource
            for (TaskAttemptId attemptId : task.inProgressAttempts) {
              if (((TaskAttemptImpl) task.getAttempt(attemptId))
                  .isContainerAssigned()) {
                shouldAddNewAttempt = false;
                break;
              }
            }
          }
          if (shouldAddNewAttempt) {
            task.addAndScheduleAttempt(Avataar.VIRGIN);
          }
        }
      } else {
        task.handleTaskAttemptCompletion(
            taskAttemptId, 
            TaskAttemptCompletionEventStatus.TIPFAILED);

        // issue kill to all non finished attempts
        for (TaskAttempt taskAttempt : task.attempts.values()) {
          task.killUnfinishedAttempt
            (taskAttempt, "Task has failed. Killing attempt!");
        }
        task.inProgressAttempts.clear();
        
        if (task.historyTaskStartGenerated) {
        TaskFailedEvent taskFailedEvent = createTaskFailedEvent(task, attempt.getDiagnostics(),
            TaskStateInternal.FAILED, taskAttemptId);
        task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
            taskFailedEvent));
        } else {
          LOG.debug("Not generating HistoryFinish event since start event not" +
              " generated for task: " + task.getID());
        }
        task.eventHandler.handle(
            new JobTaskEvent(task.taskId, TaskState.FAILED));
        return task.finished(TaskStateInternal.FAILED);
      }
      return getDefaultState(task);
    }

    protected TaskStateInternal getDefaultState(TaskImpl task) {
      return task.getInternalState();
    }
  }

  private static class RetroactiveFailureTransition
      extends AttemptFailedTransition {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
      if (task.getInternalState() == TaskStateInternal.SUCCEEDED &&
          !castEvent.getTaskAttemptID().equals(task.successfulAttempt)) {
        // don't allow a different task attempt to override a previous
        // succeeded state
        task.finishedAttempts.add(castEvent.getTaskAttemptID());
        task.inProgressAttempts.remove(castEvent.getTaskAttemptID());
        return TaskStateInternal.SUCCEEDED;
      }

      // a successful REDUCE task should not be overridden
      //TODO: consider moving it to MapTaskImpl
      if (!TaskType.MAP.equals(task.getType())) {
        LOG.error("Unexpected event for REDUCE task " + event.getType());
        task.internalError(event.getType());
      }
      
      // tell the job about the rescheduling
      task.eventHandler.handle(
          new JobMapTaskRescheduledEvent(task.taskId));
      // super.transition is mostly coded for the case where an
      //  UNcompleted task failed.  When a COMPLETED task retroactively
      //  fails, we have to let AttemptFailedTransition.transition
      //  believe that there's no redundancy.
      unSucceed(task);
      // fake increase in Uncomplete attempts for super.transition
      task.inProgressAttempts.add(castEvent.getTaskAttemptID());
      return super.transition(task, event);
    }

    @Override
    protected TaskStateInternal getDefaultState(TaskImpl task) {
      return TaskStateInternal.SCHEDULED;
    }
  }

  private static class RetroactiveKilledTransition implements
    MultipleArcTransition<TaskImpl, TaskEvent, TaskStateInternal> {

    @Override
    public TaskStateInternal transition(TaskImpl task, TaskEvent event) {
      TaskAttemptId attemptId = null;
      if (event instanceof TaskTAttemptEvent) {
        TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
        attemptId = castEvent.getTaskAttemptID(); 
        if (task.getInternalState() == TaskStateInternal.SUCCEEDED &&
            !attemptId.equals(task.successfulAttempt)) {
          // don't allow a different task attempt to override a previous
          // succeeded state
          task.finishedAttempts.add(castEvent.getTaskAttemptID());
          task.inProgressAttempts.remove(castEvent.getTaskAttemptID());
          return TaskStateInternal.SUCCEEDED;
        }
      }

      // a successful REDUCE task should not be overridden
      // TODO: consider moving it to MapTaskImpl
      if (!TaskType.MAP.equals(task.getType())) {
        LOG.error("Unexpected event for REDUCE task " + event.getType());
        task.internalError(event.getType());
      }

      // successful attempt is now killed. reschedule
      // tell the job about the rescheduling
      unSucceed(task);
      task.handleTaskAttemptCompletion(attemptId,
          TaskAttemptCompletionEventStatus.KILLED);
      task.eventHandler.handle(new JobMapTaskRescheduledEvent(task.taskId));
      // typically we are here because this map task was run on a bad node and
      // we want to reschedule it on a different node.
      // Depending on whether there are previous failed attempts or not this
      // can SCHEDULE or RESCHEDULE the container allocate request. If this
      // SCHEDULE's then the dataLocal hosts of this taskAttempt will be used
      // from the map splitInfo. So the bad node might be sent as a location
      // to the RM. But the RM would ignore that just like it would ignore
      // currently pending container requests affinitized to bad nodes.
      boolean rescheduleNextTaskAttempt = false;
      if (event instanceof TaskTAttemptKilledEvent) {
        // Decide whether to reschedule next task attempt. If true, this
        // typically indicates that a successful map attempt was killed on an
        // unusable node being reported.
        rescheduleNextTaskAttempt =
            ((TaskTAttemptKilledEvent)event).getRescheduleAttempt();
      }
      task.addAndScheduleAttempt(Avataar.VIRGIN, rescheduleNextTaskAttempt);
      return TaskStateInternal.SCHEDULED;
    }
  }

  private static class AttemptSucceededAtSucceededTransition
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
      task.finishedAttempts.add(castEvent.getTaskAttemptID());
      task.inProgressAttempts.remove(castEvent.getTaskAttemptID());
    }
  }

  private static class KillNewTransition 
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      
      if (task.historyTaskStartGenerated) {
      TaskFailedEvent taskFailedEvent = createTaskFailedEvent(task, null,
            TaskStateInternal.KILLED, null); // TODO Verify failedAttemptId is null
      task.eventHandler.handle(new JobHistoryEvent(task.taskId.getJobId(),
          taskFailedEvent));
      }else {
        LOG.debug("Not generating HistoryFinish event since start event not" +
        		" generated for task: " + task.getID());
      }

      task.eventHandler.handle(new JobTaskEvent(task.taskId,
          getExternalState(TaskStateInternal.KILLED)));
      task.metrics.endWaitingTask(task);
    }
  }

  private void killUnfinishedAttempt(TaskAttempt attempt, String logMsg) {
    if (attempt != null && !attempt.isFinished()) {
      eventHandler.handle(
          new TaskAttemptKillEvent(attempt.getID(), logMsg));
    }
  }

  private static class KillTransition 
    implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      // issue kill to all non finished attempts
      for (TaskAttempt attempt : task.attempts.values()) {
        task.killUnfinishedAttempt
            (attempt, "Task KILL is received. Killing attempt!");
      }

      task.inProgressAttempts.clear();
    }
  }

  static class LaunchTransition
      implements SingleArcTransition<TaskImpl, TaskEvent> {
    @Override
    public void transition(TaskImpl task, TaskEvent event) {
      task.metrics.launchedTask(task);
      task.metrics.runningTask(task);
      
    }
  }
}
