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

package org.apache.hadoop.mapreduce.jobhistory;

/**
 * List of all event type currently supported by Job History module
 *
 */
public  enum EventType {
    JOB_SUBMITTED (JobSubmittedEvent.class),
    JOB_INITED (JobInitedEvent.class),
    JOB_FINISHED (JobFinishedEvent.class),
    JOB_PRIORITY_CHANGED (JobPriorityChangeEvent.class),
    JOB_STATUS_CHANGED (JobStatusChangedEvent.class),
    JOB_FAILED (JobUnsuccessfulCompletionEvent.class),
    JOB_KILLED (JobUnsuccessfulCompletionEvent.class),
    JOB_INFO_CHANGED (JobInfoChangeEvent.class),
    TASK_STARTED (TaskStartedEvent.class),
    TASK_FINISHED (TaskFinishedEvent.class),
    TASK_FAILED (TaskFailedEvent.class),
    TASK_UPDATED (TaskUpdatedEvent.class),
    MAP_ATTEMPT_STARTED (TaskAttemptStartedEvent.class),
    MAP_ATTEMPT_FINISHED (MapAttemptFinishedEvent.class),
    MAP_ATTEMPT_FAILED (TaskAttemptUnsuccessfulCompletionEvent.class), 
    MAP_ATTEMPT_KILLED (TaskAttemptUnsuccessfulCompletionEvent.class),
    REDUCE_ATTEMPT_STARTED (TaskAttemptStartedEvent.class),
    REDUCE_ATTEMPT_FINISHED (ReduceAttemptFinishedEvent.class), 
    REDUCE_ATTEMPT_FAILED (TaskAttemptUnsuccessfulCompletionEvent.class), 
    REDUCE_ATTEMPT_KILLED (TaskAttemptUnsuccessfulCompletionEvent.class),
    SETUP_ATTEMPT_STARTED (TaskAttemptStartedEvent.class),
    SETUP_ATTEMPT_FINISHED (TaskAttemptFinishedEvent.class), 
    SETUP_ATTEMPT_FAILED (TaskAttemptUnsuccessfulCompletionEvent.class), 
    SETUP_ATTEMPT_KILLED (TaskAttemptUnsuccessfulCompletionEvent.class),
    CLEANUP_ATTEMPT_STARTED (TaskAttemptStartedEvent.class),
    CLEANUP_ATTEMPT_FINISHED (TaskAttemptFinishedEvent.class), 
    CLEANUP_ATTEMPT_FAILED (TaskAttemptUnsuccessfulCompletionEvent.class), 
    CLEANUP_ATTEMPT_KILLED (TaskAttemptUnsuccessfulCompletionEvent.class); 
   
    Class<? extends HistoryEvent> klass;
   
    EventType(Class< ? extends HistoryEvent> klass) {
      this.klass = klass;
    }
   
    Class<? extends HistoryEvent> getKlass() {
      return this.klass;
    }
  }
