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

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

import junit.framework.TestCase;

/**
 * Test various jobhistory events
 */
public class TestJobHistoryEvents extends TestCase {
  /**
   * Test TaskAttemptStartedEvent.
   */
  public void testTaskAttemptStartedEvent() {
    EventType expected = EventType.MAP_ATTEMPT_STARTED;
    TaskAttemptID fakeId = new TaskAttemptID("1234", 1, TaskType.MAP, 1, 1);
    
    // check jobsetup type
    TaskAttemptStartedEvent tase = 
      new TaskAttemptStartedEvent(fakeId, TaskType.JOB_SETUP, 0L, "", 0);
    assertEquals(expected, tase.getEventType());
    
    // check jobcleanup type
    tase = new TaskAttemptStartedEvent(fakeId, TaskType.JOB_CLEANUP, 0L, "", 0);
    assertEquals(expected, tase.getEventType());
    
    // check map type
    tase = new TaskAttemptStartedEvent(fakeId, TaskType.MAP, 0L, "", 0);
    assertEquals(expected, tase.getEventType());
    
    expected = EventType.REDUCE_ATTEMPT_STARTED;
    fakeId = new TaskAttemptID("1234", 1, TaskType.REDUCE, 1, 1);
    
    // check jobsetup type
    tase = new TaskAttemptStartedEvent(fakeId, TaskType.JOB_SETUP, 0L, "", 0);
    assertEquals(expected, tase.getEventType());
    
    // check jobcleanup type
    tase = new TaskAttemptStartedEvent(fakeId, TaskType.JOB_CLEANUP, 0L, "", 0);
    assertEquals(expected, tase.getEventType());
    
    // check reduce type
    tase = new TaskAttemptStartedEvent(fakeId, TaskType.REDUCE, 0L, "", 0);
    assertEquals(expected, tase.getEventType());
  }
}
