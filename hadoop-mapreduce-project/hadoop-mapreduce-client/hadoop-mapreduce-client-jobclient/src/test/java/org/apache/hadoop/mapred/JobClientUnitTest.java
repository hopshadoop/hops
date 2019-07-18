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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class JobClientUnitTest {
  
  public class TestJobClient extends JobClient {

    TestJobClient(JobConf jobConf) throws IOException {
      super(jobConf);
    }

    void setCluster(Cluster cluster) {
      this.cluster = cluster;
    }

  }

  public class TestJobClientGetJob extends TestJobClient {

    int lastGetJobRetriesCounter = 0;
    int getJobRetriesCounter = 0;
    int getJobRetries = 0;
    RunningJob runningJob;

    TestJobClientGetJob(JobConf jobConf) throws IOException {
      super(jobConf);
    }

    public int getLastGetJobRetriesCounter() {
      return lastGetJobRetriesCounter;
    }

    public void setGetJobRetries(int getJobRetries) {
      this.getJobRetries = getJobRetries;
    }

    public void setRunningJob(RunningJob runningJob) {
      this.runningJob = runningJob;
    }

    protected RunningJob getJobInner(final JobID jobid) throws IOException {
      if (getJobRetriesCounter >= getJobRetries) {
        lastGetJobRetriesCounter = getJobRetriesCounter;
        getJobRetriesCounter = 0;
        return runningJob;
      }
      getJobRetriesCounter++;
      return null;
    }

  }

  @Test
  public void testMapTaskReportsWithNullJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getMapTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @Test
  public void testReduceTaskReportsWithNullJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getReduceTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @Test
  public void testSetupTaskReportsWithNullJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getSetupTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }
  
  @Test
  public void testCleanupTaskReportsWithNullJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("test",0);
    
    when(mockCluster.getJob(id)).thenReturn(null);
    
    TaskReport[] result = client.getCleanupTaskReports(id);
    assertEquals(0, result.length);
    
    verify(mockCluster).getJob(id);
  }

  @Test
  public void testShowJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());

    long startTime = System.currentTimeMillis();

    JobID jobID = new JobID(String.valueOf(startTime), 12345);

    JobStatus mockJobStatus = mock(JobStatus.class);
    when(mockJobStatus.getJobID()).thenReturn(jobID);
    when(mockJobStatus.getJobName()).thenReturn(jobID.toString());
    when(mockJobStatus.getState()).thenReturn(JobStatus.State.RUNNING);
    when(mockJobStatus.getStartTime()).thenReturn(startTime);
    when(mockJobStatus.getUsername()).thenReturn("mockuser");
    when(mockJobStatus.getQueue()).thenReturn("mockqueue");
    when(mockJobStatus.getPriority()).thenReturn(JobPriority.NORMAL);
    when(mockJobStatus.getNumUsedSlots()).thenReturn(1);
    when(mockJobStatus.getNumReservedSlots()).thenReturn(1);
    when(mockJobStatus.getUsedMem()).thenReturn(1024);
    when(mockJobStatus.getReservedMem()).thenReturn(512);
    when(mockJobStatus.getNeededMem()).thenReturn(2048);
    when(mockJobStatus.getSchedulingInfo()).thenReturn("NA");

    Job mockJob = mock(Job.class);
    when(mockJob.getTaskReports(isA(TaskType.class))).thenReturn(
      new TaskReport[5]);

    Cluster mockCluster = mock(Cluster.class);
    when(mockCluster.getJob(jobID)).thenReturn(mockJob);

    client.setCluster(mockCluster);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    client.displayJobList(new JobStatus[] {mockJobStatus}, new PrintWriter(out));
    String commandLineOutput = out.toString();
    System.out.println(commandLineOutput);
    Assert.assertTrue(commandLineOutput.contains("Total jobs:1"));

    verify(mockJobStatus, atLeastOnce()).getJobID();
    verify(mockJobStatus).getState();
    verify(mockJobStatus).getStartTime();
    verify(mockJobStatus).getUsername();
    verify(mockJobStatus).getQueue();
    verify(mockJobStatus).getPriority();
    verify(mockJobStatus).getNumUsedSlots();
    verify(mockJobStatus).getNumReservedSlots();
    verify(mockJobStatus).getUsedMem();
    verify(mockJobStatus).getReservedMem();
    verify(mockJobStatus).getNeededMem();
    verify(mockJobStatus).getSchedulingInfo();

    // This call should not go to each AM.
    verify(mockCluster, never()).getJob(jobID);
    verify(mockJob, never()).getTaskReports(isA(TaskType.class));
  }

  @Test
  public void testGetJobWithUnknownJob() throws Exception {
    TestJobClient client = new TestJobClient(new JobConf());
    Cluster mockCluster = mock(Cluster.class);
    client.setCluster(mockCluster);
    JobID id = new JobID("unknown",0);

    when(mockCluster.getJob(id)).thenReturn(null);

    assertNull(client.getJob(id));
  }

  @Test
  public void testGetJobRetry() throws Exception {

    //To prevent the test from running for a very long time, lower the retry
    JobConf conf = new JobConf();
    conf.setInt(MRJobConfig.MR_CLIENT_JOB_MAX_RETRIES, 2);

    TestJobClientGetJob client = new TestJobClientGetJob(conf);
    JobID id = new JobID("ajob", 1);
    RunningJob rj = mock(RunningJob.class);
    client.setRunningJob(rj);

    //no retry
    assertNotNull(client.getJob(id));
    assertEquals(client.getLastGetJobRetriesCounter(), 0);

    //2 retries
    client.setGetJobRetries(2);
    assertNotNull(client.getJob(id));
    assertEquals(client.getLastGetJobRetriesCounter(), 2);

    //beyond yarn.app.mapreduce.client.job.max-retries, will get null
    client.setGetJobRetries(3);
    assertNull(client.getJob(id));
  }

  @Test
  public void testGetJobRetryDefault() throws Exception {

    //To prevent the test from running for a very long time, lower the retry
    JobConf conf = new JobConf();

    TestJobClientGetJob client = new TestJobClientGetJob(conf);
    JobID id = new JobID("ajob", 1);
    RunningJob rj = mock(RunningJob.class);
    client.setRunningJob(rj);

    //3 retries (default)
    client.setGetJobRetries(MRJobConfig.DEFAULT_MR_CLIENT_JOB_MAX_RETRIES);
    assertNotNull(client.getJob(id));
    assertEquals(client.getLastGetJobRetriesCounter(),
        MRJobConfig.DEFAULT_MR_CLIENT_JOB_MAX_RETRIES);

    //beyond yarn.app.mapreduce.client.job.max-retries, will get null
    client.setGetJobRetries(MRJobConfig.DEFAULT_MR_CLIENT_JOB_MAX_RETRIES + 1);
    assertNull(client.getJob(id));
  }

}
