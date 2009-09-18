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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

public class TestGridmixSubmission {
  {
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.mapred.gridmix")
        ).getLogger().setLevel(Level.DEBUG);
  }

  private static FileSystem dfs = null;
  private static MiniDFSCluster dfsCluster = null;
  private static MiniMRCluster mrCluster = null;

  private static final int NJOBS = 2;
  private static final long GENDATA = 50; // in megabytes
  private static final int GENSLOP = 100 * 1024; // +/- 100k for logs

  @BeforeClass
  public static void initCluster() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(JTConfig.JT_RETIREJOBS, false);
    dfsCluster = new MiniDFSCluster(conf, 3, true, null);
    dfs = dfsCluster.getFileSystem();
    mrCluster = new MiniMRCluster(3, dfs.getUri().toString(), 1);
  }

  @AfterClass
  public static void shutdownCluster() throws IOException {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  static class TestMonitor extends JobMonitor {

    private final int expected;
    private final BlockingQueue<Job> retiredJobs;

    public TestMonitor(int expected) {
      super();
      this.expected = expected;
      retiredJobs = new LinkedBlockingQueue<Job>();
    }

    public void verify(ArrayList<JobStory> submitted) {
      final ArrayList<Job> succeeded = new ArrayList<Job>();
      assertEquals("Bad job count", expected, retiredJobs.drainTo(succeeded));
    }

    @Override
    protected void onSuccess(Job job) {
      retiredJobs.add(job);
    }
    @Override
    protected void onFailure(Job job) {
      fail("Job failure: " + job);
    }
  }

  static class DebugGridmix extends Gridmix {

    private DebugJobFactory factory;
    private TestMonitor monitor;

    public void checkMonitor() {
      monitor.verify(factory.getSubmitted());
    }

    @Override
    protected JobMonitor createJobMonitor() {
      monitor = new TestMonitor(NJOBS + 1); // include data generation job
      return monitor;
    }

    @Override
    protected JobFactory createJobFactory(JobSubmitter submitter,
        String traceIn, Path scratchDir, Configuration conf,
        CountDownLatch startFlag) throws IOException {
      factory =
        new DebugJobFactory(submitter, scratchDir, NJOBS, conf, startFlag);
      return factory;
    }
  }

  @Test
  public void testSubmit() throws Exception {
    final Path in = new Path("foo").makeQualified(dfs);
    final Path out = new Path("/gridmix").makeQualified(dfs);
    final String[] argv = {
      "-D" + FilePool.GRIDMIX_MIN_FILE + "=0",
      "-D" + Gridmix.GRIDMIX_OUT_DIR + "=" + out,
      "-generate", String.valueOf(GENDATA) + "m",
      in.toString(),
      "-" // ignored by DebugGridmix
    };
    DebugGridmix client = new DebugGridmix();
    final Configuration conf = mrCluster.createJobConf();
    int res = ToolRunner.run(conf, client, argv);
    assertEquals("Client exited with nonzero status", 0, res);
    client.checkMonitor();
    final ContentSummary generated = dfs.getContentSummary(in);
    assertTrue("Mismatched data gen", // +/- 100k for logs
        (GENDATA << 20) < generated.getLength() + GENSLOP ||
        (GENDATA << 20) > generated.getLength() - GENSLOP);
    FileStatus[] outstat = dfs.listStatus(out);
    assertEquals("Mismatched job count", NJOBS, outstat.length);
  }

}
