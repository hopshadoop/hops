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

package io.hops.erasure_coding;

import io.hops.metadata.hdfs.entity.EncodingJob;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class MapReduceEncoder {

  protected static final Log LOG = LogFactory.getLog(MapReduceEncoder.class);

  static final String NAME = "mapReduceEncoder";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String OP_LIST_LABEL = NAME + ".op.list";
  static final String OP_COUNT_LABEL = NAME + ".op.count";
  static final String SCHEDULER_OPTION_LABEL = NAME + ".scheduleroption";
  static final String IGNORE_FAILURES_OPTION_LABEL = NAME + ".ignore.failures";

  private static final long OP_PER_MAP = 100;
  private static final int MAX_MAPS_PER_NODE = 20;
  private static final SimpleDateFormat dateForm =
      new SimpleDateFormat("yyyy-MM-dd HH:mm");
  private static String jobName = NAME;

  public static enum Counter {
    FILES_SUCCEEDED,
    FILES_FAILED,
    PROCESSED_BLOCKS,
    PROCESSED_SIZE,
    META_BLOCKS,
    META_SIZE,
    SAVING_SIZE
  }

  protected JobConf jobconf;

  /**
   * {@inheritDoc}
   */
  public void setConf(Configuration conf) {
    if (jobconf != conf) {
      jobconf = conf instanceof JobConf ? (JobConf) conf : new JobConf(conf);
    }
  }

  /**
   * {@inheritDoc}
   */
  public JobConf getConf() {
    return jobconf;
  }

  public MapReduceEncoder(Configuration conf) {
    setConf(createJobConf(conf));
  }

  /**
   * Recovery constructor for NameNode failures.
   *
   * The job start time is currently not recovered. This means that jobs might
   * need more time to timeout after recovery.
   *
   * @param job the job to recover
   */
  MapReduceEncoder(Configuration conf, EncodingJob job) throws IOException {
    jobconf = new JobConf(conf);
    jobconf.set(JOB_DIR_LABEL, job.getJobDir());
    JobID jobID = new JobID(job.getJtIdentifier(), job.getJobId());
    JobClient jobClient = new JobClient(jobconf);
    runningJob = jobClient.getJob(jobID);
    if (runningJob == null) {
      throw new IOException("Failed to recover");
    }
  }

  private static final Random RANDOM = new Random();

  protected static String getRandomId() {
    return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
  }

  private JobClient jobClient;
  private RunningJob runningJob;
  private int jobEventCounter = 0;
  private String lastReport = null;
  private long startTime = System.currentTimeMillis();

  /**
   * Responsible for generating splits of the src file list.
   */
  static class DistRaidInputFormat implements InputFormat<Text, PolicyInfo> {
    /**
     * Do nothing.
     */
    public void validateInput(JobConf job) {
    }

    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     *
     * @param job
     *     The handle to the JobConf object
     * @param numSplits
     *     Number of splits requested
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      final int srcCount = job.getInt(OP_COUNT_LABEL, -1);
      final int targetcount = srcCount / numSplits;
      String srclist = job.get(OP_LIST_LABEL, "");
      if (srcCount < 0 || "".equals(srclist)) {
        throw new RuntimeException(
            "Invalid metadata: #files(" + srcCount + ") listuri(" + srclist +
                ")");
      }
      Path srcs = new Path(srclist);
      FileSystem fs = srcs.getFileSystem(job);

      List<FileSplit> splits = new ArrayList<FileSplit>(numSplits);

      Text key = new Text();
      PolicyInfo value = new PolicyInfo();
      SequenceFile.Reader in = null;
      long prev = 0L;
      int count = 0; // count src
      try {
        for (in = new SequenceFile.Reader(fs, srcs, job);
             in.next(key, value); ) {
          long curr = in.getPosition();
          long delta = curr - prev;
          if (++count > targetcount) {
            count = 0;
            splits.add(new FileSplit(srcs, prev, delta, (String[]) null));
            prev = curr;
          }
        }
      } finally {
        in.close();
      }
      long remaining = fs.getFileStatus(srcs).getLen() - prev;
      if (remaining != 0) {
        splits.add(new FileSplit(srcs, prev, remaining, (String[]) null));
      }
      LOG.info("jobname= " + jobName + " numSplits=" + numSplits +
          ", splits.size()=" + splits.size());
      return splits.toArray(new FileSplit[splits.size()]);
    }

    /**
     * {@inheritDoc}
     */
    public RecordReader<Text, PolicyInfo> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, PolicyInfo>(job,
          (FileSplit) split);
    }
  }

  /**
   * The mapper for raiding files.
   */
  static class DistRaidMapper
      implements Mapper<Text, PolicyInfo, WritableComparable, Text> {
    private JobConf jobconf;
    private boolean ignoreFailures;

    private int failcount = 0;
    private int succeedcount = 0;
    private BaseEncodingManager.Statistics st = null;
    private Reporter reporter = null;

    private String getCountString() {
      return "Succeeded: " + succeedcount + " Failed: " + failcount;
    }

    /**
     * {@inheritDoc}
     */
    public void configure(JobConf job) {
      this.jobconf = job;
      ignoreFailures = jobconf.getBoolean(IGNORE_FAILURES_OPTION_LABEL, true);
      st = new BaseEncodingManager.Statistics();
    }

    /**
     * Run a FileOperation
     */
    public void map(Text key, PolicyInfo policy,
        OutputCollector<WritableComparable, Text> out, Reporter reporter)
        throws IOException {
      this.reporter = reporter;
      try {
        Codec.initializeCodecs(jobconf);

        LOG.info("Raiding file=" + key.toString() + " policy=" + policy);
        Path p = new Path(key.toString());
        st.clear();
        BaseEncodingManager.doRaid(jobconf, policy, p, st, reporter);

        ++succeedcount;

        reporter.incrCounter(Counter.PROCESSED_BLOCKS, st.numProcessedBlocks);
        reporter.incrCounter(Counter.PROCESSED_SIZE, st.processedSize);
        reporter.incrCounter(Counter.META_BLOCKS, st.numMetaBlocks);
        reporter.incrCounter(Counter.META_SIZE, st.metaSize);
        reporter.incrCounter(Counter.SAVING_SIZE,
            st.processedSize - st.remainingSize - st.metaSize);
        reporter.incrCounter(Counter.FILES_SUCCEEDED, 1);
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FILES_FAILED, 1);

        String s = "FAIL: " + policy + ", " + key + " " +
            StringUtils.stringifyException(e);
        out.collect(null, new Text(s));
        LOG.info(s);
      } finally {
        reporter.setStatus(getCountString());
      }
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws IOException {
      if (failcount == 0 || ignoreFailures) {
        return;
      }
      throw new IOException(getCountString());
    }
  }

  /**
   * create new job conf based on configuration passed.
   *
   * @param conf
   * @return
   */
  private static JobConf createJobConf(Configuration conf) {
    JobConf jobconf = new JobConf(conf, MapReduceEncoder.class);
    jobName = NAME + " " + dateForm.format(new Date(BaseEncodingManager.now()));
    jobconf.setUser(BaseEncodingManager.JOBUSER);
    jobconf.setJobName(jobName);
    jobconf.setMapSpeculativeExecution(false);
    RaidUtils.parseAndSetOptions(jobconf, SCHEDULER_OPTION_LABEL);

    jobconf.setJarByClass(MapReduceEncoder.class);
    jobconf.setInputFormat(DistRaidInputFormat.class);
    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setMapperClass(DistRaidMapper.class);
    jobconf.setNumReduceTasks(0);
    return jobconf;
  }

  /**
   * Calculate how many maps to run.
   */
  private static int getMapCount(int srcCount) {
    int numMaps = (int) (srcCount / OP_PER_MAP);
    return Math.max(numMaps, MAX_MAPS_PER_NODE);
  }

  /**
   * Invokes a map-reduce job do parallel raiding.
   *
   * @return true if the job was started, false otherwise
   */
  public boolean startDistRaid(PolicyInfo info) throws IOException {
    if (prepareJob(info)) {
      this.jobClient = new JobClient(jobconf);
      this.runningJob = this.jobClient.submitJob(jobconf);
      LOG.info("Job Started: " + runningJob.getID());
      this.startTime = System.currentTimeMillis();
      return true;
    }
    return false;
  }
  
  /**
   * Checks if the map-reduce job has completed.
   *
   * @return true if the job completed, false otherwise.
   * @throws java.io.IOException
   */
  public boolean checkComplete() throws IOException {
    JobID jobID = runningJob.getID();
    if (runningJob.isComplete()) {
      // delete job directory
      final String jobdir = jobconf.get(JOB_DIR_LABEL);
      if (jobdir != null) {
        final Path jobpath = new Path(jobdir);
        jobpath.getFileSystem(jobconf).delete(jobpath, true);
      }
      if (runningJob.isSuccessful()) {
        LOG.info("Job Complete(Succeeded): " + jobID);
      } else {
        LOG.info("Job Complete(Failed): " + jobID);
      }
      return true;
    } else {
      String report = (" job " + jobID +
          " map " + StringUtils.formatPercent(runningJob.mapProgress(), 0) +
          " reduce " +
          StringUtils.formatPercent(runningJob.reduceProgress(), 0));
      if (!report.equals(lastReport)) {
        LOG.info(report);
        lastReport = report;
      }
      TaskCompletionEvent[] events =
          runningJob.getTaskCompletionEvents(jobEventCounter);
      jobEventCounter += events.length;
      for (TaskCompletionEvent event : events) {
        if (event.getTaskStatus() == TaskCompletionEvent.Status.FAILED) {
          LOG.info(" Job " + jobID + " " + event.toString());
        }
      }
      return false;
    }
  }

  public void killJob() throws IOException {
    runningJob.killJob();
  }

  public boolean successful() throws IOException {
    return runningJob.isSuccessful();
  }

  /**
   * set up input file which has the list of input files.
   *
   * @return boolean
   * @throws java.io.IOException
   */
  private boolean prepareJob(PolicyInfo info) throws IOException {
    final String randomId = getRandomId();
    JobClient jClient = new JobClient(jobconf);
    Path jobdir = new Path(jClient.getSystemDir(), NAME + "_" + randomId);

    LOG.info(JOB_DIR_LABEL + "=" + jobdir);
    jobconf.set(JOB_DIR_LABEL, jobdir.toString());
    Path log = new Path(jobdir, "_logs");

    FileOutputFormat.setOutputPath(jobconf, log);
    LOG.info("log=" + log);

    // create operation list
    FileSystem fs = jobdir.getFileSystem(jobconf);
    Path opList = new Path(jobdir, "_" + OP_LIST_LABEL);
    jobconf.set(OP_LIST_LABEL, opList.toString());
    SequenceFile.Writer opWriter = null;

    try {
      opWriter = SequenceFile
          .createWriter(fs, jobconf, opList, Text.class, PolicyInfo.class,
              SequenceFile.CompressionType.NONE);
      opWriter.append(new Text(info.getSrcPath().toString()), info);
    } finally {
      if (opWriter != null) {
        opWriter.close();
      }
    }

    jobconf.setInt(OP_COUNT_LABEL, 1);
    jobconf.setNumMapTasks(1);
    LOG.info(
        "jobName= " + jobName + " numMapTasks=" + jobconf.getNumMapTasks());
    return true;
  }
  
  public long getStartTime() {
    return this.startTime;
  }
  
  public Counters getCounters() throws IOException {
    return this.runningJob.getCounters();
  }

  public JobID getJobID() {
    return this.runningJob.getID();
  }
}
