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

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.RepairJobsDataAccess;
import io.hops.metadata.hdfs.entity.RepairJob;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.BlockReconstructor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MapReduceBlockRepairManager extends BlockRepairManager {
  protected static final Log LOG =
      LogFactory.getLog(MapReduceBlockRepairManager.class);

  public static final String JOBUSER = "erasure_coding";

  private static final String IN_FILE_SUFFIX = ".in";

  private static final String MAX_FIX_TIME_FOR_FILE =
      "io.hops.erasure_coding.blockfix.max.fix.time.for.file";
  private static final long DEFAULT_MAX_FIX_TIME_FOR_FILE = 4 * 60 * 60 * 1000; // 4 hrs.

  private Worker corruptionWorker = new CorruptionWorker();

  static enum Counter {
    FILES_SUCCEEDED,
    FILES_FAILED
  }

  private static final String REPAIR_TYPE = "repair_type";
  private static final String SOURCE_PATH = "source_path";
  private static final String PARITY_PATH = "parity_path";
  private static final String CODEC_ID = "codec_id";

  private enum RepairType {
    SOURCE_FILE,
    PARITY_FILE
  }

  private class ActiveRepair {
    private JobID jobId;
    private Job job;
    private RunningJob runningJob;
    private Path inDir;
    private Path outDir;
    // Not persisted right now.
    private long startTime = System.currentTimeMillis();

    public ActiveRepair(JobID jobId, Job job,
        Path inDir, Path outDir) {
      this.jobId = jobId;
      this.job = job;
      this.inDir = inDir;
      this.outDir = outDir;
    }

    public ActiveRepair(JobID jobId, RunningJob job,
        Path inDir, Path outDir) {
      this.jobId = jobId;
      this.runningJob = job;
      this.inDir = inDir;
      this.outDir = outDir;
    }

    public JobID getJobId() {
      return jobId;
    }

    public void killJob() throws IOException {
      if (job != null) {
        job.killJob();
      } else {
        runningJob.killJob();
      }
    }

    public boolean isComplete() throws IOException {
      if (job != null) {
        return job.isComplete();
      } else {
        return runningJob.isComplete();
      }
    }

    public boolean isSuccessful() throws IOException {
      if (job != null) {
        return job.isSuccessful();
      } else {
        return runningJob.isSuccessful();
      }
    }

    public Path getInDir() {
      return inDir;
    }

    public Path getOutDir() {
      return outDir;
    }

    public long getStartTime() {
      return startTime;
    }
  }

  private Map<String, ActiveRepair> currentRepairs =
      new HashMap<String, ActiveRepair>();
  private Collection<ActiveRepair> completedJobs =
      new ArrayList<ActiveRepair>();

  private boolean initialized = false;

  public MapReduceBlockRepairManager(Configuration conf) {
    super(conf);
  }

  @Override
  public void repairSourceBlocks(String codecId, Path sourceFile,
      Path parityFile) {
    initialize();
    String uniqueName =
        sourceFile.getName() + "_" + UUID.randomUUID().toString();
    try {
      corruptionWorker.startJob(uniqueName, sourceFile, parityFile, codecId,
          RepairType.SOURCE_FILE);
    } catch (IOException e) {
      LOG.error("Exception", e);
      LOG.error("Exception", e);
    } catch (InterruptedException e) {
      LOG.error("Exception", e);
    } catch (ClassNotFoundException e) {
      LOG.error("Exception", e);
    }
  }

  @Override
  public void repairParityBlocks(String codecId, Path sourceFile,
      Path parityFile) {
    initialize();
    String uniqueName =
        sourceFile.getName() + "_" + UUID.randomUUID().toString();
    try {
      corruptionWorker.startJob(uniqueName, sourceFile, parityFile, codecId,
          RepairType.PARITY_FILE);
    } catch (IOException e) {
      LOG.error("Exception", e);
    } catch (InterruptedException e) {
      LOG.error("Exception", e);
    } catch (ClassNotFoundException e) {
      LOG.error("Exception", e);
    }
  }

  protected long getMaxFixTimeForFile() {
    return getConf()
        .getLong(MAX_FIX_TIME_FOR_FILE, DEFAULT_MAX_FIX_TIME_FOR_FILE);
  }

  public abstract class Worker {
    protected final Log LOG;
    protected final Class<? extends BlockReconstructor> RECONSTRUCTOR_CLASS;
    protected final String JOB_NAME_PREFIX;


    protected Worker(Log log, Class<? extends BlockReconstructor> rClass,
        String prefix) {

      this.LOG = log;
      this.RECONSTRUCTOR_CLASS = rClass;
      this.JOB_NAME_PREFIX = prefix;
    }

    /**
     * creates and submits a job, updates file index and job index
     */
    private void startJob(String jobName, Path sourceFile, Path parityFile,
        String codecId, RepairType type)
        throws IOException, InterruptedException, ClassNotFoundException {
      Path inDir = new Path(JOB_NAME_PREFIX + "/in/" + jobName);
      Path outDir = new Path(JOB_NAME_PREFIX + "/out/" + jobName);
      createInputFile(jobName, inDir, sourceFile.toUri().getPath());

      Configuration jobConf = new Configuration(getConf());
      jobConf.set(REPAIR_TYPE, type.toString());
      jobConf.set(SOURCE_PATH, sourceFile.toUri().getPath());
      jobConf.set(PARITY_PATH, parityFile.toUri().getPath());
      jobConf.set(CODEC_ID, codecId);

      Job job = new Job(jobConf, jobName);
      configureJob(job, this.RECONSTRUCTOR_CLASS);
      job.setJarByClass(getClass());
      job.setJobName(jobName);
      job.setMapperClass(ReconstructionMapper.class);
      job.setNumReduceTasks(0);
      job.setInputFormatClass(ReconstructionInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      ReconstructionInputFormat.setInputPaths(job, inDir);
      SequenceFileOutputFormat.setOutputPath(job, outDir);

      submitJob(job);
      String path = sourceFile.toUri().getPath();
      JobClient jobClient = new JobClient(getConf());
      JobID jobId = job.getJobID();
      ActiveRepair activeRepair = new ActiveRepair(jobId, job, inDir,
          outDir);
      // There is a risk that it crashes before persisting the job for recovery.
      // The ErasureCodingManager should restart the job in that case as it
      // does not set the state to active before this code succeeded.
      persistActiveJob(path, jobId, inDir.toUri().getPath(),
          outDir.toUri().getPath());
      currentRepairs.put(type == RepairType.SOURCE_FILE ?
          sourceFile.toUri().getPath() : parityFile.toUri().getPath(),
          activeRepair);
    }

    private void submitJob(Job job)
        throws IOException, InterruptedException, ClassNotFoundException {
      LOG.info("Submitting job");
      MapReduceBlockRepairManager.this.submitJob(job);
    }

    /**
     * creates the input file (containing the names of the file to be
     * reconstructed)
     */
    private void createInputFile(String jobName, Path inDir, String lostFile)
        throws IOException {
      Path file = new Path(inDir, jobName + IN_FILE_SUFFIX);
      FileSystem fs = file.getFileSystem(getConf());
      SequenceFile.Writer fileOut = SequenceFile
          .createWriter(fs, getConf(), file, LongWritable.class, Text.class);
      fileOut.append(new LongWritable(0L), new Text(lostFile));
      fileOut.close();
    }
  }

  public class CorruptionWorker extends Worker {

    public CorruptionWorker() {
      super(LogFactory.getLog(CorruptionWorker.class), BlockReconstructor.class,
          "blockfixer");
    }
  }

  public void configureJob(Job job,
      Class<? extends BlockReconstructor> reconstructorClass) {

    ((JobConf) job.getConfiguration()).setUser(JOBUSER);
    ((JobConf) job.getConfiguration())
        .setClass(ReconstructionMapper.RECONSTRUCTOR_CLASS_TAG,
            reconstructorClass, BlockReconstructor.class);
  }

  private void submitJob(Job job)
      throws IOException, InterruptedException, ClassNotFoundException {
    job.submit();
    LOG.info("Job " + job.getJobID() + "(" + job.getJobName() + ") started");
  }

  static class ReconstructionInputFormat
      extends SequenceFileInputFormat<LongWritable, Text> {

    protected static final Log LOG =
        LogFactory.getLog(ReconstructionMapper.class);

    /**
     * splits the input files into tasks handled by a single node
     * we have to read the input files to do this based on a number of
     * items in a sequence
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      long filesPerTask = 1;

      Path[] inPaths = getInputPaths(job);

      List<InputSplit> splits = new ArrayList<InputSplit>();

      long fileCounter = 0;

      for (Path inPath : inPaths) {

        FileSystem fs = inPath.getFileSystem(job.getConfiguration());

        if (!fs.getFileStatus(inPath).isDir()) {
          throw new IOException(inPath.toString() + " is not a directory");
        }

        FileStatus[] inFiles = fs.listStatus(inPath);

        for (FileStatus inFileStatus : inFiles) {
          Path inFile = inFileStatus.getPath();

          if (!inFileStatus.isDir() &&
              (inFile.getName().equals(job.getJobName() + IN_FILE_SUFFIX))) {

            fileCounter++;
            SequenceFile.Reader inFileReader =
                new SequenceFile.Reader(fs, inFile, job.getConfiguration());

            long startPos = inFileReader.getPosition();
            long counter = 0;

            // create an input split every filesPerTask items in the sequence
            LongWritable key = new LongWritable();
            Text value = new Text();
            try {
              while (inFileReader.next(key, value)) {
                if (counter % filesPerTask == filesPerTask - 1L) {
                  splits.add(new FileSplit(inFile, startPos,
                      inFileReader.getPosition() - startPos, null));
                  startPos = inFileReader.getPosition();
                }
                counter++;
              }

              // create input split for remaining items if necessary
              // this includes the case where no splits were created by the loop
              if (startPos != inFileReader.getPosition()) {
                splits.add(new FileSplit(inFile, startPos,
                    inFileReader.getPosition() - startPos, null));
              }
            } finally {
              inFileReader.close();
            }
          }
        }
      }

      LOG.info("created " + splits.size() + " input splits from " +
          fileCounter + " files");

      return splits;
    }

    /**
     * indicates that input file can be split
     */
    @Override
    public boolean isSplitable(JobContext job, Path file) {
      return true;
    }
  }

  /**
   * Mapper for reconstructing stripes with lost blocks
   */
  static class ReconstructionMapper
      extends Mapper<LongWritable, Text, Text, Text> {

    protected static final Log LOG =
        LogFactory.getLog(ReconstructionMapper.class);

    public static final String RECONSTRUCTOR_CLASS_TAG =
        "hdfs.blockintegrity.reconstructor";

    private BlockReconstructor reconstructor;

    private RepairType repairType;
    private Path sourceFile;
    private Path parityFile;
    private Decoder decoder;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {

      super.setup(context);

      Configuration conf = context.getConfiguration();
      Codec.initializeCodecs(conf);

      repairType = RepairType.valueOf(conf.get(REPAIR_TYPE));
      sourceFile = new Path(conf.get(SOURCE_PATH));
      parityFile = new Path(conf.get(PARITY_PATH));
      decoder = new Decoder(conf, Codec.getCodec(conf.get(CODEC_ID)));

      Class<? extends BlockReconstructor> reconstructorClass =
          context.getConfiguration().getClass(RECONSTRUCTOR_CLASS_TAG, null,
              BlockReconstructor.class);

      if (reconstructorClass == null) {
        LOG.error("No class supplied for reconstructor " +
            "(prop " + RECONSTRUCTOR_CLASS_TAG + ")");
        context.progress();
        return;
      }

      // We dynamically instantiate the helper based on the helperClass member
      try {
        Constructor<? extends BlockReconstructor> ctor =
            reconstructorClass.getConstructor(new Class[]{Configuration.class});

        reconstructor = ctor.newInstance(conf);

      } catch (Exception ex) {
        throw new IOException("Could not instantiate a block reconstructor " +
            "based on class " + reconstructorClass, ex);
      }
    }

    @Override
    public void map(LongWritable key, Text fileText, Mapper.Context context)
        throws IOException, InterruptedException {

      String fileStr = fileText.toString();
      LOG.info("reconstructing " + fileStr);
      Path file = new Path(fileStr);
      try {
        switch (repairType) {
          case SOURCE_FILE:
            reconstructor.processFile(sourceFile, parityFile, decoder);
            break;
          case PARITY_FILE:
            reconstructor.processParityFile(sourceFile, parityFile, decoder);
            break;
        }
      } catch (Exception e) {
        LOG.error("Reconstructing file " + file + " failed", e);

        // report file as failed
        context.getCounter(Counter.FILES_FAILED).increment(1L);
        String outkey = fileStr;
        String outval = "failed";
        context.write(new Text(outkey), new Text(outval));
        throw new RuntimeException(e);
      }
      context.progress();
    }
  }

  @Override
  public List<Report> computeReports() throws IOException {
    initialize();
    cleanRecovery();

    List<Report> reports = new ArrayList<Report>();
    for (Map.Entry<String, ActiveRepair> entry : currentRepairs.entrySet()) {
      String fileName = entry.getKey();
      ActiveRepair job = entry.getValue();
      try {
        if (job.isComplete() && job.isSuccessful()) {
          LOG.info("REPAIR COMPLETE");
          reports.add(new Report(fileName, Report.Status.FINISHED));
          cleanup(job);
        } else if (job.isComplete() && !job.isSuccessful()) {
          LOG.info("REPAIR FAILED");
          reports.add(new Report(fileName, Report.Status.FAILED));
          cleanup(job);
        } else if (job.getStartTime() > 0 && System.currentTimeMillis()
            - job.getStartTime() > getMaxFixTimeForFile()){
          LOG.info("Timeout: " + (System.currentTimeMillis()
              - job.getStartTime()) + " " + job.getStartTime());
          job.killJob();
          reports.add(new Report(fileName, Report.Status.CANCELED));
          cleanup(job);
        } else {
          LOG.info("REPAIR RUNNING");
          reports.add(new Report(fileName, Report.Status.ACTIVE));
        }
      } catch (Exception e) {
        LOG.info("Exception during completeness check", e);
        try {
          job.killJob();
        } catch (Exception e1) {
        }
        reports.add(new Report(fileName, Report.Status.FAILED));
        cleanup(job);
      }
    }

    for (Report report : reports) {
      Report.Status status = report.getStatus();
      if (status == Report.Status.FINISHED || status == Report.Status.FAILED ||
          status == Report.Status.CANCELED) {
        ActiveRepair activeRepair = currentRepairs.remove(report.getFilePath());
        completedJobs.add(activeRepair);
      }
    }

    return reports;
  }

  @Override
  public void cancelAll() {
    initialize();
    for (ActiveRepair job : currentRepairs.values()) {
      try {
        job.killJob();
      } catch (Exception e) {
        LOG.error("Exception", e);
      }
      cleanup(job);
    }
    currentRepairs.clear();
  }

  @Override
  public void cancel(String toCancel) {
    initialize();
    ActiveRepair job = currentRepairs.get(toCancel);
    try {
      job.killJob();
    } catch (Exception e) {
      LOG.error("Exception", e);
    }
    currentRepairs.remove(toCancel);
    cleanup(job);
  }

  private void cleanup(ActiveRepair job) {
    Path outDir = null;
    try {
      outDir = job.getOutDir();
      outDir.getFileSystem(getConf()).delete(outDir, true);
    } catch (IOException e) {
      LOG.warn("Could not delete output dir " + outDir, e);
    }
    Path inDir = null;
    try {
      // We only create one input directory.
      inDir = job.getInDir();
      inDir.getFileSystem(getConf()).delete(inDir, true);
    } catch (IOException e) {
      LOG.warn("Could not delete input dir " + inDir, e);
    }
  }

  private void persistActiveJob(final String path, final JobID jobId,
      final String inDir, final String outDir) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.PERSIST_ENCODING_JOB) {
      @Override
      public Object performTask() throws IOException {
        RepairJobsDataAccess da = (RepairJobsDataAccess)
            HdfsStorageFactory.getDataAccess(RepairJobsDataAccess.class);
        da.add(new RepairJob(jobId.getJtIdentifier(),
            jobId.getId(), path, inDir, outDir));
        return null;
      }
    }.handle();
  }

  private void cleanRecovery() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DELETE_ENCODING_JOBS) {
      @Override
      public Object performTask() throws IOException {
        RepairJobsDataAccess da = (RepairJobsDataAccess)
            HdfsStorageFactory.getDataAccess(RepairJobsDataAccess.class);
        Iterator<ActiveRepair> it = completedJobs.iterator();
        while (it.hasNext()) {
          ActiveRepair job = it.next();
          JobID jobId = job.getJobId();
          da.delete(new RepairJob(jobId.getJtIdentifier(), jobId.getId()));
          it.remove();
        }
        return null;
      }
    }.handle();
  }

  private void initialize() {
    if (initialized) {
      return;
    }

    try {
      JobClient jobClient = new JobClient(getConf());
      for (RepairJob job : recoverActiveJobs()) {
        org.apache.hadoop.mapred.JobID jobId =
            new org.apache.hadoop.mapred.JobID(job.getJtIdentifier(),
                job.getJobId());
        RunningJob runningJob = jobClient.getJob(jobId);
        if (runningJob == null) {
          throw new IOException("Failed to recover");
        }
        ActiveRepair recovered = new ActiveRepair(jobId, runningJob,
            new Path(job.getInDir()), new Path(job.getOutDir()));
        currentRepairs.put(job.getPath(), recovered);
      }
    } catch (IOException e) {
      LOG.error("Encoding job recovery failed", e);
      throw new RuntimeException(e);
    }

    initialized = true;
  }

  private Collection<RepairJob> recoverActiveJobs() throws IOException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(
        HDFSOperationType.RECOVER_ENCODING_JOBS) {
      @Override
      public Object performTask() throws IOException {
        RepairJobsDataAccess da = (RepairJobsDataAccess)
            HdfsStorageFactory.getDataAccess(RepairJobsDataAccess.class);
        return da.findAll();
      }
    };

    return (Collection<RepairJob>) handler.handle();
  }
}
