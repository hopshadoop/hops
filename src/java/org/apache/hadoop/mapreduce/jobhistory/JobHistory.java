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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;

/**
 * JobHistory is the class that is responsible for creating and maintaining
 * job history information.
 *
 */
public class JobHistory {

  final Log LOG = LogFactory.getLog(JobHistory.class);

  private long jobHistoryBlockSize;
  private Map<JobID, MetaInfo> fileMap;
  private ThreadPoolExecutor executor = null;
  static final FsPermission HISTORY_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0750); // rwxr-x---

  public static final FsPermission HISTORY_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0740); // rwxr-----
 
  private JobTracker jobTracker;

  static final long DEFAULT_HISTORY_MAX_AGE = 7 * 24 * 60 * 60 * 1000L; //week
  private FileSystem logDirFs; // log Dir FS
  private FileSystem doneDirFs; // done Dir FS

  private Path logDir = null;
  private Path done = null; // folder for completed jobs

  public static final String OLD_SUFFIX = ".old";
  
  // Version string that will prefix all History Files
  public static final String HISTORY_VERSION = "1.0";

  private HistoryCleaner historyCleanerThread = null;

  /**
   * Initialize Job History Module
   * @param jt Job Tracker handle
   * @param conf Configuration
   * @param hostname Host name of JT
   * @param jobTrackerStartTime Start time of JT
   * @throws IOException
   */
  public void init(JobTracker jt, JobConf conf, String hostname,
      long jobTrackerStartTime) throws IOException {

    // Get and create the log folder
    String logDirLoc = conf.get("hadoop.job.history.location" ,
        "file:///" +
        new File(System.getProperty("hadoop.log.dir")).getAbsolutePath()
        + File.separator + "history");

    LOG.info("History log directory is " + logDirLoc);

    logDir = new Path(logDirLoc);
    logDirFs = logDir.getFileSystem(conf);

    if (!logDirFs.exists(logDir)){
      if (!logDirFs.mkdirs(logDir, 
          new FsPermission(HISTORY_DIR_PERMISSION))) {
        throw new IOException("Mkdirs failed to create " +
            logDir.toString());
      }
    }
    conf.set("hadoop.job.history.location", logDirLoc);

    jobHistoryBlockSize = 
      conf.getLong("mapred.jobtracker.job.history.block.size", 
          3 * 1024 * 1024);
    
    jobTracker = jt;
    
    fileMap = new HashMap<JobID, MetaInfo> ();
  }
  
  /** Initialize the done directory and start the history cleaner thread */
  public void initDone(JobConf conf, FileSystem fs) throws IOException {
    //if completed job history location is set, use that
    String doneLocation =
      conf.get("mapred.job.tracker.history.completed.location");
    if (doneLocation != null) {
      done = fs.makeQualified(new Path(doneLocation));
      doneDirFs = fs;
    } else {
      done = logDirFs.makeQualified(new Path(logDir, "done"));
      doneDirFs = logDirFs;
    }

    //If not already present create the done folder with appropriate 
    //permission
    if (!doneDirFs.exists(done)) {
      LOG.info("Creating DONE folder at "+ done);
      if (! doneDirFs.mkdirs(done, 
          new FsPermission(HISTORY_DIR_PERMISSION))) {
        throw new IOException("Mkdirs failed to create " + done.toString());
      }
    }
    LOG.info("Inited the done directory to " + done.toString());

    moveOldFiles();
    startFileMoverThreads();

    // Start the History Cleaner Thread
    long maxAgeOfHistoryFiles = conf.getLong(
        "mapreduce.cluster.jobhistory.maxage", DEFAULT_HISTORY_MAX_AGE);
    historyCleanerThread = new HistoryCleaner(maxAgeOfHistoryFiles);
    historyCleanerThread.start();
  }

  /**
   * Move the completed job into the completed folder.
   * This assumes that the job history file is closed and 
   * all operations on the job history file is complete.
   * This *should* be the last call to job history for a given job.
   */
  public void markCompleted(JobID id) throws IOException {
    moveToDone(id);
  }

  /** Shut down JobHistory after stopping the History cleaner */
  public void shutDown() {
    LOG.info("Interrupting History Cleaner");
    historyCleanerThread.interrupt();
    try {
      historyCleanerThread.join();
    } catch (InterruptedException e) {
      LOG.info("Error with shutting down history thread");
    }
  }

  /** Get the done directory */
  public synchronized String getDoneJobHistoryFileName(JobConf jobConf,
      JobID id) throws IOException {
    if (done == null) {
      return null;
    }
    return getJobHistoryFileName(jobConf, id, done, doneDirFs);
  }

  /**
   * Get the history location
   */
  public Path getJobHistoryLocation() {
    return logDir;
  }

  /**
   * Get the history location for completed jobs
   */
  public Path getCompletedJobHistoryLocation() {
    return done;
  }

  /**
   * @param dir The directory where to search.
   */
  private synchronized String getJobHistoryFileName(JobConf jobConf,
      JobID id, Path dir, FileSystem fs)
  throws IOException {
    String user = getUserName(jobConf);
    // Make the pattern matching the job's history file
    final Pattern historyFilePattern =
      Pattern.compile(id.toString() + "_" + user + "+");
    // a path filter that matches the parts of the filenames namely
    //  - job-id, user name
    PathFilter filter = new PathFilter() {
      public boolean accept(Path path) {
        String fileName = path.getName();
        return historyFilePattern.matcher(fileName).find();
      }
    };
  
    FileStatus[] statuses = fs.listStatus(dir, filter);
    String filename = null;
    if (statuses.length == 0) {
      LOG.info("Nothing to recover for job " + id);
    } else {
      filename = statuses[0].getPath().getName();
      LOG.info("Recovered job history filename for job " + id + " is "
          + filename);
    }
    return filename;
  }

  String getNewJobHistoryFileName(JobConf conf, JobID jobId) {
    return jobId.toString() +
    "_" + getUserName(conf);
  }

  /**
   * Get the job history file path given the history filename
   */
  private Path getJobHistoryLogLocation(String logFileName) {
    return logDir == null ? null : new Path(logDir, logFileName);
  }

  /**
   * Create an event writer for the Job represented by the jobID.
   * This should be the first call to history for a job
   * @param jobId
   * @param jobConf
   * @throws IOException
   */
  public void setupEventWriter(JobID jobId, JobConf jobConf)
  throws IOException {
    String logFileName = getNewJobHistoryFileName(jobConf, jobId);
  
    Path logFile = getJobHistoryLogLocation(logFileName);
  
    if (logDir == null) {
      LOG.info("Log Directory is null, returning");
      throw new IOException("Missing Log Directory for History");
    }
  
    int defaultBufferSize = 
      logDirFs.getConf().getInt("io.file.buffer.size", 4096);
  
    LOG.info("SetupWriter, creating file " + logFile);
  
    FSDataOutputStream out = logDirFs.create(logFile, 
        new FsPermission(JobHistory.HISTORY_FILE_PERMISSION),
        EnumSet.of(CreateFlag.OVERWRITE), 
        defaultBufferSize, 
        logDirFs.getDefaultReplication(), 
        jobHistoryBlockSize, null);
  
    EventWriter writer = new EventWriter(out);
  
    /* Storing the job conf on the log dir */
  
    Path logDirConfPath = getConfFile(jobId);
    LOG.info("LogDirConfPath is " + logDirConfPath);
  
    FSDataOutputStream jobFileOut = null;
    try {
      if (logDirConfPath != null) {
        defaultBufferSize =
          logDirFs.getConf().getInt("io.file.buffer.size", 4096);
        if (!logDirFs.exists(logDirConfPath)) {
          jobFileOut = logDirFs.create(logDirConfPath,
              new FsPermission(JobHistory.HISTORY_FILE_PERMISSION),
              EnumSet.of(CreateFlag.OVERWRITE),
              defaultBufferSize,
              logDirFs.getDefaultReplication(),
              logDirFs.getDefaultBlockSize(), null);
          jobConf.writeXml(jobFileOut);
          jobFileOut.close();
        }
      }
    } catch (IOException e) {
      LOG.info("Failed to close the job configuration file " 
          + StringUtils.stringifyException(e));
    }
  
    MetaInfo fi = new MetaInfo(logFile, logDirConfPath, writer);
    fileMap.put(jobId, fi);
  }

  /** Close the event writer for this id */
  public void closeWriter(JobID id) {
    try {
      EventWriter writer = getWriter(id);
      writer.close();
    } catch (IOException e) {
      LOG.info("Error closing writer for JobID: " + id);
    }
  }


  /**
   * Get the JsonEventWriter for the specified Job Id
   * @param jobId
   * @return
   * @throws IOException if a writer is not available
   */
  private EventWriter getWriter(final JobID jobId) throws IOException {
    EventWriter writer = null;
    MetaInfo mi = fileMap.get(jobId);
    if (mi == null || (writer = mi.getEventWriter()) == null) {
      throw new IOException("History File does not exist for JobID");
    }
    return writer;
  }

  /**
   * Method to log the specified event
   * @param event The event to log
   * @param id The Job ID of the event
   */
  public void logEvent(HistoryEvent event, JobID id) {
    try {
      EventWriter writer = getWriter(id);
      writer.write(event);
    } catch (IOException e) {
      LOG.error("Error creating writer, " + e.getMessage());
    }
  }


  private void moveToDoneNow(Path fromPath, Path toPath) throws IOException {
    //check if path exists, in case of retries it may not exist
    if (logDirFs.exists(fromPath)) {
      LOG.info("Moving " + fromPath.toString() + " to " +
          toPath.toString());
      doneDirFs.moveFromLocalFile(fromPath, toPath);
      doneDirFs.setPermission(toPath,
          new FsPermission(JobHistory.HISTORY_FILE_PERMISSION));
    }
  }
  
  private void startFileMoverThreads() {
    executor = new ThreadPoolExecutor(1, 3, 1, 
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
  }

  Path getConfFile(JobID jobId) {
    Path jobFilePath = null;
    if (logDir != null) {
      jobFilePath = new Path(logDir + File.separator +
          jobId.toString() + "_conf.xml");
    }
    return jobFilePath;
  }

  private void moveOldFiles() throws IOException {
    //move the log files remaining from last run to the DONE folder
    //suffix the file name based on Jobtracker identifier so that history
    //files with same job id don't get over written in case of recovery.
    FileStatus[] files = logDirFs.listStatus(logDir);
    String jtIdentifier = jobTracker.getTrackerIdentifier();
    String fileSuffix = "." + jtIdentifier + JobHistory.OLD_SUFFIX;
    for (FileStatus fileStatus : files) {
      Path fromPath = fileStatus.getPath();
      if (fromPath.equals(done)) { //DONE can be a subfolder of log dir
        continue;
      }
      LOG.info("Moving log file from last run: " + fromPath);
      Path toPath = new Path(done, fromPath.getName() + fileSuffix);
      moveToDoneNow(fromPath, toPath);
    }
  }
  
  private void moveToDone(final JobID id) {
    final List<Path> paths = new ArrayList<Path>();
    MetaInfo metaInfo = fileMap.get(id);
    if (metaInfo == null) {
      LOG.info("No file for job-history with " + id + " found in cache!");
      return;
    }
    
    final Path historyFile = metaInfo.getHistoryFile();
    if (historyFile == null) {
      LOG.info("No file for job-history with " + id + " found in cache!");
    } else {
      paths.add(historyFile);
    }

    final Path confPath = metaInfo.getConfFile();
    if (confPath == null) {
      LOG.info("No file for jobconf with " + id + " found in cache!");
    } else {
      paths.add(confPath);
    }

    executor.execute(new Runnable() {

      public void run() {
        //move the files to DONE folder
        try {
          for (Path path : paths) { 
            moveToDoneNow(path, new Path(done, path.getName()));
          }
        } catch (Throwable e) {
          LOG.error("Unable to move history file to DONE folder.", e);
        }
        String historyFileDonePath = null;
        if (historyFile != null) {
          historyFileDonePath = new Path(done, 
              historyFile.getName()).toString();
        }
        
        jobTracker.retireJob(org.apache.hadoop.mapred.JobID.downgrade(id),
            historyFileDonePath);

        //purge the job from the cache
        fileMap.remove(id);
      }

    });
  }
  
  private String getUserName(JobConf jobConf) {
    String user = jobConf.getUser();
    if (user == null) {
      user = "";
    }
    return user;
  }
  
  private static class MetaInfo {
    private Path historyFile;
    private Path confFile;
    private EventWriter writer;

    MetaInfo(Path historyFile, Path conf, EventWriter writer) {
      this.historyFile = historyFile;
      this.confFile = conf;
      this.writer = writer;
    }

    Path getHistoryFile() { return historyFile; }
    Path getConfFile() { return confFile; }
    EventWriter getEventWriter() { return writer; }
  }

  /**
   * Delete history files older than a specified time duration.
   */
  class HistoryCleaner extends Thread {
    static final long ONE_DAY_IN_MS = 24 * 60 * 60 * 1000L;
    private long cleanupFrequency;
    private long maxAgeOfHistoryFiles;
  
    public HistoryCleaner(long maxAge) {
      setName("Thread for cleaning up History files");
      setDaemon(true);
      this.maxAgeOfHistoryFiles = maxAge;
      cleanupFrequency = Math.min(ONE_DAY_IN_MS, maxAgeOfHistoryFiles);
      LOG.info("Job History Cleaner Thread started." +
          " MaxAge is " + 
          maxAge + " ms(" + ((float)maxAge)/(ONE_DAY_IN_MS) + " days)," +
          " Cleanup Frequency is " +
          + cleanupFrequency + " ms (" +
          ((float)cleanupFrequency)/ONE_DAY_IN_MS + " days)");
    }
  
    @Override
    public void run(){
  
      while (true) {
        try {
          doCleanup(); 
          Thread.sleep(cleanupFrequency);
        }
        catch (InterruptedException e) {
          LOG.info("History Cleaner thread exiting");
          return;
        }
        catch (Throwable t) {
          LOG.warn("History cleaner thread threw an exception", t);
        }
      }
    }
  
    private void doCleanup() {
      long now = System.currentTimeMillis();
      try {
        FileStatus[] historyFiles = doneDirFs.listStatus(done);
        if (historyFiles != null) {
          for (FileStatus f : historyFiles) {
            if (now - f.getModificationTime() > maxAgeOfHistoryFiles) {
              doneDirFs.delete(f.getPath(), true); 
              LOG.info("Deleting old history file : " + f.getPath());
            }
          }
        }
      } catch (IOException ie) {
        LOG.info("Error cleaning up history directory" + 
            StringUtils.stringifyException(ie));
      }
    }
  }
}
