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
package org.apache.hadoop.mapred.gridmix.test.system;

import java.io.IOException;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.test.system.JTClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.ZombieJob;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.junit.Assert;
import java.text.ParseException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Verifying each Gridmix job with corresponding job story in a trace file.
 */
public class GridmixJobVerification {

  private static Log LOG = LogFactory.getLog(GridmixJobVerification.class);
  private Path path;
  private Configuration conf;
  private JTClient jtClient;
  private String userResolverVal;
  static final String origJobIdKey = GridMixConfig.GRIDMIX_ORIGINAL_JOB_ID;
  static final String jobSubKey = GridMixConfig.GRIDMIX_SUBMISSION_POLICY;
  static final String jobTypeKey = GridMixConfig.GRIDMIX_JOB_TYPE;
  static final String mapTaskKey = GridMixConfig.GRIDMIX_SLEEPJOB_MAPTASK_ONLY;
  static final String usrResolver = GridMixConfig.GRIDMIX_USER_RESOLVER;

  /**
   * Gridmix job verification constructor
   * @param path - path of the gridmix output directory.
   * @param conf - cluster configuration.
   * @param jtClient - jobtracker client.
   */
  public GridmixJobVerification(Path path, Configuration conf, 
     JTClient jtClient) {
    this.path = path;
    this.conf = conf;
    this.jtClient = jtClient;
  }
  
  /**
   * It verifies the Gridmix jobs with corresponding job story in a trace file.
   * @param jobids - gridmix job ids.
   * @throws IOException - if an I/O error occurs.
   * @throws ParseException - if an parse error occurs.
   */
  public void verifyGridmixJobsWithJobStories(List<JobID> jobids) 
      throws IOException, ParseException {

    SortedMap <Long, String> origSubmissionTime = new TreeMap <Long, String>();
    SortedMap <Long, String> simuSubmissionTime = new TreeMap<Long, String>();
    GridmixJobStory gjs = new GridmixJobStory(path, conf);
    final Iterator<JobID> ite = jobids.iterator();
    File destFolder = new File(System.getProperty("java.io.tmpdir") 
                              + "/gridmix-st/");
    destFolder.mkdir();
    while (ite.hasNext()) {
      JobID simuJobId = ite.next();
      JobHistoryParser.JobInfo jhInfo = getSimulatedJobHistory(simuJobId);
      Assert.assertNotNull("Job history not found.", jhInfo);
      Counters counters = jhInfo.getTotalCounters();
      JobConf simuJobConf = getSimulatedJobConf(simuJobId, destFolder);
      String origJobId = simuJobConf.get(origJobIdKey);
      LOG.info("OriginalJobID<->CurrentJobID:" 
              + origJobId + "<->" + simuJobId);

      if (userResolverVal == null) {
        userResolverVal = simuJobConf.get(usrResolver);
      }
      ZombieJob zombieJob = gjs.getZombieJob(JobID.forName(origJobId));
      Map<String, Long> mapJobCounters = getJobMapCounters(zombieJob);
      Map<String, Long> reduceJobCounters = getJobReduceCounters(zombieJob);
      if (simuJobConf.get(jobSubKey).contains("REPLAY")) {
          origSubmissionTime.put(zombieJob.getSubmissionTime(), 
                                 origJobId.toString() + "^" + simuJobId); 
          simuSubmissionTime.put(jhInfo.getSubmitTime() , 
                                 origJobId.toString() + "^" + simuJobId); ;
      }

      LOG.info("Verifying the job <" + simuJobId + "> and wait for a while...");
      verifySimulatedJobSummary(zombieJob, jhInfo, simuJobConf);
      verifyJobMapCounters(counters, mapJobCounters, simuJobConf);
      verifyJobReduceCounters(counters, reduceJobCounters, simuJobConf); 
      LOG.info("Done.");
    }
  }

  /**
   * Verify the job subimssion order between the jobs in replay mode.
   * @param origSubmissionTime - sorted map of original jobs submission times.
   * @param simuSubmissionTime - sorted map of simulated jobs submission times.
   */
  public void verifyJobSumissionTime(SortedMap<Long, String> origSubmissionTime, 
      SortedMap<Long, String> simuSubmissionTime) { 
    Assert.assertEquals("Simulated job's submission time count has " 
                     + "not match with Original job's submission time count.", 
                     origSubmissionTime.size(), simuSubmissionTime.size());
    for ( int index = 0; index < origSubmissionTime.size(); index ++) {
        String origAndSimuJobID = origSubmissionTime.get(index);
        String simuAndorigJobID = simuSubmissionTime.get(index);
        Assert.assertEquals("Simulated jobs have not submitted in same " 
                           + "order as original jobs submitted in REPLAY mode.", 
                           origAndSimuJobID, simuAndorigJobID);
    }
  }

  /**
   * It verifies the simulated job map counters.
   * @param counters - Original job map counters.
   * @param mapJobCounters - Simulated job map counters.
   * @param jobConf - Simulated job configuration.
   * @throws ParseException - If an parser error occurs.
   */
  public void verifyJobMapCounters(Counters counters, 
     Map<String,Long> mapCounters, JobConf jobConf) throws ParseException {
    if (!jobConf.get(jobTypeKey, "LOADJOB").equals("SLEEPJOB")) {
      Assert.assertEquals("Map input records have not matched.",
                          mapCounters.get("MAP_INPUT_RECS").longValue(), 
                          getCounterValue(counters, "MAP_INPUT_RECORDS"));
    } else {
      Assert.assertTrue("Map Input Bytes are zero", 
                        getCounterValue(counters,"HDFS_BYTES_READ") != 0);
      Assert.assertNotNull("Map Input Records are zero", 
                           getCounterValue(counters, "MAP_INPUT_RECORDS")!=0);
    }
  }

  /**
   *  It verifies the simulated job reduce counters.
   * @param counters - Original job reduce counters.
   * @param reduceCounters - Simulated job reduce counters.
   * @param jobConf - simulated job configuration.
   * @throws ParseException - if an parser error occurs.
   */
  public void verifyJobReduceCounters(Counters counters, 
     Map<String,Long> reduceCounters, JobConf jobConf) throws ParseException {
    if (jobConf.get(jobTypeKey, "LOADJOB").equals("SLEEPJOB")) {
      Assert.assertTrue("Reduce output records are not zero for sleep job.",
          getCounterValue(counters, "REDUCE_OUTPUT_RECORDS") == 0);
      Assert.assertTrue("Reduce output bytes are not zero for sleep job.", 
          getCounterValue(counters,"HDFS_BYTES_WRITTEN") == 0);
    }
  }

  /**
   * It verifies the gridmix simulated job summary.
   * @param zombieJob - Original job summary.
   * @param jhInfo  - Simulated job history info.
   * @param jobConf - simulated job configuration.
   * @throws IOException - if an I/O error occurs.
   */
  public void verifySimulatedJobSummary(ZombieJob zombieJob, 
     JobHistoryParser.JobInfo jhInfo, JobConf jobConf) throws IOException {
    Assert.assertEquals("Job id has not matched", zombieJob.getJobID(), 
                        JobID.forName(jobConf.get(origJobIdKey)));

    Assert.assertEquals("Job maps have not matched", zombieJob.getNumberMaps(),
                        jhInfo.getTotalMaps());

    if (!jobConf.getBoolean(mapTaskKey, false)) { 
      Assert.assertEquals("Job reducers have not matched", 
          zombieJob.getNumberReduces(), jhInfo.getTotalReduces());
    } else {
      Assert.assertEquals("Job reducers have not matched",
                          0, jhInfo.getTotalReduces());
    }

    Assert.assertEquals("Job status has not matched.", 
                        zombieJob.getOutcome().name(), 
                        convertJobStatus(jhInfo.getJobStatus()));

    LoggedJob loggedJob = zombieJob.getLoggedJob();
    Assert.assertEquals("Job priority has not matched.", 
                        loggedJob.getPriority().toString(), 
                        jhInfo.getPriority());

    if (jobConf.get(usrResolver).contains("RoundRobin")) {
       String user = UserGroupInformation.getLoginUser().getShortUserName();
       Assert.assertTrue(jhInfo.getJobId().toString() 
                        + " has not impersonate with other user.", 
                        !jhInfo.getUsername().equals(user));
    }
  }

  /**
   * Get the original job map counters from a trace.
   * @param zombieJob - Original job story.
   * @return - map counters as a map.
   */
  public Map<String, Long> getJobMapCounters(ZombieJob zombieJob) {
    long expMapInputBytes = 0;
    long expMapOutputBytes = 0;
    long expMapInputRecs = 0;
    long expMapOutputRecs = 0;
    Map<String,Long> mapCounters = new HashMap<String,Long>();
    for (int index = 0; index < zombieJob.getNumberMaps(); index ++) {
      TaskInfo mapTask = zombieJob.getTaskInfo(TaskType.MAP, index);
      expMapInputBytes += mapTask.getInputBytes();
      expMapOutputBytes += mapTask.getOutputBytes();
      expMapInputRecs += mapTask.getInputRecords();
      expMapOutputRecs += mapTask.getOutputRecords();
    }
    mapCounters.put("MAP_INPUT_BYTES", expMapInputBytes);
    mapCounters.put("MAP_OUTPUT_BYTES", expMapOutputBytes);
    mapCounters.put("MAP_INPUT_RECS", expMapInputRecs);
    mapCounters.put("MAP_OUTPUT_RECS", expMapOutputRecs);
    return mapCounters;
  }
  
  /**
   * Get the original job reduce counters from a trace.
   * @param zombieJob - Original job story.
   * @return - reduce counters as a map.
   */
  public Map<String,Long> getJobReduceCounters(ZombieJob zombieJob) {
    long expReduceInputBytes = 0;
    long expReduceOutputBytes = 0;
    long expReduceInputRecs = 0;
    long expReduceOutputRecs = 0;
    Map<String,Long> reduceCounters = new HashMap<String,Long>();
    for (int index = 0; index < zombieJob.getNumberReduces(); index ++) {
      TaskInfo reduceTask = zombieJob.getTaskInfo(TaskType.REDUCE, index);
      expReduceInputBytes += reduceTask.getInputBytes();
      expReduceOutputBytes += reduceTask.getOutputBytes();
      expReduceInputRecs += reduceTask.getInputRecords();
      expReduceOutputRecs += reduceTask.getOutputRecords();
    }
    reduceCounters.put("REDUCE_INPUT_BYTES", expReduceInputBytes);
    reduceCounters.put("REDUCE_OUTPUT_BYTES", expReduceOutputBytes);
    reduceCounters.put("REDUCE_INPUT_RECS", expReduceInputRecs);
    reduceCounters.put("REDUCE_OUTPUT_RECS", expReduceOutputRecs);
    return reduceCounters;
  }

  /**
   * Get the simulated job configuration of a job.
   * @param simulatedJobID - Simulated job id.
   * @param tmpJHFolder - temporary job history folder location.
   * @return - simulated job configuration.
   * @throws IOException - If an I/O error occurs.
   */
  public JobConf getSimulatedJobConf(JobID simulatedJobID, File tmpJHFolder) 
      throws IOException{
    FileSystem fs = null;
    try {

      String historyFilePath = 
         jtClient.getProxy().getJobHistoryLocationForRetiredJob(simulatedJobID);
      Path jhpath = new Path(historyFilePath);
      fs = jhpath.getFileSystem(conf);
      fs.copyToLocalFile(jhpath,new Path(tmpJHFolder.toString()));
      fs.copyToLocalFile(new Path(historyFilePath + "_conf.xml"), 
                         new Path(tmpJHFolder.toString()));
      JobConf jobConf = new JobConf();
      jobConf.addResource(new Path(tmpJHFolder.toString() 
                         + "/" + simulatedJobID + "_conf.xml"));
      jobConf.reloadConfiguration();
      return jobConf;

    }finally {
      fs.close();
    }
  }

  /**
   * Get the simulated job history of a job.
   * @param simulatedJobID - simulated job id.
   * @return - simulated job information.
   * @throws IOException - if an I/O error occurs.
   */
  public JobHistoryParser.JobInfo getSimulatedJobHistory(JobID simulatedJobID) 
      throws IOException {
    FileSystem fs = null;
    try {
      String historyFilePath = jtClient.getProxy().
          getJobHistoryLocationForRetiredJob(simulatedJobID);
      Path jhpath = new Path(historyFilePath);
      fs = jhpath.getFileSystem(conf);
      JobHistoryParser jhparser = new JobHistoryParser(fs, jhpath);
      JobHistoryParser.JobInfo jhInfo = jhparser.parse();
      return jhInfo;

    } finally {
      fs.close();
    }
  }
  
  /**
   * Get the user resolver of a job.
   */
  public String getJobUserResolver() {
    return userResolverVal;
  }

  private String convertJobStatus(String jobStatus) {
    if (jobStatus.equals("SUCCEEDED")) { 
      return "SUCCESS";
    } else {
      return jobStatus;
    }
  }
  
  private String convertBytes(long bytesValue) {
    int units = 1024;
    if( bytesValue < units ) {
      return String.valueOf(bytesValue)+ "B";
    } else {
      // it converts the bytes into either KB or MB or GB or TB etc.
      int exp = (int)(Math.log(bytesValue) / Math.log(units));
      return String.format("%1d%sB",(long)(bytesValue / Math.pow(units, exp)), 
          "KMGTPE".charAt(exp -1));
    }
  }
 

  private long getCounterValue(Counters counters, String key) 
     throws ParseException { 
    for (String groupName : counters.getGroupNames()) {
       CounterGroup totalGroup = counters.getGroup(groupName);
       Iterator<Counter> itrCounter = totalGroup.iterator();
       while (itrCounter.hasNext()) {
         Counter counter = itrCounter.next();
         if (counter.getName().equals(key)) {
           return counter.getValue();
         }
       }
    }
    return 0;
  }
}

