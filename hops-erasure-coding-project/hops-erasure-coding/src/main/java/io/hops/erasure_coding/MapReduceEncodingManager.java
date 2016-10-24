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
import io.hops.metadata.hdfs.dal.EncodingJobsDataAccess;
import io.hops.metadata.hdfs.entity.EncodingJob;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link BaseEncodingManager} that uses map reduce jobs to
 * raid files.
 */
public class MapReduceEncodingManager extends BaseEncodingManager {

  public static final Log LOG =
      LogFactory.getLog(MapReduceEncodingManager.class);

  public static final String ENCODING_JOB_EXECUTION_LIMIT =
      "io.hops.erasure_coding.encoding_job_execution_limit";
  public static final long DEFAULT_ENCODING_JOB_EXECUTION_LIMIT =
      24L * 3600L * 1000L;

  private final long executionLimit;
  private Map<String, MapReduceEncoder> currentJobs =
      new HashMap<String, MapReduceEncoder>();
  private Collection<MapReduceEncoder> completedJobs =
      new ArrayList<MapReduceEncoder>();

  private boolean initialized = false;

  public MapReduceEncodingManager(Configuration conf) throws IOException {
    super(conf);
    executionLimit = conf.getLong(ENCODING_JOB_EXECUTION_LIMIT,
        DEFAULT_ENCODING_JOB_EXECUTION_LIMIT);
    LOG.info("created");
  }

  @Override
  public void encodeFile(EncodingPolicy policy, Path sourceFile,
      Path parityFile, boolean copy) {
    initialize();

    Codec codec = Codec.getCodec(policy.getCodec());
    LOG.info("Start encoding with policy: " + policy + " for source file " +
        sourceFile.toUri().getPath() + " and parity file " + parityFile +
        " copy " + copy);
    PolicyInfo policyInfo = new PolicyInfo();
    try {
      policyInfo.setSrcPath(sourceFile.toUri().getPath());
      policyInfo.setCodecId(codec.getId());
      policyInfo.setProperty(PolicyInfo.PROPERTY_PARITY_PATH,
          parityFile.toUri().getPath());
      policyInfo.setProperty(PolicyInfo.PROPERTY_REPLICATION,
          String.valueOf(policy.getTargetReplication()));
      policyInfo.setProperty(PolicyInfo.PROPERTY_PARITY_REPLICATION,
          String.valueOf(1));
      policyInfo.setProperty(PolicyInfo.PROPERTY_COPY, String.valueOf(copy));
      raidFiles(policyInfo);
    } catch (IOException e) {
      LOG.error("Exception", e);
    }
  }

  /**
   * {@inheritDocs}
   */
  public void raidFiles(PolicyInfo info) throws IOException {
    MapReduceEncoder dr = new MapReduceEncoder(conf);
    boolean started = dr.startDistRaid(info);
    if (started) {
      String path = info.getSrcPath().toUri().getPath();
      // There is a risk that it crashes before persisting the job for recovery.
      // The ErasureCodingManager should restart the job in that case as it
      // does not set the state to active before this code succeeded.
      persistActiveJob(path, dr.getJobID(),
          dr.getConf().get(MapReduceEncoder.JOB_DIR_LABEL));
      currentJobs.put(path, dr);
    }
  }

  @Override
  public List<Report> computeReports() throws IOException {
    initialize();
    cleanRecovery();

    List<Report> reports = new ArrayList<Report>(currentJobs.size());
    for (Map.Entry<String, MapReduceEncoder> entry : currentJobs.entrySet()) {
      String fileName = entry.getKey();
      MapReduceEncoder job = entry.getValue();
      try {
        if (job.checkComplete() && job.successful()) {
          reports.add(new Report(fileName, Report.Status.FINISHED));
          LOG.info("Encoding successful for job " + job.getJobID());
        } else if (job.checkComplete() && !job.successful()) {
          reports.add(new Report(fileName, Report.Status.FAILED));
          LOG.info("Encoding failed for job " + job.getJobID());
        } else if (System.currentTimeMillis() - job.getStartTime() >
            executionLimit) {
          job.killJob();
          reports.add(new Report(fileName, Report.Status.CANCELED));
          LOG.info("Encoding canceled for job " + job.getJobID());
        } else {
          reports.add(new Report(fileName, Report.Status.ACTIVE));
          LOG.info("Encoding active for job " + job.getJobID());
        }
      } catch (IOException e) {
        LOG.error("Exception during completeness check", e);
        try {
          job.killJob();
        } catch (IOException e1) {
        }
        reports.add(new Report(fileName, Report.Status.FAILED));
      }
    }

    for (Report report : reports) {
      Report.Status status = report.getStatus();
      if (status == Report.Status.FINISHED || status == Report.Status.FAILED ||
          status == Report.Status.CANCELED) {
        MapReduceEncoder job = currentJobs.remove(report.getFilePath());
        completedJobs.add(job);
      }
    }

    return reports;
  }

  private void persistActiveJob(final String path, final JobID jobId,
      final String jobDir) throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.PERSIST_ENCODING_JOB) {
      @Override
      public Object performTask() throws IOException {
        EncodingJobsDataAccess da = (EncodingJobsDataAccess)
            HdfsStorageFactory.getDataAccess(EncodingJobsDataAccess.class);
          da.add(new EncodingJob(jobId.getJtIdentifier(),
              jobId.getId(), path, jobDir));
        return null;
      }
    }.handle();
  }

  private void cleanRecovery() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DELETE_ENCODING_JOBS) {
      @Override
      public Object performTask() throws IOException {
        EncodingJobsDataAccess da = (EncodingJobsDataAccess)
            HdfsStorageFactory.getDataAccess(EncodingJobsDataAccess.class);
        Iterator<MapReduceEncoder> it = completedJobs.iterator();
        while (it.hasNext()) {
          MapReduceEncoder job = it.next();
          JobID jobId = job.getJobID();
          da.delete(new EncodingJob(jobId.getJtIdentifier(), jobId.getId()));
          it.remove();
        }
        return null;
      }
    }.handle();
  }

  @Override
  public void cancelAll() {
    initialize();
    for (MapReduceEncoder job : currentJobs.values()) {
      try {
        job.killJob();
      } catch (IOException e) {
        LOG.error("Exception", e);
      }
    }
    currentJobs.clear();
  }

  @Override
  public void cancel(String toCancel) {
    initialize();
    MapReduceEncoder job = currentJobs.get(toCancel);
    try {
      job.killJob();
    } catch (IOException e) {
      LOG.error("Exception", e);
    }
    currentJobs.remove(toCancel);
  }

  private void initialize() {
    if (initialized) {
      return;
    }

    try {
      for (EncodingJob job : recoverActiveJobs()) {
        MapReduceEncoder recovered = new MapReduceEncoder(conf, job);
        currentJobs.put(job.getPath(), recovered);
      }
    } catch (IOException e) {
      LOG.error("Encoding job recovery failed", e);
      throw new RuntimeException(e);
    }

    initialized = true;
  }

  private Collection<EncodingJob> recoverActiveJobs() throws IOException {
    LightWeightRequestHandler handler = new LightWeightRequestHandler(
        HDFSOperationType.RECOVER_ENCODING_JOBS) {
      @Override
      public Object performTask() throws IOException {
        EncodingJobsDataAccess da = (EncodingJobsDataAccess)
            HdfsStorageFactory.getDataAccess(EncodingJobsDataAccess.class);
        return da.findAll();
      }
    };

    return (Collection<EncodingJob>) handler.handle();
  }
}
