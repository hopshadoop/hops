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

import io.hops.metadata.hdfs.entity.EncodingPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

  private boolean initialized = false;

  public MapReduceEncodingManager(Configuration conf) throws IOException {
    super(conf);
    executionLimit = conf.getLong(ENCODING_JOB_EXECUTION_LIMIT,
        DEFAULT_ENCODING_JOB_EXECUTION_LIMIT);
    LOG.info("created");
  }

  @Override
  public void encodeFile(EncodingPolicy policy, Path sourceFile,
      Path parityFile) {
    if (!initialized) {
      try {
        cleanUpTempDirectory(conf);
      } catch (IOException e) {
        LOG.error("Cleanup tmp failed ", e);
      }
      initialized = true;
    }

    Codec codec = Codec.getCodec(policy.getCodec());
    LOG.info("Start encoding with policy: " + policy + " for source file " +
        sourceFile.toUri().getPath() + " and parity file " + parityFile);
    PolicyInfo policyInfo = new PolicyInfo();
    try {
      // This is somewhat redundant with the list below
      policyInfo.setSrcPath(sourceFile.toUri().getPath());
      policyInfo.setCodecId(codec.getId());
      policyInfo.setProperty("parityPath", parityFile.toUri().getPath());
      policyInfo.setProperty("targetReplication",
          String.valueOf(policy.getTargetReplication()));
      policyInfo.setProperty("metaReplication", String.valueOf(1));
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
      currentJobs.put(info.getSrcPath().toUri().getPath(), dr);
    }
  }

  @Override
  public List<Report> computeReports() {
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
        currentJobs.remove(report.getFilePath());
      }
    }

    return reports;
  }

  @Override
  public void cancelAll() {
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
    MapReduceEncoder job = currentJobs.get(toCancel);
    try {
      job.killJob();
    } catch (IOException e) {
      LOG.error("Exception", e);
    }
    currentJobs.remove(toCancel);
  }
}
