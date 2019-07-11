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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * Implementation of {@link BaseEncodingManager} that performs raiding locally.
 */
public class LocalEncodingManager extends BaseEncodingManager {

  public static final Log LOG = LogFactory.getLog(LocalEncodingManager.class);

  public LocalEncodingManager(Configuration conf) throws IOException {
    super(conf);

    LOG.info("created");
  }

  @Override
  public void encodeFile(EncodingPolicy policy, Path sourceFile,
      Path parityFile, boolean copy) {
    Codec codec = Codec.getCodec(policy.getCodec());
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
      doRaid(conf, policyInfo);
    } catch (IOException e) {
      LOG.error("Exception", e);
    }
  }

  @Override
  public List<Report> computeReports() {
    throw new NotImplementedException("not implemented");
  }

  @Override
  public void cancelAll() {
    throw new NotImplementedException("not implemented");
  }

  @Override
  public void cancel(String toCancel) {
    throw new NotImplementedException("not implemented");
  }
}
