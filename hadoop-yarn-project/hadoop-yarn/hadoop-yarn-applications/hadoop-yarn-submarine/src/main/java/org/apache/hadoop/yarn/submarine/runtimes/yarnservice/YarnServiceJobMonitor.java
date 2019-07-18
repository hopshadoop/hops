/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.api.JobStatus;
import org.apache.hadoop.yarn.submarine.common.api.builder.JobStatusBuilder;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;

import java.io.IOException;

public class YarnServiceJobMonitor extends JobMonitor {
  private ServiceClient serviceClient = null;

  public YarnServiceJobMonitor(ClientContext clientContext) {
    super(clientContext);
  }

  @Override
  public synchronized JobStatus getTrainingJobStatus(String jobName)
      throws IOException, YarnException {
    if (this.serviceClient == null) {
      this.serviceClient = YarnServiceUtils.createServiceClient(
          clientContext.getYarnConfig());
    }

    Service serviceSpec = this.serviceClient.getStatus(jobName);
    JobStatus jobStatus = JobStatusBuilder.fromServiceSpec(serviceSpec);
    return jobStatus;
  }
}
