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

package org.apache.hadoop.yarn.server.metrics;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
public class ApplicationMetricsConstants {

  public static final String ENTITY_TYPE =
      "YARN_APPLICATION";

  public static final String CREATED_EVENT_TYPE =
      "YARN_APPLICATION_CREATED";

  public static final String FINISHED_EVENT_TYPE =
      "YARN_APPLICATION_FINISHED";

  public static final String ACLS_UPDATED_EVENT_TYPE =
      "YARN_APPLICATION_ACLS_UPDATED";

  public static final String NAME_ENTITY_INFO =
      "YARN_APPLICATION_NAME";

  public static final String TYPE_ENTITY_INFO =
      "YARN_APPLICATION_TYPE";

  public static final String USER_ENTITY_INFO =
      "YARN_APPLICATION_USER";

  public static final String QUEUE_ENTITY_INFO =
      "YARN_APPLICATION_QUEUE";

  public static final String SUBMITTED_TIME_ENTITY_INFO =
      "YARN_APPLICATION_SUBMITTED_TIME";

  public static final String APP_VIEW_ACLS_ENTITY_INFO =
      "YARN_APPLICATION_VIEW_ACLS";

  public static final String DIAGNOSTICS_INFO_EVENT_INFO =
      "YARN_APPLICATION_DIAGNOSTICS_INFO";

  public static final String FINAL_STATUS_EVENT_INFO =
      "YARN_APPLICATION_FINAL_STATUS";

  public static final String STATE_EVENT_INFO =
      "YARN_APPLICATION_STATE";
  
  public static final String APP_CPU_METRICS =
      "YARN_APPLICATION_CPU_METRIC";
  
  public static final String APP_GPU_METRICS =
      "YARN_APPLICATION_GPU_METRIC";
  
  public static final String APP_MEM_METRICS =
      "YARN_APPLICATION_MEM_METRIC";

  public static final String LATEST_APP_ATTEMPT_EVENT_INFO =
      "YARN_APPLICATION_LATEST_APP_ATTEMPT";

}
