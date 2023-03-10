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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CGroups2CpuResourceHandlerImpl extends BaseCGroupsCpuResourceHandler implements CpuResourceHandler {

  static final Integer DEFAULT_CPU_PERIOD = 100000;
  static final int DEFAULT_WEIGHT_OPPORTUNISTIC = 1;
  static final int DEFAULT_WEIGHT = 100;
  static final String UNLIMITED_QUOTA = "max";
  private static final Pattern CPU_MAX_PATTERN = Pattern.compile("^(.*)\\s+(.*)$");
  CGroups2CpuResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
    super(cGroupsHandler);
  }


  @Override
  String getCpuUnlimitedQuota() {
    return UNLIMITED_QUOTA;
  }

  @Override
  boolean doCpuLimitsExist(String path) throws IOException {
    Path cpuMax = Paths.get(path, String.format("%s.%s", CPU.getName(), CGroupsHandler.CpuParameters.MAX.getName()));
    if (cpuMax.toFile().exists()) {
      String[] values = parseCpuMax(cpuMax);
      return !values[0].equals("max");
    }
    return false;
  }

  @Override
  void updateCpuBandwidthParams(String groupId, String cpuQuotaUs, String cpuPeriodUs) throws ResourceHandlerException {
    String effectivePeriod = cpuPeriodUs.isEmpty() ? String.valueOf(DEFAULT_CPU_PERIOD) : cpuPeriodUs;
    String param = String.format("%s %s", cpuQuotaUs, effectivePeriod);
    cGroupsHandler.updateCGroupParam(CPU, groupId, CGroupsHandler.CpuParameters.MAX.getName(), param);
  }

  @Override
  void updateCpuWeightParam(String cgroupId, int weight) throws ResourceHandlerException {
    cGroupsHandler
        .updateCGroupParam(CPU, cgroupId,
            CGroupsHandler.CpuParameters.WEIGHT.getName(),
            String.valueOf(weight));
  }

  @Override
  int getDefaultOpportunisticWeight() {
    return DEFAULT_WEIGHT_OPPORTUNISTIC;
  }

  @Override
  int getDefaultWeight() {
    return DEFAULT_WEIGHT;
  }

  private String[] parseCpuMax(Path path) throws IOException {
    String content = FileUtils.readFileToString(path.toFile(), StandardCharsets.UTF_8).trim();
    Matcher matcher = CPU_MAX_PATTERN.matcher(content);
    if (matcher.find()) {
      return new String[] {
          matcher.group(1),
          matcher.group(2)
      };
    }
    throw new IOException("Failed to parse " + path);
  }
}
