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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

public class MockRepairManager extends BlockRepairManager {
  private static final List<Report> EMPTY_REPORT = new ArrayList<>(0);

  public MockRepairManager(Configuration conf) {
    super(conf);
  }

  @Override
  public void repairSourceBlocks(String codecId, Path sourceFile,
      Path parityFile) {

  }

  @Override
  public void repairParityBlocks(String codecId, Path sourceFile,
      Path parityFile) {

  }

  @Override
  public List<Report> computeReports() {
    return EMPTY_REPORT;
  }

  @Override
  public void cancelAll() {

  }

  @Override
  public void cancel(String toCancel) {

  }
}
