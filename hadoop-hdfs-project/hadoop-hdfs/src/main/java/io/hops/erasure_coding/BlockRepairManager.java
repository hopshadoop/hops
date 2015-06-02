/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public abstract class BlockRepairManager extends Configured
    implements Cancelable<String> {

  public BlockRepairManager(Configuration conf) {
    super(conf);
  }

  public abstract void repairSourceBlocks(String codecId, Path sourceFile,
      Path parityFile) throws IOException;

  public abstract void repairParityBlocks(String codecId, Path sourceFile,
      Path parityFile) throws IOException;

  public abstract List<Report> computeReports() throws IOException;
}
