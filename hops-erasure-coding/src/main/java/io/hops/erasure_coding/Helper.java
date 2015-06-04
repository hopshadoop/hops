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
import org.apache.hadoop.fs.ErasureCodingFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;

public class Helper {

  public static DistributedFileSystem getDFS(Configuration conf, Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    DistributedFileSystem dfs =(DistributedFileSystem)
        (fs instanceof ErasureCodingFileSystem ?
            ((ErasureCodingFileSystem) fs).getFileSystem() : fs);
    return dfs;
  }
}
