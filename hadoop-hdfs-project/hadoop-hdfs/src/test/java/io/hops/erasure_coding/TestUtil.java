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

import io.hops.metadata.hdfs.entity.EncodingPolicy;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.util.Random;

public class TestUtil {

  public static void createRandomFile(DistributedFileSystem dfs, Path path,
      long seed, int blockCount, int blockSize) throws IOException {

    FSDataOutputStream out =
        dfs.create(path, new EncodingPolicy("src", (short) 1));
    byte[] buffer = randomBytes(seed, blockCount, blockSize);
    out.write(buffer, 0, buffer.length);
    out.close();
  }

  public static byte[] randomBytes(long seed, int blockCount, int blockSize) {
    return randomBytes(seed, blockCount * blockSize);
  }

  public static byte[] randomBytes(long seed, int size) {
    final byte[] b = new byte[size];
    final Random rand = new Random(seed);
    rand.nextBytes(b);
    return b;
  }
}
