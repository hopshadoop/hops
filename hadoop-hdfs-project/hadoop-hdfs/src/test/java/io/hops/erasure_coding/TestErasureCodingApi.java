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
import io.hops.metadata.hdfs.entity.EncodingStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class TestErasureCodingApi extends BasicClusterTestCase {

  private final Path testFile = new Path("/test_file");

  @Test
  public void testCreateEncodedFile() throws IOException {
    FSDataOutputStream out = getDfs().create(testFile);
    out.close();

    assertNotNull(getDfs().getEncodingStatus(testFile.toUri().getPath()));
  }

  @Test
  public void testGetEncodingStatusIfRequested() throws IOException {
    Codec codec = Codec.getCodec("src");
    EncodingPolicy policy = new EncodingPolicy(codec.getId(), (short) 1);
    FSDataOutputStream out = getDfs().create(testFile, policy);
    out.close();

    EncodingStatus status =
        getDfs().getEncodingStatus(testFile.toUri().getPath());
    assertNotNull(status);
    assertEquals(EncodingStatus.Status.ENCODING_REQUESTED, status.getStatus());
  }

  public void testGetEncodingStatusForNonExistingFile() throws IOException {
    try {
      getDfs().getEncodingStatus("/DEAD_BEEF");
      fail();
    } catch (IOException e) {

    }
  }

  @Test
  public void testGetEncodingStatusIfNotRequested() throws IOException {
    FSDataOutputStream out = getDfs().create(testFile);
    out.close();

    EncodingStatus status =
        getDfs().getEncodingStatus(testFile.toUri().getPath());
    assertNotNull(status);
    assertEquals(EncodingStatus.Status.NOT_ENCODED, status.getStatus());
  }
}
