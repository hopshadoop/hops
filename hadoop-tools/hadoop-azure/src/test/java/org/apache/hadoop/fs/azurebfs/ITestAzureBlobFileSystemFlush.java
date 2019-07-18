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

package org.apache.hadoop.fs.azurebfs;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.IOException;

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.azurebfs.services.AuthType;

/**
 * Test flush operation.
 * This class cannot be run in parallel test mode--check comments in
 * testWriteHeavyBytesToFileSyncFlush().
 */
public class ITestAzureBlobFileSystemFlush extends AbstractAbfsScaleTest {
  private static final int BASE_SIZE = 1024;
  private static final int ONE_THOUSAND = 1000;
  private static final int TEST_BUFFER_SIZE = 5 * ONE_THOUSAND * BASE_SIZE;
  private static final int ONE_MB = 1024 * 1024;
  private static final int FLUSH_TIMES = 200;
  private static final int THREAD_SLEEP_TIME = 1000;

  private static final int TEST_FILE_LENGTH = 1024 * 1024 * 8;
  private static final int WAITING_TIME = 1000;

  public ITestAzureBlobFileSystemFlush() throws Exception {
    super();
  }

  @Test
  public void testAbfsOutputStreamAsyncFlushWithRetainUncommittedData() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());
    final byte[] b;
    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);

      for (int i = 0; i < 2; i++) {
        stream.write(b);

        for (int j = 0; j < FLUSH_TIMES; j++) {
          stream.flush();
          Thread.sleep(10);
        }
      }
    }

    final byte[] r = new byte[TEST_BUFFER_SIZE];
    try (FSDataInputStream inputStream = fs.open(testFilePath, 4 * ONE_MB)) {
      while (inputStream.available() != 0) {
        int result = inputStream.read(r);

        assertNotEquals("read returned -1", -1, result);
        assertArrayEquals("buffer read from stream", r, b);
      }
    }
  }

  @Test
  public void testAbfsOutputStreamSyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());

    final byte[] b;
    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);
      stream.write(b);

      for (int i = 0; i < FLUSH_TIMES; i++) {
        stream.hsync();
        stream.hflush();
        Thread.sleep(10);
      }
    }

    final byte[] r = new byte[TEST_BUFFER_SIZE];
    try (FSDataInputStream inputStream = fs.open(testFilePath, 4 * ONE_MB)) {
      int result = inputStream.read(r);

      assertNotEquals(-1, result);
      assertArrayEquals(r, b);
    }
  }


  @Test
  public void testWriteHeavyBytesToFileSyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path testFilePath = path(methodName.getMethodName());
    ExecutorService es;
    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      es = Executors.newFixedThreadPool(10);

      final byte[] b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);

      List<Future<Void>> tasks = new ArrayList<>();
      for (int i = 0; i < FLUSH_TIMES; i++) {
        Callable<Void> callable = new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            stream.write(b);
            return null;
          }
        };

        tasks.add(es.submit(callable));
      }

      boolean shouldStop = false;
      while (!shouldStop) {
        shouldStop = true;
        for (Future<Void> task : tasks) {
          if (!task.isDone()) {
            stream.hsync();
            shouldStop = false;
            Thread.sleep(THREAD_SLEEP_TIME);
          }
        }
      }

      tasks.clear();
    }

    es.shutdownNow();
    FileStatus fileStatus = fs.getFileStatus(testFilePath);
    long expectedWrites = (long) TEST_BUFFER_SIZE * FLUSH_TIMES;
    assertEquals("Wrong file length in " + testFilePath, expectedWrites, fileStatus.getLen());
  }

  @Test
  public void testWriteHeavyBytesToFileAsyncFlush() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    ExecutorService es = Executors.newFixedThreadPool(10);

    final Path testFilePath = path(methodName.getMethodName());
    try (FSDataOutputStream stream = fs.create(testFilePath)) {

      final byte[] b = new byte[TEST_BUFFER_SIZE];
      new Random().nextBytes(b);

      List<Future<Void>> tasks = new ArrayList<>();
      for (int i = 0; i < FLUSH_TIMES; i++) {
        Callable<Void> callable = new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            stream.write(b);
            return null;
          }
        };

        tasks.add(es.submit(callable));
      }

      boolean shouldStop = false;
      while (!shouldStop) {
        shouldStop = true;
        for (Future<Void> task : tasks) {
          if (!task.isDone()) {
            stream.flush();
            shouldStop = false;
          }
        }
      }
      Thread.sleep(THREAD_SLEEP_TIME);
      tasks.clear();
    }

    es.shutdownNow();
    FileStatus fileStatus = fs.getFileStatus(testFilePath);
    assertEquals((long) TEST_BUFFER_SIZE * FLUSH_TIMES, fileStatus.getLen());
  }

  @Test
  public void testFlushWithFlushEnabled() throws Exception {
    testFlush(true);
  }

  @Test
  public void testFlushWithFlushDisabled() throws Exception {
    testFlush(false);
  }

  private void testFlush(boolean flushEnabled) throws Exception {
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);

    final AzureBlobFileSystem fs = (AzureBlobFileSystem) getFileSystem();

    // Simulate setting "fs.azure.enable.flush" to true or false
    fs.getAbfsStore().getAbfsConfiguration().setEnableFlush(flushEnabled);

    final Path testFilePath = path(methodName.getMethodName());
    byte[] buffer = getRandomBytesArray();

    // The test case must write "fs.azure.write.request.size" bytes
    // to the stream in order for the data to be uploaded to storage.
    assertEquals(
        fs.getAbfsStore().getAbfsConfiguration().getWriteBufferSize(),
        buffer.length);

    try (FSDataOutputStream stream = fs.create(testFilePath)) {
      stream.write(buffer);

      // Write asynchronously uploads data, so we must wait for completion
      AbfsOutputStream abfsStream = (AbfsOutputStream) stream.getWrappedStream();
      abfsStream.waitForPendingUploads();

      // Flush commits the data so it can be read.
      stream.flush();

      // Verify that the data can be read if flushEnabled is true; and otherwise
      // cannot be read.
      validate(fs.open(testFilePath), buffer, flushEnabled);
    }
  }

  @Test
  public void testHflushWithFlushEnabled() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    byte[] buffer = getRandomBytesArray();
    String fileName = UUID.randomUUID().toString();
    final Path testFilePath = path(fileName);

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, testFilePath, buffer, true)) {
      stream.hflush();
      validate(fs, testFilePath, buffer, true);
    }
  }

  @Test
  public void testHflushWithFlushDisabled() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    byte[] buffer = getRandomBytesArray();
    final Path testFilePath = path(methodName.getMethodName());

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, testFilePath, buffer, false)) {
      stream.hflush();
      validate(fs, testFilePath, buffer, false);
    }
  }

  @Test
  public void testHsyncWithFlushEnabled() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    byte[] buffer = getRandomBytesArray();

    final Path testFilePath = path(methodName.getMethodName());

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, testFilePath, buffer, true)) {
      stream.hsync();
      validate(fs, testFilePath, buffer, true);
    }
  }

  @Test
  public void testStreamCapabilitiesWithFlushDisabled() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    byte[] buffer = getRandomBytesArray();

    final Path testFilePath = path(methodName.getMethodName());

    try (FSDataOutputStream stream = getStreamAfterWrite(fs, testFilePath, buffer, false)) {
      assertFalse(stream.hasCapability(StreamCapabilities.HFLUSH));
      assertFalse(stream.hasCapability(StreamCapabilities.HSYNC));
      assertFalse(stream.hasCapability(StreamCapabilities.DROPBEHIND));
      assertFalse(stream.hasCapability(StreamCapabilities.READAHEAD));
      assertFalse(stream.hasCapability(StreamCapabilities.UNBUFFER));
    }
  }

  @Test
  public void testStreamCapabilitiesWithFlushEnabled() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    byte[] buffer = getRandomBytesArray();
    final Path testFilePath = path(methodName.getMethodName());
    try (FSDataOutputStream stream = getStreamAfterWrite(fs, testFilePath, buffer, true)) {
      assertTrue(stream.hasCapability(StreamCapabilities.HFLUSH));
      assertTrue(stream.hasCapability(StreamCapabilities.HSYNC));
      assertFalse(stream.hasCapability(StreamCapabilities.DROPBEHIND));
      assertFalse(stream.hasCapability(StreamCapabilities.READAHEAD));
      assertFalse(stream.hasCapability(StreamCapabilities.UNBUFFER));
    }
  }

  @Test
  public void testHsyncWithFlushDisabled() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    byte[] buffer = getRandomBytesArray();
    final Path testFilePath = path(methodName.getMethodName());
    try (FSDataOutputStream stream = getStreamAfterWrite(fs, testFilePath, buffer, false)) {
      stream.hsync();
      validate(fs, testFilePath, buffer, false);
    }
  }

  private byte[] getRandomBytesArray() {
    final byte[] b = new byte[TEST_FILE_LENGTH];
    new Random().nextBytes(b);
    return b;
  }

  private FSDataOutputStream getStreamAfterWrite(AzureBlobFileSystem fs, Path path, byte[] buffer, boolean enableFlush) throws IOException {
    fs.getAbfsStore().getAbfsConfiguration().setEnableFlush(enableFlush);
    FSDataOutputStream stream = fs.create(path);
    stream.write(buffer);
    return stream;
  }

  private void validate(InputStream stream, byte[] writeBuffer, boolean isEqual)
      throws IOException {
    try {
      byte[] readBuffer = new byte[writeBuffer.length];

      int numBytesRead = stream.read(readBuffer, 0, readBuffer.length);

      if (isEqual) {
        assertArrayEquals(
            "Bytes read do not match bytes written.",
            writeBuffer,
            readBuffer);
      } else {
        assertThat(
            "Bytes read unexpectedly match bytes written.",
            readBuffer,
            IsNot.not(IsEqual.equalTo(writeBuffer)));
      }
    } finally {
      stream.close();
    }
  }
  private void validate(FileSystem fs, Path path, byte[] writeBuffer, boolean isEqual) throws IOException {
    String filePath = path.toUri().toString();
    try (FSDataInputStream inputStream = fs.open(path)) {
      byte[] readBuffer = new byte[TEST_FILE_LENGTH];
      int numBytesRead = inputStream.read(readBuffer, 0, readBuffer.length);
      if (isEqual) {
        assertArrayEquals(
                String.format("Bytes read do not match bytes written to %1$s", filePath), writeBuffer, readBuffer);
      } else {
        assertThat(
                String.format("Bytes read unexpectedly match bytes written to %1$s",
                        filePath),
                readBuffer,
                IsNot.not(IsEqual.equalTo(writeBuffer)));
      }
    }
  }
}
