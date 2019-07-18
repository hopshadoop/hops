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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.SendingRequestEvent;
import com.microsoft.azure.storage.StorageEvent;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.TestHookOperationContext;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.NO_ACCESS_TO_CONTAINER_MSG;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assume.assumeNotNull;

/**
 * Error handling.
 */
public class ITestAzureFileSystemErrorConditions extends
    AbstractWasbTestWithTimeout {
  private static final int ALL_THREE_FILE_SIZE = 1024;

  @Test
  public void testNoInitialize() throws Exception {
    intercept(AssertionError.class,
        new Callable<FileMetadata>() {
          @Override
          public FileMetadata call() throws Exception {
            return new AzureNativeFileSystemStore()
                .retrieveMetadata("foo");
          }
        });
  }

  /**
   * Try accessing an unauthorized or non-existent (treated the same) container
   * from WASB.
   */
  @Test
  public void testAccessUnauthorizedPublicContainer() throws Exception {
    final String container = "nonExistentContainer";
    final String account = "hopefullyNonExistentAccount";
    Path noAccessPath = new Path(
        "wasb://" + container + "@" + account + "/someFile");
    NativeAzureFileSystem.suppressRetryPolicy();
    try {
      FileSystem.get(noAccessPath.toUri(), new Configuration())
        .open(noAccessPath);
      assertTrue("Should've thrown.", false);
    } catch (AzureException ex) {
      GenericTestUtils.assertExceptionContains(
          String.format(NO_ACCESS_TO_CONTAINER_MSG, account, container), ex);
    } finally {
      NativeAzureFileSystem.resumeRetryPolicy();
    }
  }

  @Test
  public void testAccessContainerWithWrongVersion() throws Exception {
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    MockStorageInterface mockStorage = new MockStorageInterface();
    store.setAzureStorageInteractionLayer(mockStorage);
    try (FileSystem fs = new NativeAzureFileSystem(store)) {
      Configuration conf = new Configuration();
      AzureBlobStorageTestAccount.setMockAccountKey(conf);
      HashMap<String, String> metadata = new HashMap<String, String>();
      metadata.put(AzureNativeFileSystemStore.VERSION_METADATA_KEY,
          "2090-04-05"); // It's from the future!
      mockStorage.addPreExistingContainer(
          AzureBlobStorageTestAccount.getMockContainerUri(), metadata);

      AzureException ex = intercept(AzureException.class,
          new Callable<FileStatus[]>() {
            @Override
            public FileStatus[] call() throws Exception {
              fs.initialize(new URI(AzureBlobStorageTestAccount.MOCK_WASB_URI),
                  conf);
              return fs.listStatus(new Path("/"));
            }
          });
      GenericTestUtils.assertExceptionContains(
          "unsupported version: 2090-04-05.", ex);
    }
  }

  private interface ConnectionRecognizer {
    boolean isTargetConnection(HttpURLConnection connection);
  }

  private class TransientErrorInjector extends StorageEvent<SendingRequestEvent> {
    private final ConnectionRecognizer connectionRecognizer;
    private boolean injectedErrorOnce = false;

    public TransientErrorInjector(ConnectionRecognizer connectionRecognizer) {
      this.connectionRecognizer = connectionRecognizer;
    }

    @Override
    public void eventOccurred(SendingRequestEvent eventArg) {
      HttpURLConnection connection
          = (HttpURLConnection) eventArg.getConnectionObject();
      if (!connectionRecognizer.isTargetConnection(connection)) {
        return;
      }
      if (!injectedErrorOnce) {
        connection.setReadTimeout(1);
        connection.disconnect();
        injectedErrorOnce = true;
      }
    }
  }

  private void injectTransientError(NativeAzureFileSystem fs,
      final ConnectionRecognizer connectionRecognizer) {
    fs.getStore().addTestHookToOperationContext(new TestHookOperationContext() {
      @Override
      public OperationContext modifyOperationContext(OperationContext original) {
        original.getSendingRequestEventHandler().addListener(
            new TransientErrorInjector(connectionRecognizer));
        return original;
      }
    });
  }

  @Test
  public void testTransientErrorOnDelete() throws Exception {
    // Need to do this test against a live storage account
    AzureBlobStorageTestAccount testAccount =
        AzureBlobStorageTestAccount.create();
    assumeNotNull(testAccount);
    try {
      NativeAzureFileSystem fs = testAccount.getFileSystem();
      injectTransientError(fs, new ConnectionRecognizer() {
        @Override
        public boolean isTargetConnection(HttpURLConnection connection) {
          return connection.getRequestMethod().equals("DELETE");
        }
      });
      Path testFile = new Path("/a/b");
      assertTrue(fs.createNewFile(testFile));
      assertTrue(fs.rename(testFile, new Path("/x")));
    } finally {
      testAccount.cleanup();
    }
  }

  private void writeAllThreeFile(NativeAzureFileSystem fs, Path testFile)
      throws IOException {
    byte[] buffer = new byte[ALL_THREE_FILE_SIZE];
    Arrays.fill(buffer, (byte) 3);
    try(OutputStream stream = fs.create(testFile)) {
      stream.write(buffer);
    }
  }

  private void readAllThreeFile(NativeAzureFileSystem fs, Path testFile)
      throws IOException {
    byte[] buffer = new byte[ALL_THREE_FILE_SIZE];
    InputStream inStream = fs.open(testFile);
    assertEquals(buffer.length,
        inStream.read(buffer, 0, buffer.length));
    inStream.close();
    for (int i = 0; i < buffer.length; i++) {
      assertEquals(3, buffer[i]);
    }
  }

  @Test
  public void testTransientErrorOnCommitBlockList() throws Exception {
    // Need to do this test against a live storage account
    AzureBlobStorageTestAccount testAccount =
        AzureBlobStorageTestAccount.create();
    assumeNotNull(testAccount);
    try {
      NativeAzureFileSystem fs = testAccount.getFileSystem();
      injectTransientError(fs, new ConnectionRecognizer() {
        @Override
        public boolean isTargetConnection(HttpURLConnection connection) {
          return connection.getRequestMethod().equals("PUT")
              && connection.getURL().getQuery() != null
              && connection.getURL().getQuery().contains("blocklist");
        }
      });
      Path testFile = new Path("/a/b");
      writeAllThreeFile(fs, testFile);
      readAllThreeFile(fs, testFile);
    } finally {
      testAccount.cleanup();
    }
  }

  @Test
  public void testTransientErrorOnRead() throws Exception {
    // Need to do this test against a live storage account
    AzureBlobStorageTestAccount testAccount =
        AzureBlobStorageTestAccount.create();
    assumeNotNull(testAccount);
    try {
      NativeAzureFileSystem fs = testAccount.getFileSystem();
      Path testFile = new Path("/a/b");
      writeAllThreeFile(fs, testFile);
      injectTransientError(fs, new ConnectionRecognizer() {
        @Override
        public boolean isTargetConnection(HttpURLConnection connection) {
          return connection.getRequestMethod().equals("GET");
        }
      });
      readAllThreeFile(fs, testFile);
    } finally {
      testAccount.cleanup();
    }
  }
}
