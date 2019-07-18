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

package org.apache.hadoop.fs.azurebfs.constants;

/**
 * Responsible to keep all the Azure Blob File System configurations keys in Hadoop configuration file.
 */
public final class TestConfigurationKeys {
  public static final String FS_AZURE_ACCOUNT_NAME = "fs.azure.account.name";
  public static final String FS_AZURE_ABFS_ACCOUNT_NAME = "fs.azure.abfs.account.name";
  public static final String FS_AZURE_ACCOUNT_KEY = "fs.azure.account.key";
  public static final String FS_AZURE_CONTRACT_TEST_URI = "fs.contract.test.fs.abfs";

  public static final String FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_ID = "fs.azure.account.oauth2.contributor.client.id";
  public static final String FS_AZURE_BLOB_DATA_CONTRIBUTOR_CLIENT_SECRET = "fs.azure.account.oauth2.contributor.client.secret";

  public static final String FS_AZURE_BLOB_DATA_READER_CLIENT_ID = "fs.azure.account.oauth2.reader.client.id";
  public static final String FS_AZURE_BLOB_DATA_READER_CLIENT_SECRET = "fs.azure.account.oauth2.reader.client.secret";

  public static final String TEST_CONFIGURATION_FILE_NAME = "azure-test.xml";
  public static final String TEST_CONTAINER_PREFIX = "abfs-testcontainer-";
  public static final int TEST_TIMEOUT = 15 * 60 * 1000;

  private TestConfigurationKeys() {}
}
