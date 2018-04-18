/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.ssl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestRevocationListFetcher {
  private static final Logger LOG = LogManager.getLogger(TestRevocationListFetcher.class);
  
  private static final String BASE_DIR = Paths.get(
      System.getProperty("test.build.dir", Paths.get("target", "test-dir").toString()),
      TestRevocationListFetcher.class.getSimpleName())
      .toString();
  private final File outputFile = Paths.get(BASE_DIR, "crl.pem").toFile();
  private final File baseDirFile = new File(BASE_DIR);
  
  private Configuration conf;
  
  @Rule
  public final ExpectedException rule = ExpectedException.none();
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    CRLFetcherFactory.getInstance().clearFetcherCache();
    baseDirFile.mkdirs();
  }
  
  @After
  public void tearDown() throws Exception {
    if (baseDirFile.exists()) {
      FileUtils.deleteDirectory(baseDirFile);
    }
    conf = null;
  }
  
  @Test
  @Ignore
  // This test requires the resource to be in a web-server, so normally we should ignore it
  public void testRemoteCRLFetcher() throws Exception {
    assertFalse(outputFile.exists());
    conf.set(CommonConfigurationKeys.HOPS_CRL_FETCHER_CLASS_KEY, "org.apache.hadoop.security.ssl.RemoteCRLFetcher");
    conf.set(CommonConfigurationKeys.HOPS_CRL_INPUT_URI_KEY, "http://bbc4.sics.se:34821/intermediate.crl.pem");
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, outputFile.getAbsolutePath());
    CRLFetcher fetcher = CRLFetcherFactory.getInstance().getCRLFetcher(conf);
    assertNotNull(fetcher);
    fetcher.fetch();
    assertTrue(outputFile.exists());
    CRLFetcher fetcher1 = CRLFetcherFactory.getInstance().getCRLFetcher(conf);
    assertEquals(fetcher, fetcher1);
  }
  
  @Test
  @Ignore
  // This test requires the resource to be in a web-server, so normally we should ignore it
  public void testHTTPSRemoteCRLFetcher() throws Exception {
    assertFalse(outputFile.exists());
    conf.set(CommonConfigurationKeysPublic.HOPS_CRL_FETCHER_CLASS_KEY, "org.apache.hadoop.security.ssl.DevRemoteCRLFetcher");
    conf.set(CommonConfigurationKeys.HOPS_CRL_INPUT_URI_KEY, "https://bbc4.sics.se:38591/intermediate.crl.pem");
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, outputFile.getAbsolutePath());
    CRLFetcher fetcher = CRLFetcherFactory.getInstance().getCRLFetcher(conf);
    assertNotNull(fetcher);
    fetcher.fetch();
    assertTrue(outputFile.exists());
  }
  
  @Test
  public void testDummyLocalCRLFetcher() throws Exception {
    String payload = "something_to_write";
    prepareForDummyCRLFetcher(payload);
    CRLFetcher fetcher = CRLFetcherFactory.getInstance().getCRLFetcher(conf);
    fetcher.fetch();
    assertTrue(outputFile.exists());
    String in = FileUtils.readFileToString(outputFile);
    assertEquals(payload, in);
  }
  
  @Test(timeout = 10000)
  public void testRevocationListFetcherService() throws Exception {
    String payload = "something_to_write";
    prepareForDummyCRLFetcher(payload);
    RevocationListFetcherService crlFetcherService = new RevocationListFetcherService();
    crlFetcherService.setIntervalTimeUnit(TimeUnit.SECONDS);
    conf.set(CommonConfigurationKeys.HOPS_CRL_FETCHER_INTERVAL_KEY, "1s");
    crlFetcherService.serviceInit(conf);
    crlFetcherService.serviceStart();
  
    TimeUnit.SECONDS.sleep(crlFetcherService.getFetcherInterval());
    long lastModified = outputFile.lastModified();
    assertTrue(outputFile.exists());
  
    TimeUnit.SECONDS.sleep(crlFetcherService.getFetcherInterval() + 1);
    assertTrue(outputFile.exists());
    assertTrue(outputFile.lastModified() > lastModified);
    
    crlFetcherService.serviceStop();
  }
  
  @Test(timeout = 40000)
  public void testFailedRevocationListFetcherService() throws Exception {
    conf.set(CommonConfigurationKeys.HOPS_CRL_FETCHER_CLASS_KEY, "org.apache.hadoop.security.ssl.RemoteCRLFetcher");
    conf.set(CommonConfigurationKeys.HOPS_CRL_INPUT_URI_KEY, "http://i_hope_this_does_not_exist.sics" +
        ".se:24376/intermediate.crl.pem");
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, outputFile.getAbsolutePath());
    conf.set(CommonConfigurationKeys.HOPS_CRL_FETCHER_INTERVAL_KEY, "1s");
    RevocationListFetcherService crlFetcherService = new RevocationListFetcherService();
    crlFetcherService.setIntervalTimeUnit(TimeUnit.SECONDS);
    crlFetcherService.serviceInit(conf);
    
    rule.expect(IllegalStateException.class);
    crlFetcherService.serviceStart();
  }
  
  @Test(timeout = 12000)
  public void testFailAfterAWhileFetcher() throws Exception {
    conf.set(CommonConfigurationKeysPublic.HOPS_CRL_FETCHER_CLASS_KEY, FailingCRLFetcher.class.getCanonicalName());
    conf.set(CommonConfigurationKeys.HOPS_CRL_INPUT_URI_KEY, "file://" + Paths.get(BASE_DIR, "something"));
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, "file://" + Paths.get(BASE_DIR, "something"));
    conf.set(CommonConfigurationKeysPublic.HOPS_CRL_FETCHER_INTERVAL_KEY, "1s");
    RevocationListFetcherService crlFetcherService = new RevocationListFetcherService();
    crlFetcherService.setIntervalTimeUnit(TimeUnit.SECONDS);
    crlFetcherService.serviceInit(conf);
    crlFetcherService.serviceStart();
    
    assertTrue(crlFetcherService.getFetcherThread().isAlive());
    
    TimeUnit.SECONDS.sleep(crlFetcherService.getFetcherInterval() * 8);
    
    // Fetcher thread should be dead by now
    assertFalse(crlFetcherService.getFetcherThread().isAlive());
    crlFetcherService.serviceStop();
  }
  
  private void prepareForDummyCRLFetcher(String payload) throws IOException {
    // Write some bytes to disk for the LocalDummyCRLFetcher
    byte[] bytes = payload.getBytes();
    File inputFile = Paths.get(BASE_DIR, "intermediate.crl.pem").toFile();
    FileUtils.writeByteArrayToFile(inputFile, bytes, false);
    conf.set(CommonConfigurationKeys.HOPS_CRL_FETCHER_CLASS_KEY, LocalDummyCRLFetcher.class.getCanonicalName());
    conf.set(CommonConfigurationKeys.HOPS_CRL_INPUT_URI_KEY, "file://" + inputFile.getAbsolutePath());
    conf.set(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, outputFile.getAbsolutePath());
  }
}
