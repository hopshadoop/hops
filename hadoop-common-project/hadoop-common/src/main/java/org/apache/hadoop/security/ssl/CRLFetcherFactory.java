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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Factory for CRL fetchers used by RevocationListFetcherService
 */
public class CRLFetcherFactory {
  
  private CRLFetcher fetcher = null;
  private static volatile CRLFetcherFactory instance;
  
  private CRLFetcherFactory() {
  }
  
  public static CRLFetcherFactory getInstance() {
    if (instance == null) {
      synchronized (CRLFetcherFactory.class) {
        if (instance == null) {
          instance = new CRLFetcherFactory();
        }
      }
    }
    
    return instance;
  }
  
  /**
   * Get a CRL fetcher based on the configuration
   * @param conf Hadoop configuration
   * @return CRLFetcher instance
   * @throws ClassNotFoundException
   * @throws URISyntaxException
   */
  public synchronized CRLFetcher getCRLFetcher(Configuration conf) throws ClassNotFoundException, URISyntaxException {
    
    String crlFetcherClass = conf.get(CommonConfigurationKeys.HOPS_CRL_FETCHER_CLASS_KEY, CommonConfigurationKeys
        .HOPS_CRL_FETCHER_CLASS_DEFAULT);
    if (fetcher != null) {
      return fetcher;
    }
    
    Class<?> clazz = conf.getClassByName(crlFetcherClass);
    fetcher = (CRLFetcher) ReflectionUtils.newInstance(clazz, conf);
    setInputURI(conf, fetcher);
    setOutputURI(conf, fetcher);
    return fetcher;
  }
  
  @VisibleForTesting
  public void clearFetcherCache() {
    fetcher = null;
  }
  
  private void setOutputURI(Configuration conf, CRLFetcher fetcher) throws URISyntaxException {
    String outputString = conf.get(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY,
        CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_DEFAULT);
    URI outputURI = new URI(outputString);
    // If scheme is missing, assume it is the local filesystem
    if (outputURI.getScheme() == null) {
      outputURI = new URI("file://" + outputString);
    }
    fetcher.setOutputURI(outputURI);
  }
  
  private void setInputURI(Configuration conf, CRLFetcher fetcher) throws URISyntaxException {
    String inputURI = conf.get(CommonConfigurationKeys.HOPS_CRL_INPUT_URI_KEY);
    fetcher.setInputURI(new URI(inputURI));
  }
}
