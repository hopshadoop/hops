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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.CRL;
import java.security.cert.CRLException;
import java.security.cert.CertPathBuilder;
import java.security.cert.CertPathBuilderResult;
import java.security.cert.CertPathValidator;
import java.security.cert.CertStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class to validate a certificate chain against a CRL in the local filesystem
 * The CRL and the Trust Store are reloadable
 */
public class CRLValidator {
  private final static Logger LOG = LogManager.getLogger(CRLValidator.class);
  
  private final Configuration conf;
  private final Configuration sslConf;
  private final Path crl;
  private final File trustStoreLocation;
  private final AtomicReference<Collection<? extends CRL>> crlReference;
  private final AtomicReference<KeyStore> trustStoreReference;
  
  private CertPathBuilder certPathBuilder;
  private CertPathValidator certPathValidator;
  private CertificateFactory certificateFactory;
  
  private TimeUnit reloadTimeunit;
  private long reloadInterval = -1L;
  private long crlLastLoadedTimestamp = 0L;
  private long trustStoreLastLoadedTimestamp = 0L;
  private Thread reloaderThread;
  private final RetryAction<Collection<? extends CRL>> loadCRLWithRetry;
  private final RetryAction<KeyStore> loadTruststoreWithRetry;
  
  CRLValidator(Configuration conf) throws IOException, GeneralSecurityException {
    this(conf, null);
  }
  
  CRLValidator(Configuration conf, Configuration sslConf) throws IOException, GeneralSecurityException {
    this.conf = conf;
    if (sslConf != null) {
      this.sslConf = sslConf;
    } else {
      this.sslConf = readSSLConfiguration();
    }
    Security.setProperty("ocsp.enable", "false");
    // That is to check the crlDistributionPoint of X.509 certificate
    System.setProperty("com.sun.security.enableCRLDP", "false");
    certPathBuilder = CertPathBuilder.getInstance("PKIX");
    certPathValidator = CertPathValidator.getInstance("PKIX");
    certificateFactory = CertificateFactory.getInstance("X.509");
    
    loadCRLWithRetry = new RetryAction<Collection<? extends CRL>>() {
      @Override
      Collection<? extends CRL> operationToPerform() throws GeneralSecurityException, IOException {
        return loadCRL();
      }
    };
    
    crl = Paths.get(conf.get(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, CommonConfigurationKeys
        .HOPS_CRL_OUTPUT_FILE_DEFAULT));
    crlReference = new AtomicReference<Collection<? extends CRL>>(loadCRLWithRetry.retry());
    
    loadTruststoreWithRetry = new RetryAction<KeyStore>() {
      @Override
      KeyStore operationToPerform() throws GeneralSecurityException, IOException {
        return loadTruststore();
      }
    };
    
    trustStoreLocation = new File(this.sslConf.get(FileBasedKeyStoresFactory.resolvePropertyName(
        SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY)));
    trustStoreReference = new AtomicReference<>(loadTruststoreWithRetry.retry());
  }
  
  public void startReloadingThread() {
    if (reloadTimeunit == null) {
      reloadTimeunit = TimeUnit.MINUTES;
    }
    
    if (reloadInterval == -1L) {
      reloadInterval = 60;
    }
    
    reloaderThread = new ReloaderThread();
    reloaderThread.setName("CRL Validator reloader thread");
    reloaderThread.setDaemon(true);
    reloaderThread.start();
  }
  
  public void stopReloaderThread() {
    if (reloaderThread != null) {
      reloaderThread.interrupt();
    }
  }
  
  @VisibleForTesting
  public void setReloadTimeunit(TimeUnit reloadTimeunit) {
    this.reloadTimeunit = reloadTimeunit;
  }
  
  public void setReloadInterval(long reloadInterval) {
    this.reloadInterval = reloadInterval;
  }
  
  @VisibleForTesting
  public void setCertPathBuilder(CertPathBuilder certPathBuilder) {
    this.certPathBuilder = certPathBuilder;
  }
  
  @VisibleForTesting
  public void setCertPathValidator(CertPathValidator certPathValidator) {
    this.certPathValidator = certPathValidator;
  }
  
  @VisibleForTesting
  public void setCertificateFactory(CertificateFactory certificateFactory) {
    this.certificateFactory = certificateFactory;
  }
  
  @VisibleForTesting
  public TimeUnit getReloadTimeunit() {
    return reloadTimeunit;
  }
  
  @VisibleForTesting
  public long getReloadInterval() {
    return reloadInterval;
  }
  
  /**
   * Validates a client certificate chain against a Certificate Revocation List
   * @param certificateChain Client's certificate chain
   * @throws CertificateException In case client's certificate has been revoked
   */
  public void validate(Certificate[] certificateChain) throws CertificateException {
    try {
      List<X509Certificate> certificateList = new ArrayList<>(certificateChain.length);
      for (Certificate cert : certificateChain) {
        if (!(cert instanceof X509Certificate)) {
          throw new IllegalStateException("Certificate type in chain is not X.509");
        }
        certificateList.add((X509Certificate) cert);
      }
      
      if (certificateList.isEmpty()) {
        throw new IllegalStateException("Certificate chain is empty");
      }
  
      X509CertSelector certificateSelector = new X509CertSelector();
      // First certificate in the certificate chain is always the client's certificate
      certificateSelector.setCertificate(certificateList.get(0));
  
      PKIXBuilderParameters params = new PKIXBuilderParameters(trustStoreReference.get(), certificateSelector);
      params.addCertStore(CertStore.getInstance("Collection", new CollectionCertStoreParameters(certificateList)));
      params.setMaxPathLength(-1);
      params.setRevocationEnabled(true);
      params.addCertStore(CertStore.getInstance("Collection", new CollectionCertStoreParameters(crlReference.get())));
  
      LOG.debug("Validating certificate " + certificateSelector.getCertificate());
      CertPathBuilderResult builderResult = certPathBuilder.build(params);
      
      certPathValidator.validate(builderResult.getCertPath(), params);
      LOG.debug("Certificate " + certificateSelector.getCertificate() + " is valid");
    } catch (GeneralSecurityException gse) {
      LOG.debug("Could not validate certificate against CRL", gse);
      throw new CertificateException(gse);
    }
  }
  
  private Configuration readSSLConfiguration() {
    Configuration sslConf = new Configuration(false);
    String sslConfResource = conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml");
    sslConf.addResource(sslConfResource);
    return sslConf;
  }
  
  private KeyStore loadTruststore() throws GeneralSecurityException, IOException {
    String type = sslConf.get(FileBasedKeyStoresFactory.resolvePropertyName(
        SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_TYPE_TPL_KEY),
        FileBasedKeyStoresFactory.DEFAULT_KEYSTORE_TYPE);
    String trustStorePassword = sslConf.get(FileBasedKeyStoresFactory.resolvePropertyName(
        SSLFactory.Mode.SERVER, FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY));
    
    KeyStore trustStore = KeyStore.getInstance(type);
    try (FileInputStream in = new FileInputStream(trustStoreLocation)) {
      trustStore.load(in, trustStorePassword.toCharArray());
    }
    trustStoreLastLoadedTimestamp = trustStoreLocation.lastModified();
    return trustStore;
  }
  
  private Collection<? extends CRL> loadCRL() throws IOException, CertificateException, CRLException {
    try (InputStream in = Files.newInputStream(crl, StandardOpenOption.READ)) {
      crlLastLoadedTimestamp = crl.toFile().lastModified();
      return certificateFactory.generateCRLs(in);
    }
  }
  
  private abstract class RetryAction<T> {
    private int numberOfFailures;
    
    private RetryAction() {
      numberOfFailures = 0;
    }
    
    abstract T operationToPerform() throws GeneralSecurityException, IOException;
    
    private T retry() throws GeneralSecurityException, IOException {
      while (true) {
        try {
          return operationToPerform();
        } catch (GeneralSecurityException | IOException ex) {
          if (numberOfFailures > 5) {
            throw ex;
          } else {
            numberOfFailures++;
            try {
              TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException iex) {
              throw new IOException(iex);
            }
          }
        }
      }
    }
  }
  
  private class ReloaderThread extends Thread {
    
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          reloadTimeunit.sleep(reloadInterval);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
        
        if (crlNeedsReload()) {
          try {
            crlReference.set(loadCRLWithRetry.retry());
          } catch (GeneralSecurityException | IOException ex) {
            LOG.error("Could not reload CRL", ex);
          }
        }
        if (trustStoreNeedsReload()) {
          try {
            trustStoreReference.set(loadTruststoreWithRetry.retry());
          } catch (GeneralSecurityException | IOException ex) {
            LOG.error("Could not reload TrustStore", ex);
          }
        }
      }
    }
    
    private boolean crlNeedsReload() {
      if (crl.toFile().exists()) {
        if (crl.toFile().lastModified() > crlLastLoadedTimestamp) {
          return true;
        }
      }
      return false;
    }
    
    private boolean trustStoreNeedsReload() {
      if (trustStoreLocation.exists()) {
        if (trustStoreLocation.lastModified() > trustStoreLastLoadedTimestamp) {
          return true;
        }
      }
      return false;
    }
  }
}
