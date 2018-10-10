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
import java.security.cert.CRLException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509CRL;
import java.security.cert.X509CRLEntry;
import java.security.cert.X509Certificate;
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
  private final AtomicReference<X509CRL> crlReference;
  private final AtomicReference<KeyStore> trustStoreReference;
  
  private CertificateFactory certificateFactory;
  
  private TimeUnit reloadTimeunit;
  private long reloadInterval = -1L;
  private long crlLastLoadedTimestamp = 0L;
  private long trustStoreLastLoadedTimestamp = 0L;
  private Thread reloaderThread;
  private final RetryAction<X509CRL> loadCRLWithRetry;
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
    certificateFactory = CertificateFactory.getInstance("X.509");
    
    loadCRLWithRetry = new RetryAction<X509CRL>() {
      @Override
      X509CRL operationToPerform() throws GeneralSecurityException, IOException {
        return loadCRL();
      }
    };
    
    crl = Paths.get(conf.get(CommonConfigurationKeys.HOPS_CRL_OUTPUT_FILE_KEY, CommonConfigurationKeys
        .HOPS_CRL_OUTPUT_FILE_DEFAULT));
    crlReference = new AtomicReference<>(loadCRLWithRetry.retry());
    
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
    X509CRL crl = crlReference.get();
    for (Certificate certificate : certificateChain) {
      if (!(certificate instanceof X509Certificate)) {
        throw new CertificateException("Certificate is not X.509");
      }
      X509Certificate x509Certificate = (X509Certificate) certificate;
      X509CRLEntry crlEntry = crl.getRevokedCertificate(x509Certificate);
      if (crlEntry != null) {
        String revocationReason = crlEntry.getRevocationReason() != null
            ? " REASON: " + crlEntry.getRevocationReason().toString() : "";
        throw new CertificateException("HopsCRLValidator: Certificate " + x509Certificate.getSubjectDN().toString()
            + " has been revoked by " + crl.getIssuerX500Principal().getName()
            + revocationReason);
      }
    }
  
    LOG.debug("Certificate " + certificateChain[0] + " is valid");
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
  
  private X509CRL loadCRL() throws IOException, CertificateException, CRLException {
    try (InputStream in = Files.newInputStream(crl, StandardOpenOption.READ)) {
      crlLastLoadedTimestamp = crl.toFile().lastModified();
      return (X509CRL) certificateFactory.generateCRL(in);
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
