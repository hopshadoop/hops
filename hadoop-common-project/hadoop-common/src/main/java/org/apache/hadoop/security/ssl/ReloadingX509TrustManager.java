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

package org.apache.hadoop.security.ssl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.ExponentialBackOff;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TrustManager} implementation that reloads its configuration when
 * the truststore file on disk changes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ReloadingX509TrustManager
  implements X509TrustManager, Runnable {

  @VisibleForTesting
  static final Log LOG = LogFactory.getLog(ReloadingX509TrustManager.class);
  @VisibleForTesting
  static final String RELOAD_ERROR_MESSAGE =
      "Could not load truststore (keep using existing one) : ";

  private String type;
  private File file;
  private String password;
  private final File passwordFileLocation;
  private long lastLoaded;
  private long reloadInterval;
  private AtomicReference<X509TrustManager> trustManagerRef;

  private WeakReference<ScheduledFuture> reloader = null;
  private final BackOff backOff;
  private long backOffTimeout = 0L;
  
  private int numberOfFailures = 0;
  
  /**
   * Creates a reloadable trustmanager. The trustmanager reloads itself
   * if the underlying trustore file has changed.
   *
   * @param type type of truststore file, typically 'jks'.
   * @param location local path to the truststore file.
   * @param password password of the truststore file.
   * @param reloadInterval interval to check if the truststore file has
   * changed, in milliseconds.
   * @throws IOException thrown if the truststore could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be
   * initialized due to a security error.
   */
  public ReloadingX509TrustManager(String type, String location, String password, long reloadInterval)
    throws IOException, GeneralSecurityException {
    this(type, location, password, null, reloadInterval);
  }
  
  /**
   * Creates a reloadable trustmanager. The trustmanager reloads itself
   * if the underlying trustore file has changed.
   *
   * @param type type of truststore file, typically 'jks'.
   * @param location local path to the truststore file.
   * @param password password of the truststore file.
   * @param passwordFileLocation path to the file containing the password
   * @param reloadInterval interval to check if the truststore file has
   * changed, in milliseconds.
   * @throws IOException thrown if the truststore could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be
   * initialized due to a security error.
   */
  public ReloadingX509TrustManager(String type, String location, String password, String passwordFileLocation,
      long reloadInterval)
    throws IOException, GeneralSecurityException {
    this.type = type;
    file = new File(location);
    this.password = password;
    if (passwordFileLocation != null) {
      this.passwordFileLocation = new File(passwordFileLocation);
    } else {
      this.passwordFileLocation = null;
    }
    trustManagerRef = new AtomicReference<X509TrustManager>();
    trustManagerRef.set(loadTrustManager());
    this.reloadInterval = reloadInterval;
    this.backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(50)
        .setMaximumIntervalMillis(2000)
        .setMaximumRetries(KeyManagersReloaderThreadPool.MAX_NUMBER_OF_RETRIES)
        .build();
  }

  /**
   * Starts the reloader thread.
   */
  public void init() {
    if (reloader == null) {
      ScheduledFuture task = KeyManagersReloaderThreadPool.getInstance().scheduleTask(this, reloadInterval, TimeUnit
          .MILLISECONDS);
      reloader = new WeakReference<>(task);
    }
  }

  /**
   * Stops the reloader thread.
   */
  public void destroy() {
    if (reloader != null) {
      ScheduledFuture task = reloader.get();
      if (task != null) {
        task.cancel(true);
        reloader = null;
      }
    }
  }

  /**
   * Returns the reload check interval.
   *
   * @return the reload check interval, in milliseconds.
   */
  public long getReloadInterval() {
    return reloadInterval;
  }

  public int getNumberOfFailures() {
    return numberOfFailures;
  }
  
  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
    throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkClientTrusted(chain, authType);
    } else {
      throw new CertificateException("Unknown client chain certificate: " +
                                     chain[0].toString());
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
    throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkServerTrusted(chain, authType);
    } else {
      throw new CertificateException("Unknown server chain certificate: " +
                                     chain[0].toString());
    }
  }

  private static final X509Certificate[] EMPTY = new X509Certificate[0];
  @Override
  public X509Certificate[] getAcceptedIssuers() {
    X509Certificate[] issuers = EMPTY;
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      issuers = tm.getAcceptedIssuers();
    }
    return issuers;
  }

  boolean needsReload() {
    boolean reload = true;
    if (file.exists()) {
      if (file.lastModified() == lastLoaded) {
        reload = false;
      }
    } else {
      lastLoaded = 0;
    }
    return reload;
  }

  X509TrustManager loadTrustManager() throws IOException, GeneralSecurityException {
    X509TrustManager trustManager = null;
    KeyStore ks = KeyStore.getInstance(type);
    String tstorePassword;
    if (passwordFileLocation != null) {
      tstorePassword = FileUtils.readFileToString(passwordFileLocation).trim();
    } else {
      tstorePassword = password;
    }
    FileInputStream in = new FileInputStream(file);
    try {
      ks.load(in, tstorePassword.toCharArray());
      lastLoaded = file.lastModified();
      LOG.debug("Loaded truststore '" + file + "'");
    } finally {
      in.close();
    }

    TrustManagerFactory trustManagerFactory = 
      TrustManagerFactory.getInstance(SSLFactory.SSLCERTIFICATE);
    trustManagerFactory.init(ks);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    for (TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof X509TrustManager) {
        trustManager = (X509TrustManager) trustManager1;
        break;
      }
    }
    return trustManager;
  }

  private boolean hasFailed() {
    return backOffTimeout > 0;
  }
  
  @Override
  public void run() {
    if (needsReload()) {
      try {
        TimeUnit.MILLISECONDS.sleep(backOffTimeout);
        trustManagerRef.set(loadTrustManager());
        if (hasFailed()) {
          backOff.reset();
          numberOfFailures = 0;
          backOffTimeout = 0L;
        }
      } catch (Exception ex) {
        backOffTimeout = backOff.getBackOffInMillis();
        numberOfFailures++;
        if (backOffTimeout != -1) {
          LOG.warn(RELOAD_ERROR_MESSAGE + ex.toString() + " trying again in " + backOffTimeout + " ms");
        } else {
          LOG.error(RELOAD_ERROR_MESSAGE + ", stop retrying", ex);
          destroy();
        }
      }
    }
  }

}
