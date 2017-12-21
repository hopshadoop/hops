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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509KeyManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ReloadingX509KeyManager extends X509ExtendedKeyManager {
  private final Log LOG = LogFactory.getLog(ReloadingX509KeyManager.class);
  private final String type;
  private final File location;
  private final String keystorePassword;
  private final String keyPassword;
  private final long reloadInterval;
  private final TimeUnit reloadTimeUnit;
  private final ExecutorService exec;
  
  private AtomicReference<X509ExtendedKeyManager> keyManagerLocalRef;
  private long lastLoadedTimestamp;
  private Reloader reloader = null;
  // For testing
  private final AtomicBoolean fileExists = new AtomicBoolean(true);
  
  /**
   * Creates a reloadable keystore manager.
   *
   * @param type type of the keystore, jks
   * @param location path to keystore in the local file system
   * @param keystorePassword password of the keystore
   * @param keyPassword password of the key
   * @param reloadInterval interval to check if the keystore has altered (ms)
   */
  public ReloadingX509KeyManager(String type, String location, String keystorePassword, String keyPassword,
      long reloadInterval, TimeUnit reloadTimeUnit) throws GeneralSecurityException, IOException {
    this.type = type;
    this.location = new File(location);
    this.keystorePassword = keystorePassword;
    this.keyPassword = keyPassword;
    this.reloadInterval = reloadInterval;
    this.reloadTimeUnit = reloadTimeUnit;
    keyManagerLocalRef = new AtomicReference<>(loadKeyManager());
    exec = Executors.newSingleThreadExecutor();
  }
  
  /**
   * Starts the reloading thread
   */
  public void init() {
    if (reloader == null) {
      reloader = new Reloader();
      exec.submit(reloader);
    }
  }
  
  /**
   * Stops the reloading thread
   */
  public void stop() {
    exec.shutdownNow();
  }
  
  @VisibleForTesting
  public long getReloadInterval() {
    return reloadInterval;
  }
  
  @VisibleForTesting
  public TimeUnit getReloadTimeUnit() {
    return reloadTimeUnit;
  }
  
  @VisibleForTesting
  public AtomicBoolean getFileExists() {
    return fileExists;
  }
  
  private boolean needsReload() {
    if (location.exists()) {
      if (location.lastModified() > lastLoadedTimestamp) {
        return true;
      }
    } else {
      fileExists.set(false);
    }
    
    return false;
  }
  
  @Override
  public String[] getClientAliases(String s, Principal[] principals) {
    X509ExtendedKeyManager km = keyManagerLocalRef.get();
    if (km != null) {
      return km.getClientAliases(s, principals);
    }
    return null;
  }
  
  @Override
  public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
    X509ExtendedKeyManager km = keyManagerLocalRef.get();
    if (km != null) {
      return km.chooseClientAlias(strings, principals, socket);
    }
    return null;
  }
  
  @Override
  public String[] getServerAliases(String s, Principal[] principals) {
    X509ExtendedKeyManager km = keyManagerLocalRef.get();
    if (km != null) {
      return km.getServerAliases(s, principals);
    }
    return null;
  }
  
  @Override
  public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
    X509ExtendedKeyManager km = keyManagerLocalRef.get();
    if (km != null) {
      return km.chooseServerAlias(s, principals, socket);
    }
    return null;
  }
  
  @Override
  public X509Certificate[] getCertificateChain(String s) {
    X509ExtendedKeyManager km = keyManagerLocalRef.get();
    if (km != null) {
      return km.getCertificateChain(s);
    }
    return null;
  }
  
  @Override
  public PrivateKey getPrivateKey(String s) {
    X509ExtendedKeyManager km = keyManagerLocalRef.get();
    if (km != null) {
      return km.getPrivateKey(s);
    }
    return null;
  }
  
  @Override
  public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
    X509ExtendedKeyManager km = keyManagerLocalRef.get();
    if (km != null) {
      return km.chooseEngineClientAlias(keyType, issuers, engine);
    }
    return null;
  }
  
  @Override
  public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
    X509ExtendedKeyManager km = keyManagerLocalRef.get();
    if (km != null) {
      return km.chooseEngineServerAlias(keyType, issuers, engine);
    }
    return null;
  }
  
  private X509ExtendedKeyManager loadKeyManager() throws GeneralSecurityException, IOException {
    KeyStore keyStore = KeyStore.getInstance(type);
    try (FileInputStream in = new FileInputStream(location)) {
      keyStore.load(in, keystorePassword.toCharArray());
      lastLoadedTimestamp = location.lastModified();
      LOG.debug("Loaded keystore file: " + location);
    }
    
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(SSLFactory.SSLCERTIFICATE);
    kmf.init(keyStore, keyPassword.toCharArray());
    X509ExtendedKeyManager keyManager = null;
    KeyManager[] keyManagers = kmf.getKeyManagers();
    for (KeyManager km : keyManagers) {
      if (km instanceof X509ExtendedKeyManager) {
        keyManager = (X509ExtendedKeyManager) km;
        break;
      }
    }
    
    return keyManager;
  }
  
  private class Reloader implements Runnable {
    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          reloadTimeUnit.sleep(reloadInterval);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
        
        if (needsReload()) {
          try {
            keyManagerLocalRef.set(loadKeyManager());
          } catch (GeneralSecurityException | IOException ex) {
            LOG.error("Could not reload Key Manager. Using the previously loaded key store", ex);
          }
        }
      }
    }
  }
}
