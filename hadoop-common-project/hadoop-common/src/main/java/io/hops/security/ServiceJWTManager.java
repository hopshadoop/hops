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
package io.hops.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.BackOff;
import org.apache.hadoop.util.DateUtils;
import org.apache.hadoop.util.ExponentialBackOff;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.hops.security.AbstractSecurityActions.BEARER_AUTH_HEADER_CONTENT;

public class ServiceJWTManager extends AbstractService {
  
  public static final String JWT_MANAGER_PREFIX = CommonConfigurationKeysPublic.HOPS_PREFIX + "jwt-manager.";
  public static final String JWT_MANAGER_MASTER_TOKEN_KEY = JWT_MANAGER_PREFIX + "master-token";
  public static final String JWT_MANAGER_RENEW_TOKEN_PATTERN = JWT_MANAGER_PREFIX + "renew-token-%d";
  
  private static final Log LOG = LogFactory.getLog(ServiceJWTManager.class);
  private static final String LOCK_FILE = ".ssl-server.lock";
  private static final Set<PosixFilePermission> LOCK_FILE_PERMISSIONS = new HashSet<>();
  static {
    LOCK_FILE_PERMISSIONS.add(PosixFilePermission.OWNER_READ);
    LOCK_FILE_PERMISSIONS.add(PosixFilePermission.OWNER_WRITE);
    LOCK_FILE_PERMISSIONS.add(PosixFilePermission.OWNER_EXECUTE);
    
    LOCK_FILE_PERMISSIONS.add(PosixFilePermission.GROUP_READ);
    LOCK_FILE_PERMISSIONS.add(PosixFilePermission.GROUP_WRITE);
    LOCK_FILE_PERMISSIONS.add(PosixFilePermission.GROUP_EXECUTE);
  }
  
  private final Gson jsonParser;
  private CloseableHttpClient httpClient;
  private final ExecutorService tokenRenewer;
  private CountDownLatch waiter;
  
  private Configuration sslConf;
  private URL serviceJWTRenewPath;
  private URL serviceJWTInvalidatePath;
  private long serviceJWTValidityPeriodSeconds;
  private final AtomicReference<String> masterToken;
  private LocalDateTime masterTokenExpiration;
  
  private String[] renewalTokens;
  
  public ServiceJWTManager(String name) {
    super(name);
    jsonParser = new GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        .create();
    this.masterToken = new AtomicReference<>();
    this.tokenRenewer = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("JWT renewer thread")
        .setDaemon(true)
        .build());
  }
  
  public void setHTTPClient(CloseableHttpClient client) {
    this.httpClient = client;
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    URL host = new URL(conf.get(CommonConfigurationKeysPublic.HOPS_HOPSWORKS_HOST_KEY,
        "https://127.0.0.1"));
    sslConf = new Configuration(false);
    sslConf.addResource(conf.get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
    loadMasterJWT();
    loadRenewalJWTs();
    serviceJWTValidityPeriodSeconds = conf.getTimeDuration(
        CommonConfigurationKeysPublic.JWT_MANAGER_MASTER_TOKEN_VALIDITY_PERIOD,
        CommonConfigurationKeysPublic.DEFAULT_JWT_MANAGER_MASTER_TOKEN_VALIDITY_PERIOD,
        TimeUnit.SECONDS);
    if (serviceJWTValidityPeriodSeconds == 0) {
      serviceJWTValidityPeriodSeconds = 30L;
    }
    String serviceJWTRenewPathConf = conf.get(CommonConfigurationKeysPublic.JWT_MANAGER_SERVICE_RENEW_PATH,
        CommonConfigurationKeysPublic.DEFAULT_JWT_MANAGER_SERVICE_RENEW_PATH);
    serviceJWTRenewPath = new URL(host, serviceJWTRenewPathConf);
    String serviceJWTInvalidatePathConf = conf.get(CommonConfigurationKeysPublic.JWT_MANAGER_SERVICE_INVALIDATE_PATH,
        CommonConfigurationKeysPublic.DEFAULT_JWT_MANAGER_SERVICE_INVALIDATE_PATH);
    if (!serviceJWTInvalidatePathConf.endsWith("/")) {
      serviceJWTInvalidatePathConf = serviceJWTRenewPathConf + "/";
    }
    serviceJWTInvalidatePath = new URL(host, serviceJWTInvalidatePathConf);
    super.serviceInit(conf);
  }
  
  private void createLockFileIfMissing() throws IOException {
    Path lockFile = getLockFilePath();
    if (!lockFile.toFile().exists()) {
      LOG.debug("Lock file " + lockFile.toString() + " does not exist");
      FileChannel fc = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      try (FileLock fl = fc.tryLock()) {
        if (fl == null) {
          return;
        }
        Files.setPosixFilePermissions(lockFile, LOCK_FILE_PERMISSIONS);
      }
    }
  }
  
  private Path getLockFilePath() {
    String tmp = System.getProperty("java.io.tmpdir");
    return Paths.get(tmp, LOCK_FILE);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    createLockFileIfMissing();
    waiter = new CountDownLatch(1);
    tokenRenewer.execute(new TokenRenewer());
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping ServiceJWTManager");
    try {
      try {
        tokenRenewer.shutdown();
        if (!tokenRenewer.awaitTermination(100L, TimeUnit.MILLISECONDS)) {
          tokenRenewer.shutdownNow();
        }
      } catch (InterruptedException ex) {
        tokenRenewer.shutdownNow();
        Thread.currentThread().interrupt();
      }
  
      if (waiter != null) {
        waiter.await(100L, TimeUnit.MILLISECONDS);
      }
      
      super.serviceStop();
    } finally {
      // Try to delete lock file if there is no lock
      Path lockFile = getLockFilePath();
      if (lockFile.toFile().exists()) {
        FileChannel fc = FileChannel.open(lockFile, StandardOpenOption.WRITE);
        try (FileLock fl = fc.tryLock()) {
          if (fl != null) {
            FileUtils.deleteQuietly(lockFile.toFile());
          }
        } catch (Exception ex) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void setMasterToken(String masterToken) {
    this.masterToken.set(masterToken);
  }
  
  public String getMasterToken() {
    return masterToken.get();
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected LocalDateTime getMasterTokenExpiration() {
    return masterTokenExpiration;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void setMasterTokenExpiration(LocalDateTime masterTokenExpiration) {
    this.masterTokenExpiration = masterTokenExpiration;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void setRenewalTokens(String[] renewalTokens) {
    this.renewalTokens = renewalTokens;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected ExecutorService getExecutorService() {
    return tokenRenewer;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void loadMasterJWT() throws GeneralSecurityException {
    String masterTokenConf = sslConf.get(JWT_MANAGER_MASTER_TOKEN_KEY);
    if (masterTokenConf == null) {
      throw new GeneralSecurityException("Could not parse master JWT from configuration");
    }
    masterToken.set(masterTokenConf);
    try {
      JWT jwt = JWTParser.parse(masterToken.get());
      masterTokenExpiration = DateUtils.date2LocalDateTime(jwt.getJWTClaimsSet().getExpirationTime());
    } catch (ParseException ex) {
      throw new GeneralSecurityException("Could not parse master JWT", ex);
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void loadRenewalJWTs() throws GeneralSecurityException {
    String renewToken = null;
    List<String> renewalTokens = new ArrayList<>();
    int idx = 0;
    while (true) {
      String renewTokenKey = String.format(JWT_MANAGER_RENEW_TOKEN_PATTERN, idx);
      renewToken = sslConf.get(renewTokenKey, "");
      if (renewToken.isEmpty()) {
        break;
      }
      renewalTokens.add(renewToken);
      idx++;
    }
    if (renewalTokens.isEmpty()) {
      throw new GeneralSecurityException("Could not load one-time renewal JWTs");
    }
    this.renewalTokens = renewalTokens.toArray(new String[renewalTokens.size()]);
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void reloadJWTs() throws GeneralSecurityException {
    sslConf.reloadConfiguration();
    loadMasterJWT();
    loadRenewalJWTs();
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected ServiceTokenDTO renewServiceJWT(String token, String oneTimeToken, LocalDateTime expiresAt,
      LocalDateTime notBefore) throws URISyntaxException, IOException {
    CloseableHttpResponse httpResponse = null;
    try {
      JWTDTO request = new JWTDTO();
      request.token = token;
      request.expiresAt = DateUtils.localDateTime2Date(expiresAt);
      request.nbf = DateUtils.localDateTime2Date(notBefore);
      String jsonRequest = jsonParser.toJson(request);
      HttpPut httpRequest = new HttpPut(serviceJWTRenewPath.toURI());
      httpRequest.addHeader(createAuthenticationHeader(oneTimeToken));
      httpRequest.setEntity(new StringEntity(jsonRequest));
      httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      httpResponse = httpClient.execute(httpRequest);
      checkHTTPResponseCode(httpResponse, "Could not make HTTP request to renew service JWT");
      ServiceTokenDTO renewedTokenResponse = jsonParser.fromJson(EntityUtils.toString(httpResponse.getEntity()),
          ServiceTokenDTO.class);
      return renewedTokenResponse;
    } finally {
      if (httpResponse != null) {
        httpResponse.close();
      }
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected void invalidateServiceJWT(String token2invalidate) throws URISyntaxException, IOException {
    CloseableHttpResponse httpResponse = null;
    try {
      URL invalidateURL = new URL(serviceJWTInvalidatePath, token2invalidate);
      HttpDelete httpRequest = new HttpDelete(invalidateURL.toURI());
      httpRequest.addHeader(createAuthenticationHeader(masterToken.get()));
      httpRequest.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      httpResponse = httpClient.execute(httpRequest);
      checkHTTPResponseCode(httpResponse, "Could not make HTTP request to invalidate master JWT");
    } finally {
      if (httpResponse != null) {
        httpResponse.close();
      }
    }
  }
  
  private Header createAuthenticationHeader(String token) {
    String authHeaderContent = String.format(BEARER_AUTH_HEADER_CONTENT, token);
    return new BasicHeader(HttpHeaders.AUTHORIZATION, authHeaderContent);
  }
  
  private void checkHTTPResponseCode(HttpResponse response, String extraMessage) throws IOException {
    int code = response.getStatusLine().getStatusCode();
    if (code != HttpStatus.SC_OK) {
      throw new IOException("HTTP error, response code " + code + " Reason: " + response.getStatusLine()
          .getReasonPhrase() + " Message: " + extraMessage);
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  protected boolean isTime2Renew(LocalDateTime now, LocalDateTime tokenExpiration) {
    return now.isAfter(tokenExpiration) || now.isEqual(tokenExpiration);
  }
  
  protected FileLock tryAndGetLock() {
    try {
      FileChannel fc = FileChannel.open(getLockFilePath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      return fc.tryLock();
    } catch (Exception ex) {
      return null;
    }
  }
  
  private class TokenRenewer implements Runnable {
    private final BackOff backOff;
    private final long sleepPeriodSeconds;
    
    private TokenRenewer() {
      int maximumRetries = Math.max(1, renewalTokens.length - 1);
      backOff = new ExponentialBackOff.Builder()
          .setInitialIntervalMillis(1000)
          .setMaximumIntervalMillis(7000)
          .setMultiplier(2)
          .setMaximumRetries(maximumRetries)
          .build();
      sleepPeriodSeconds = serviceJWTValidityPeriodSeconds / 2;
    }
    
    @Override
    public void run() {
      // Block until we take the exclusive lock to the file lock
      // If multiple processes run on the same host and all try to
      // update the content of ssl-server.xml, only one should succeed
      FileLock fileLock = null;
      while (!Thread.currentThread().isInterrupted()) {
        fileLock = tryAndGetLock();
        if (fileLock != null) {
          LOG.debug(getName() + " Managed to get file lock to renew JWTs");
          break;
        } else {
          LOG.debug(getName() + " Did NOT manage to get file lock to renew JWTs");
          try {
            // It's not our job to renew tokens, nevertheless refresh
            // them in memory
            LOG.debug(getName() + " But refreshing JWTs from ssl-server");
            reloadJWTs();
            TimeUnit.SECONDS.sleep(sleepPeriodSeconds);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          } catch (GeneralSecurityException ex) {
            LOG.warn("Could not refresh service JWTs when failed to acquire exclusive lock");
          }
        }
      }
      
      // Now we have the exclusive lock
      boolean shouldTry2renew = true;
      while (!Thread.currentThread().isInterrupted() && shouldTry2renew) {
        try {
          // Check if it is time to renew
          LocalDateTime now = DateUtils.getNow();
          if (isTime2Renew(now, masterTokenExpiration)) {
            backOff.reset();
            LocalDateTime expiresAt = DateUtils.getNow().plus(serviceJWTValidityPeriodSeconds, ChronoUnit.SECONDS);
            int renewalTokenIdx = 0;
            while (renewalTokenIdx < renewalTokens.length) {
              try {
                // Use one-time token to authenticate
                ServiceTokenDTO renewedTokens = renewServiceJWT(masterToken.get(), renewalTokens[renewalTokenIdx],
                    expiresAt, now);
                String oldMasterToken = masterToken.get();
                masterToken.set(renewedTokens.jwt.token);
                masterTokenExpiration = DateUtils.date2LocalDateTime(renewedTokens.jwt.expiresAt);
                renewalTokens = renewedTokens.renewTokens;
                try {
                  // Since renewal of master JWT has gone through, invalidate the old one
                  invalidateServiceJWT(oldMasterToken);
                } catch (Exception ex) {
                  // Do not retry if we failed to invalidate old master token
                  LOG.warn("Failed to invalidate old service master JWT. Continue...");
                }
                sslConf.set(JWT_MANAGER_MASTER_TOKEN_KEY, masterToken.get());
                for (int i = 0; i < renewalTokens.length; i++) {
                  String confKey = String.format(JWT_MANAGER_RENEW_TOKEN_PATTERN, i);
                  sslConf.set(confKey, renewalTokens[i]);
                }
                URL sslServerURL = sslConf.getResource(getConfig().get(SSLFactory.SSL_SERVER_CONF_KEY, "ssl-server.xml"));
                File sslServerFile = new File(sslServerURL.getFile());
                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(sslServerFile))) {
                  sslConf.writeXml(bos);
                  bos.flush();
                }
                LOG.info("Updated service JWT");
                break;
              } catch (URISyntaxException ex) {
                LOG.error("There is an error in service JWT renewal URI: " + serviceJWTRenewPath.toString(), ex);
                shouldTry2renew = false;
                break;
              } catch (Exception ex) {
                // If for some reason we fail to parse the new token,
                // we retry and invalidate the failed token
                renewalTokenIdx++;
                long backoffTimeout = backOff.getBackOffInMillis();
                if (backoffTimeout != -1) {
                  LOG.warn("Error while trying to renew service JWT. Retrying in " + backoffTimeout + " ms", ex);
                  TimeUnit.MILLISECONDS.sleep(backoffTimeout);
                } else {
                  LOG.error("Could not renew service JWT. Manual update is necessary!", ex);
                  shouldTry2renew = false;
                  break;
                }
              }
            }
            if (!shouldTry2renew) {
              break;
            }
          }
          TimeUnit.SECONDS.sleep(sleepPeriodSeconds);
        } catch (InterruptedException ex) {
          LOG.warn("Service JWT renewer has been interrupted");
          Thread.currentThread().interrupt();
        }
      }
      if (fileLock != null) {
        try {
          fileLock.release();
        } catch (IOException ex) {
          // Nothing to do anymore
          LOG.warn("Failed to release ssl-server file lock", ex);
        }
      }
      waiter.countDown();
      LOG.info("JWT renewal thread finishing");
    }
  }
  
  public static class JWTDTO {
    private String token;
    private String subject;
    private String keyName;
    private String audiences;
    private Boolean renewable;
    private Integer expLeeway;
    private Date expiresAt;
    private Date nbf;
    
    public String getToken() {
      return token;
    }
    
    public void setToken(String token) {
      this.token = token;
    }
    
    public String getSubject() {
      return subject;
    }
    
    public void setSubject(String subject) {
      this.subject = subject;
    }
    
    public String getKeyName() {
      return keyName;
    }
    
    public void setKeyName(String keyName) {
      this.keyName = keyName;
    }
    
    public String getAudiences() {
      return audiences;
    }
    
    public void setAudiences(String audiences) {
      this.audiences = audiences;
    }
    
    public Boolean getRenewable() {
      return renewable;
    }
    
    public void setRenewable(Boolean renewable) {
      this.renewable = renewable;
    }
    
    public Integer getExpLeeway() {
      return expLeeway;
    }
    
    public void setExpLeeway(Integer expLeeway) {
      this.expLeeway = expLeeway;
    }
    
    public Date getExpiresAt() {
      return expiresAt;
    }
    
    public void setExpiresAt(Date expiresAt) {
      this.expiresAt = expiresAt;
    }
    
    public Date getNbf() {
      return nbf;
    }
    
    public void setNbf(Date nbf) {
      this.nbf = nbf;
    }
  }
  
  public class ServiceTokenDTO {
    private JWTDTO jwt;
    private String[] renewTokens;
    
    public JWTDTO getJwt() {
      return jwt;
    }
    
    public void setJwt(JWTDTO jwt) {
      this.jwt = jwt;
    }
    
    public String[] getRenewTokens() {
      return renewTokens;
    }
    
    public void setRenewTokens(String[] renewTokens) {
      this.renewTokens = renewTokens;
    }
  }
}
