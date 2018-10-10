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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.CertPathBuilder;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CRLValidatorFactory {
  
  public enum TYPE {
    NORMAL,
    TESTING
  }
  
  private final Map<TYPE, CRLValidator> validatorInstances;
  private static volatile CRLValidatorFactory instance = null;
  
  private CRLValidatorFactory() {
    validatorInstances = new ConcurrentHashMap<>();
  }
  
  public static CRLValidatorFactory getInstance() {
    if (instance == null) {
      synchronized (CRLValidatorFactory.class) {
        if (instance == null) {
          instance = new CRLValidatorFactory();
        }
      }
    }
    return instance;
  }
  
  public synchronized CRLValidator getValidator(TYPE type, Configuration conf, Configuration sslConf)
      throws GeneralSecurityException, IOException {
    if (type.equals(TYPE.NORMAL)) {
      if (validatorInstances.containsKey(TYPE.NORMAL)) {
        return validatorInstances.get(TYPE.NORMAL);
      } else {
        CRLValidator validator = new CRLValidator(conf, sslConf);
        validator.startReloadingThread();
        validatorInstances.putIfAbsent(TYPE.NORMAL, validator);
        return validatorInstances.get(TYPE.NORMAL);
      }
    } else if (type.equals(TYPE.TESTING)) {
      if (validatorInstances.containsKey(TYPE.TESTING)) {
        return validatorInstances.get(TYPE.TESTING);
      } else {
        CRLValidator testingValidator = new CRLValidator(conf, sslConf);
        testingValidator.setReloadTimeunit(TimeUnit.SECONDS);
        testingValidator.setReloadInterval(1);
        testingValidator.setCertificateFactory(CertificateFactory.getInstance("X.509", "BC"));
        testingValidator.startReloadingThread();
        validatorInstances.putIfAbsent(TYPE.TESTING, testingValidator);
        return validatorInstances.get(TYPE.TESTING);
      }
    } else {
      throw new IllegalArgumentException("Type " + type + " of CRL validator is not supported");
    }
  }
  
  @VisibleForTesting
  public void registerValidator(TYPE type, CRLValidator crlValidator) {
    validatorInstances.put(type, crlValidator);
  }
  
  @VisibleForTesting
  public void clearCache() {
    for (CRLValidator validator : validatorInstances.values()) {
      validator.stopReloaderThread();
    }
    validatorInstances.clear();
  }
}
