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
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.X509SecurityMaterial;
import org.apache.hadoop.util.envVars.EnvironmentVariables;
import org.apache.hadoop.util.envVars.EnvironmentVariablesFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SuperuserKeystoresLoader {
  private final static Log LOG = LogFactory.getLog(SuperuserKeystoresLoader.class);
  
  public static final String SUPER_MATERIAL_DIRECTORY_ENV_VARIABLE = "SUPERUSER_MATERIAL_DIRECTORY";
  protected static final String SUPER_MATERIAL_HOME_SUBDIRECTORY = ".hops_tls";
  
  private static final String SUPER_KEYSTORE_FILE_FORMAT = "%s__kstore.jks";
  private static final String SUPER_TRUSTSTORE_FILE_FORMAT = "%s__tstore.jks";
  private static final String SUPER_MATERIAL_PASSWD_FILE_FORMAT = "%s__passwd";
  private static final Pattern HOME_PATTERN = Pattern.compile(".*\\$\\{HOME\\}.*");
  private static final Pattern HOME_REPLACEMENT_PATTERN = Pattern.compile("\\$\\{HOME\\}");
  private static final Pattern USER_PATTERN = Pattern.compile(".*\\$\\{USER\\}.*");
  private static final Pattern USER_REPLACEMENT_PATTERN = Pattern.compile("\\$\\{USER\\}");
  
  private final Configuration configuration;
  private final EnvironmentVariables environmentVariables;
  
  public SuperuserKeystoresLoader(Configuration configuration) {
    this.configuration = configuration;
    this.environmentVariables = EnvironmentVariablesFactory.getInstance();
  }
  
  public X509SecurityMaterial loadSuperUserMaterial() throws IOException {
    Path superMaterialDirectory = getMaterialDirectory();
    String username = UserGroupInformation.getLoginUser().getUserName();
    Path keystore = superMaterialDirectory.resolve(getSuperKeystoreFilename(username));
    Path truststore = superMaterialDirectory.resolve(getSuperTruststoreFilename(username));
    Path password = superMaterialDirectory.resolve(getSuperMaterialPasswdFilename(username));
    return new X509SecurityMaterial(keystore, truststore, password);
  }
  
  private Path getMaterialDirectory() throws IOException {
    // First start with environment variable
    String superuserMaterialDirectory = environmentVariables.getEnv(SUPER_MATERIAL_DIRECTORY_ENV_VARIABLE);
    if (superuserMaterialDirectory != null) {
      LOG.debug("Found environment variable for super user material directory. Path is " + superuserMaterialDirectory);
      return Paths.get(superuserMaterialDirectory);
    }
    
    // Then check if there is a configured superuser material directory
    String userHome = System.getProperty("user.home");
    superuserMaterialDirectory = configuration.get(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, null);
    if (!Strings.isNullOrEmpty(superuserMaterialDirectory)) {
      LOG.debug("Super user material directory has been set in configuration file " + superuserMaterialDirectory);
      // If the property has ${HOME} it will be substituted my user's home directory
      Matcher matcher = HOME_PATTERN.matcher(superuserMaterialDirectory);
      if (matcher.matches()) {
        matcher = HOME_REPLACEMENT_PATTERN.matcher(superuserMaterialDirectory);
        String templatedDirectory = matcher.replaceAll(userHome);
        LOG.debug("Replacing ${HOME} - Super user material directory: " + templatedDirectory);
        return Paths.get(templatedDirectory);
      }
      matcher = USER_PATTERN.matcher(superuserMaterialDirectory);
      if (matcher.matches()) {
        matcher = USER_REPLACEMENT_PATTERN.matcher(superuserMaterialDirectory);
        String templatedDirectory = matcher.replaceAll(UserGroupInformation.getLoginUser().getUserName());
        LOG.debug("Replacing ${USER} - Super user material directory: " + templatedDirectory);
        return Paths.get(templatedDirectory);
      }
      return Paths.get(superuserMaterialDirectory);
    }
    
    // Finally fallback to user's $HOME
    Path path = Paths.get(userHome, SUPER_MATERIAL_HOME_SUBDIRECTORY);
    LOG.debug("Falling back to $HOME for super user material directory: " + path);
    return path;
  }
  
  @VisibleForTesting
  public String getSuperKeystoreFilename(String username) {
    return String.format(SUPER_KEYSTORE_FILE_FORMAT, username);
  }
  
  @VisibleForTesting
  public String getSuperTruststoreFilename(String username) {
    return String.format(SUPER_TRUSTSTORE_FILE_FORMAT, username);
  }
  
  @VisibleForTesting
  public String getSuperMaterialPasswdFilename(String username) {
    return String.format(SUPER_MATERIAL_PASSWD_FILE_FORMAT, username);
  }
}
