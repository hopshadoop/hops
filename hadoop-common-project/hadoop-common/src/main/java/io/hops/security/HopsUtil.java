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

import com.google.common.io.ByteStreams;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class HopsUtil {
  
  private static final Log LOG = LogFactory.getLog(HopsUtil.class);
  
  private static final String CERT_PASS_RESOURCE =
      "/hopsworks-api/api/appservice/certpw";
  
  /**
   * Makes a REST call to Hopsworks to get the password of a keystore
   * associated with a Hopsworks user
   * @param keyStore Binary form of the keystore
   * @param username Username associated with the certificate
   * @param conf Hadoop configuration object
   * @return Password of the keystore, it is the same for the truststore
   * @throws JSONException
   * @throws IOException
   */
  public static String getCertificatePasswordFromHopsworks(byte[] keyStore,
      String username, Configuration conf) throws JSONException, IOException {
    String keystoreEncoded = Base64.encodeBase64String(keyStore);
    return getCertPassFromHwInternal(keystoreEncoded, username, conf);
  }
  
  /**
   * Makes a REST call to Hopsworks to get the password of a keystore
   * associated with a Hopsworks user
   * @param keyStorePath Path in the local filesystem of the keystore
   * @param username Username associated with the certificate
   * @param conf Hadoop configuration object
   * @return Password of the keystore, it is the same for the truststore
   * @throws JSONException
   * @throws IOException
   */
  public static String getCertificatePasswordFromHopsworks(String keyStorePath,
      String username, Configuration conf) throws JSONException, IOException {
    String keyStoreEncoded = keystoreEncode(keyStorePath);
    return getCertPassFromHwInternal(keyStoreEncoded, username, conf);
  }
  
  /**
   * Read password for cryptographic material from a file. The file could be
   * either localized in a container or from Hopsworks certificates transient
   * directory.
   * @param passwdFile Location of the password file
   * @return Password to unlock cryptographic material
   * @throws IOException
   */
  public static String readCryptoMaterialPassword(File passwdFile) throws
      IOException {
    
    if (!passwdFile.exists()) {
      throw new FileNotFoundException("File containing crypto material " +
          "password could not be found");
    }
    
    return FileUtils.readFileToString(passwdFile);
  }
  
  private static String getCertPassFromHwInternal(String keyStoreEncoded,
      String username, Configuration conf) throws JSONException, IOException {
    String uri = getUriForCertPassword(conf);
    
    Client client = Client.create();
    WebResource resource = client.resource(uri);
    ClientResponse response = resource
        .queryParam("keyStore", keyStoreEncoded)
        .queryParam("projectUser", username)
        .type(MediaType.TEXT_PLAIN)
        .get(ClientResponse.class);
    
    JSONObject jsonObj = new JSONObject(response.getEntity(String.class));
  
    return jsonObj.getString("keyPw");
  }
  
  private static String keystoreEncode(String keyStorePath) throws IOException {
    FileInputStream keyStoreInStream = new FileInputStream(
        new File(keyStorePath));
    byte[] kStoreBlob = ByteStreams.toByteArray(keyStoreInStream);
    return Base64.encodeBase64String(kStoreBlob);
  }
  
  private static String getUriForCertPassword(Configuration conf)
    throws IOException {
    return getHopsworksEndpoint(conf) + CERT_PASS_RESOURCE;
  }
  
  private static String getHopsworksEndpoint(Configuration conf)
    throws IOException {
    String hopsworksEndpoint = conf.get(HopsSSLSocketFactory
        .HOPSWORKS_REST_ENDPOINT_KEY);
    if (hopsworksEndpoint == null) {
      throw new IOException("Configuration property " + HopsSSLSocketFactory
          .HOPSWORKS_REST_ENDPOINT_KEY + " has not been set");
    }
    
    return hopsworksEndpoint;
  }
}
