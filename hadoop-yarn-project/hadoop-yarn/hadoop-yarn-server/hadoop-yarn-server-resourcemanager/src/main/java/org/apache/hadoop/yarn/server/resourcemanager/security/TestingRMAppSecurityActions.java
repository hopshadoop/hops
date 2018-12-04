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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequest;

import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Random;

public class TestingRMAppSecurityActions implements RMAppSecurityActions, Configurable {
  private final static Logger LOG = LogManager.getLogger(TestingRMAppSecurityActions.class);
  
  private final static String KEY_ALGORITHM = "RSA";
  private final static String SIGNATURE_ALGORITHM = "SHA256withRSA";
  private final static int KEY_SIZE = 1024;
  
  private KeyPair caKeyPair;
  private X509Certificate caCert;
  private ContentSigner sigGen;
  private Configuration conf;
  
  public TestingRMAppSecurityActions() {
  }
  
  public X509Certificate getCaCert() {
    return caCert;
  }
  
  @Override
  public void init() throws MalformedURLException, GeneralSecurityException {
    Security.addProvider(new BouncyCastleProvider());
    KeyPairGenerator kpg = KeyPairGenerator.getInstance(KEY_ALGORITHM, "BC");
    kpg.initialize(KEY_SIZE);
    caKeyPair = kpg.genKeyPair();
  
    X500NameBuilder subjectBuilder = new X500NameBuilder(BCStyle.INSTANCE);
    subjectBuilder.addRDN(BCStyle.CN, "RootCA");
  
    try {
      sigGen = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).setProvider("BC").build(caKeyPair
          .getPrivate());
      X509v3CertificateBuilder certGen = new JcaX509v3CertificateBuilder(subjectBuilder.build(),
          BigInteger.ONE, new Date(), new Date(System.currentTimeMillis() + 600000),
          subjectBuilder.build(), caKeyPair.getPublic());
      caCert = new JcaX509CertificateConverter().setProvider("BC").getCertificate(certGen.build(sigGen));
  
      caCert.checkValidity();
      caCert.verify(caKeyPair.getPublic());
      caCert.verify(caCert.getPublicKey());
    } catch (OperatorCreationException ex) {
      throw new GeneralSecurityException(ex);
    }
  }
  
  @Override
  public void destroy() {
    // Nothing to do here
    LOG.debug("Nothing to do here");
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public X509SecurityHandler.CertificateBundle sign(PKCS10CertificationRequest csr)
      throws URISyntaxException, IOException, GeneralSecurityException {
    JcaPKCS10CertificationRequest jcaRequest = new JcaPKCS10CertificationRequest(csr);
    X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(caCert,
        BigInteger.valueOf(System.currentTimeMillis()),
        new Date(), new Date(System.currentTimeMillis() + 50000),
        csr.getSubject(),
        jcaRequest.getPublicKey());
  
    JcaX509ExtensionUtils extensionUtils = new JcaX509ExtensionUtils();
    certBuilder
        .addExtension(Extension.authorityKeyIdentifier, false, extensionUtils.createAuthorityKeyIdentifier(caCert))
        .addExtension(Extension.subjectKeyIdentifier, false, extensionUtils
            .createSubjectKeyIdentifier(jcaRequest.getPublicKey()))
        .addExtension(Extension.basicConstraints, true, new BasicConstraints(false))
        .addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
    X509Certificate certificate = new JcaX509CertificateConverter().setProvider("BC")
        .getCertificate(certBuilder.build(sigGen));
    return new X509SecurityHandler.CertificateBundle(certificate, caCert);
  }
  
  @Override
  public int revoke(String certificateIdentifier) throws URISyntaxException, IOException {
    LOG.info("Revoking certificate " + certificateIdentifier);
    return HttpStatus.SC_OK;
  }
  
  @Override
  public String generateJWT(JWTSecurityHandler.JWTMaterialParameter jwtParameter)
    throws URISyntaxException, IOException {
    String jwt = RandomStringUtils.randomAlphanumeric(16);
    return jwt;
  }
  
  @Override
  public void invalidateJWT(String signingKeyName) throws URISyntaxException, IOException {
    // Nothing to do
  }
}
