/**
 * Copyright 2016 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.ssl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class HopsSSLTestUtils {
    private final Log LOG = LogFactory.getLog(HopsSSLTestUtils.class);

    protected enum CERT_ERR {
        ERR_CN,
        NO_CA,
        NO_ERROR
    }

    @Parameterized.Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
                {CERT_ERR.ERR_CN},
                {CERT_ERR.NO_CA}
        });
    }

    protected CERT_ERR error_mode = CERT_ERR.ERR_CN;
    protected  String passwd = "123456";
    private String outDir;
    private Path serverKeyStore, serverTrustStore;
    private Path c_clientKeyStore, c_clientTrustStore;
    protected Path err_clientKeyStore, err_clientTrustStore;
    protected List<Path> filesToPurge;
    protected Configuration conf;

    protected Thread invoker;

    @Rule
    public final ExpectedException rule = ExpectedException.none();

    @After
    public void destroy() throws Exception {
        if (null != filesToPurge) {
            purgeFiles(filesToPurge);
        }
    }

    private void purgeFiles(List<Path> files) throws Exception {
        File file;
        for (Path path : files) {
            file = new File(path.toUri());
            if (file.exists()) {
                file.delete();
            }
        }
    }
    
    protected void setCryptoConfig(Configuration conf, String classPathDir) throws Exception {
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                "org.apache.hadoop.net.HopsSSLSocketFactory");
        conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);
        conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS_KEY, "TLSv1.2,TLSv1.1");
        conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
        String user = UserGroupInformation.getCurrentUser().getUserName();
        conf.set(ProxyUsers.CONF_HADOOP_PROXYUSER + "." + user, "*");

        Configuration sslServerConf = KeyStoreTestUtil.createServerSSLConfig(serverKeyStore.toString(),
                passwd, passwd, serverTrustStore.toString(), passwd, "");
        Path sslServerPath = Paths.get(classPathDir, HopsSSLTestUtils.class.getSimpleName() + ".ssl-server.xml");
        filesToPurge.add(sslServerPath);
        File sslServer = new File(sslServerPath.toUri());
        KeyStoreTestUtil.saveConfig(sslServer, sslServerConf);
        conf.set(SSLFactory.SSL_SERVER_CONF_KEY, HopsSSLTestUtils.class.getSimpleName() + ".ssl-server.xml");

        // Set the client certificate with correct CN and signed by the CA
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), c_clientKeyStore.toString());
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), c_clientTrustStore.toString());
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.SOCKET_ENABLED_PROTOCOL
            .getValue(), "TLSv1.2");
    }

    protected static final String KEY_ALG = "RSA";
    protected static final String SIGN_ALG = "SHA256withRSA";
    
    protected Pair<KeyPair, X509Certificate> generateCAMaterial(String subject) throws GeneralSecurityException {
        KeyPair keyPair = KeyStoreTestUtil.generateKeyPair(KEY_ALG);
        X509Certificate x509 = KeyStoreTestUtil.generateCertificate(subject, keyPair, 42, SIGN_ALG);
        return new Pair<>(keyPair, x509);
    }
    
    protected List<Path> prepareCryptoMaterial(String outDir) throws Exception {
        return prepareCryptoMaterial(outDir, generateCAMaterial("CN=CARoot"));
    }
    
    protected List<Path> prepareCryptoMaterial(String outDir, Pair<KeyPair, X509Certificate> caMaterial)
        throws Exception {
        List<Path> filesToPurge = new ArrayList<>();
        this.outDir = outDir;

        // Generate CA
        KeyPair caKeyPair = caMaterial.getFirst();
        X509Certificate caCert = caMaterial.getSecond();
        
        // Generate server certificate signed by CA
        KeyPair serverKeyPair = KeyStoreTestUtil.generateKeyPair(KEY_ALG);
        X509Certificate serverCrt = KeyStoreTestUtil.generateSignedCertificate("CN=serverCrt", serverKeyPair, 42,
                SIGN_ALG, caKeyPair.getPrivate(), caCert);

        serverKeyStore = Paths.get(outDir, "server.keystore.jks");
        serverTrustStore = Paths.get(outDir, "server.truststore.jks");
        filesToPurge.add(serverKeyStore);
        filesToPurge.add(serverTrustStore);
        KeyStoreTestUtil.createKeyStore(serverKeyStore.toString(), passwd, passwd,
                "server_alias", serverKeyPair.getPrivate(), serverCrt);
        KeyStoreTestUtil.createTrustStore(serverTrustStore.toString(), passwd, "CARoot", caCert);

        // Generate client certificate with the correct CN field and signed by the CA
        KeyPair c_clientKeyPair = KeyStoreTestUtil.generateKeyPair(KEY_ALG);
        String c_cn = "CN=" + UserGroupInformation.getCurrentUser().getUserName();
        X509Certificate c_clientCrt = KeyStoreTestUtil.generateSignedCertificate(c_cn, c_clientKeyPair, 42,
                SIGN_ALG, caKeyPair.getPrivate(), caCert);

        c_clientKeyStore = Paths.get(outDir, "c_client.keystore.jks");
        c_clientTrustStore = Paths.get(outDir, "c_client.truststore.jks");
        filesToPurge.add(c_clientKeyStore);
        filesToPurge.add(c_clientTrustStore);
        KeyStoreTestUtil.createKeyStore(c_clientKeyStore.toString(), passwd, passwd,
                "c_client_alias", c_clientKeyPair.getPrivate(), c_clientCrt);
        KeyStoreTestUtil.createTrustStore(c_clientTrustStore.toString(), passwd, "CARoot", caCert);

        if (error_mode.equals(CERT_ERR.NO_CA)) {
            LOG.info("no ca error mode");
            // Generate client certificate with the correct CN field but NOT signed by the CA
            KeyPair noCA_clientKeyPair = KeyStoreTestUtil.generateKeyPair(KEY_ALG);
            X509Certificate noCA_clientCrt = KeyStoreTestUtil.generateCertificate(c_cn, noCA_clientKeyPair, 42,
                    SIGN_ALG);

            err_clientKeyStore = Paths.get(outDir, "noCA_client.keystore.jks");
            err_clientTrustStore = Paths.get(outDir, "noCA_client.truststore.jks");
            filesToPurge.add(err_clientKeyStore);
            filesToPurge.add(err_clientTrustStore);
            KeyStoreTestUtil.createKeyStore(err_clientKeyStore.toString(), passwd, passwd,
                    "noca_client_alias", noCA_clientKeyPair.getPrivate(), noCA_clientCrt);
            KeyStoreTestUtil.createTrustStore(err_clientTrustStore.toString(), passwd, "CARoot", caCert);

        } else if (error_mode.equals(CERT_ERR.ERR_CN)) {
            LOG.info("wrong cn error mode");
            // Generate client with INCORRECT CN field but signed by the CA
            KeyPair errCN_clientKeyPair = KeyStoreTestUtil.generateKeyPair(KEY_ALG);
            X509Certificate errCN_clientCrt = KeyStoreTestUtil.generateSignedCertificate("CN=Phil Lynott",
                    errCN_clientKeyPair, 42, SIGN_ALG, caKeyPair.getPrivate(), caCert);

            err_clientKeyStore = Paths.get(outDir, "errCN_client.keystore.jks");
            err_clientTrustStore = Paths.get(outDir, "errCN_client.truststore.jks");
            filesToPurge.add(err_clientKeyStore);
            filesToPurge.add(err_clientTrustStore);
            KeyStoreTestUtil.createKeyStore(err_clientKeyStore.toString(), passwd, passwd,
                    "errcn_client_alias", errCN_clientKeyPair.getPrivate(), errCN_clientCrt);
            KeyStoreTestUtil.createTrustStore(err_clientTrustStore.toString(), passwd, "CARoot", caCert);
        }

        return filesToPurge;
    }
}
