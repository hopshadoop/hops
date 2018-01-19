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
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class HopsSSLTestUtils {
    private final Log LOG = LogFactory.getLog(HopsSSLTestUtils.class);

    protected enum CERT_ERR {
        NO_ERR,
        ERR_CN,
        NO_CA
    }

    @Parameterized.Parameters
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
                {CERT_ERR.NO_ERR},
                {CERT_ERR.ERR_CN},
                {CERT_ERR.NO_CA}
        });
    }

    protected static final String TEST_USER = "TESTUSER";
    protected static final String passwd = "123456";

    protected CERT_ERR error_mode = CERT_ERR.NO_ERR;
    private String outDir;
    private Path serverKeyStore, serverTrustStore;
    private Path clientKeyStore, clientTrustStore;
    protected List<Path> filesToPurge;
    protected Configuration clusterConf, clientConf;

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

    protected ByteBuffer[] getCryptoMaterial() throws Exception {
        ByteBuffer[] material = new ByteBuffer[2];
        ByteBuffer kstore = ByteBuffer.wrap(Files.readAllBytes
            (clientKeyStore));
        ByteBuffer tstore = ByteBuffer.wrap(Files.readAllBytes
            (clientTrustStore));
        material[0] = kstore;
        material[1] = tstore;
        
        return material;
    }
    
    protected void setCryptoConfig(Configuration conf, boolean clusterConf) throws Exception {
        conf.set(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
                "org.apache.hadoop.net.HopsSSLSocketFactory");
        conf.setBoolean(CommonConfigurationKeysPublic.IPC_SERVER_SSL_ENABLED, true);
        conf.set(SSLFactory.SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.1,TLSv1");
        conf.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");
        String user = UserGroupInformation.getCurrentUser().getUserName();
        conf.set(ProxyUsers.CONF_HADOOP_PROXYUSER + "." + user + ".groups", "*");
        conf.set(ProxyUsers.CONF_HADOOP_PROXYUSER + "." + user + ".hosts", "*");

        if (!clusterConf) {
            conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), clientKeyStore.toString());
            conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), clientTrustStore.toString());
        } else {
            Configuration sslServerConf = KeyStoreTestUtil.createServerSSLConfig(serverKeyStore.toString(),
                  passwd, passwd, serverTrustStore.toString(), passwd, "");
            Path sslServerPath = Paths.get(outDir, "ssl-server.xml");
            filesToPurge.add(sslServerPath);
            File sslServer = new File(sslServerPath.toUri());
            KeyStoreTestUtil.saveConfig(sslServer, sslServerConf);

            conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), serverKeyStore.toString());
            conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), serverTrustStore.toString());
        }

        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.SOCKET_ENABLED_PROTOCOL
            .getValue(), "TLSv1.1");
    }

    protected List<Path> prepareCryptoMaterial(String outDir, String user) throws Exception {
        List<Path> filesToPurge = new ArrayList<>();
        this.outDir = outDir;

        String keyAlg = "RSA";
        String signAlg = "SHA256withRSA";

        // Generate CA
        KeyPair caKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
        X509Certificate caCert = KeyStoreTestUtil.generateCertificate("CN=CARoot", caKeyPair, 42, signAlg);

        // Generate server certificate signed by CA
        KeyPair serverKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
        X509Certificate serverCrt = KeyStoreTestUtil.generateSignedCertificate("CN=localhost", serverKeyPair, 42,
                signAlg, caKeyPair.getPrivate(), caCert);

        serverKeyStore = Paths.get(outDir, "server.keystore.jks");
        serverTrustStore = Paths.get(outDir, "server.truststore.jks");
        filesToPurge.add(serverKeyStore);
        filesToPurge.add(serverTrustStore);
        KeyStoreTestUtil.createKeyStore(serverKeyStore.toString(), passwd, passwd,
                "server_alias", serverKeyPair.getPrivate(), serverCrt);
        KeyStoreTestUtil.createTrustStore(serverTrustStore.toString(), passwd, "CARoot", caCert);

        KeyPair c_clientKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
        String c_cn = "CN=" + user;
        clientKeyStore = Paths.get(outDir, user + "__kstore.jks");
        clientTrustStore = Paths.get(outDir, user +"__tstore.jks");
        filesToPurge.add(clientKeyStore);
        filesToPurge.add(clientTrustStore);

        switch (error_mode) {
            case NO_ERR:
                // Generate client certificate with the correct CN field and signed by the CA
                LOG.info("No error mode");
                X509Certificate c_clientCrt = KeyStoreTestUtil.generateSignedCertificate(c_cn, c_clientKeyPair, 42,
                        signAlg, caKeyPair.getPrivate(), caCert);

                KeyStoreTestUtil.createKeyStore(clientKeyStore.toString(), passwd, passwd,
                        "c_client_alias", c_clientKeyPair.getPrivate(), c_clientCrt);
                KeyStoreTestUtil.createTrustStore(clientTrustStore.toString(), passwd, "CARoot", caCert);
                break;
            case NO_CA:
                // Generate client certificate with the correct CN field but NOT signed by the CA
                LOG.info("no ca error mode");
                KeyPair noCA_clientKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
                X509Certificate noCA_clientCrt = KeyStoreTestUtil.generateCertificate(c_cn, noCA_clientKeyPair, 42,
                        signAlg);

                KeyStoreTestUtil.createKeyStore(clientKeyStore.toString(), passwd, passwd,
                        "noca_client_alias", noCA_clientKeyPair.getPrivate(), noCA_clientCrt);
                KeyStoreTestUtil.createTrustStore(clientTrustStore.toString(), passwd, "CARoot", caCert);
                break;
            case ERR_CN:
                // Generate client with INCORRECT CN field but signed by the CA
                LOG.info("wrong cn error mode");
                KeyPair errCN_clientKeyPair = KeyStoreTestUtil.generateKeyPair(keyAlg);
                X509Certificate errCN_clientCrt = KeyStoreTestUtil.generateSignedCertificate("CN=Phil Lynott",
                        errCN_clientKeyPair, 42, signAlg, caKeyPair.getPrivate(), caCert);

                KeyStoreTestUtil.createKeyStore(clientKeyStore.toString(), passwd, passwd,
                        "errcn_client_alias", errCN_clientKeyPair.getPrivate(), errCN_clientCrt);
                KeyStoreTestUtil.createTrustStore(clientTrustStore.toString(), passwd, "CARoot", caCert);
        }

        return filesToPurge;
    }
}
