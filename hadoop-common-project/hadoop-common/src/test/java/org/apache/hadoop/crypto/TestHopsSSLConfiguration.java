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
package org.apache.hadoop.crypto;

import io.hops.security.MockEnvironmentVariablesService;
import io.hops.security.SuperuserKeystoresLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SSLCertificateException;
import org.apache.hadoop.security.ssl.HopsSSLTestUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.envVars.EnvironmentVariables;
import org.apache.hadoop.util.envVars.EnvironmentVariablesFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestHopsSSLConfiguration extends HopsSSLTestUtils {
    private final Log LOG = LogFactory.getLog(TestHopsSSLConfiguration.class);
    private final String BASEDIR =
        System.getProperty("test.build.dir", "target/test-dir") + "/" +
            TestHopsSSLConfiguration.class.getSimpleName();
    private File baseDirFile;
    private static File classPathDir;
    
    @Rule
    public final ExpectedException rule = ExpectedException.none();
    
    Configuration conf;
    HopsSSLSocketFactory hopsFactory;
    final List<String> filesToPurge = new ArrayList<>();

    @BeforeClass
    public static void beforeClass() throws Exception {
        classPathDir = new File(KeyStoreTestUtil.getClasspathDir(TestHopsSSLConfiguration.class));
    }
    
    @Before
    public void setUp() {
        conf = new Configuration();
        hopsFactory = new HopsSSLSocketFactory();
        baseDirFile = new File(BASEDIR);
        baseDirFile.mkdirs();
        
        filesToPurge.clear();
    }

    @After
    public void tearDown() throws IOException {
        if (baseDirFile.exists()) {
            FileUtils.deleteQuietly(baseDirFile);
        }
        purgeFiles();
        EnvironmentVariablesFactory.setInstance(null);
    }
    
    @AfterClass
    public static void afterClass() throws Exception {
        if (classPathDir != null) {
            File sslServerConf = Paths.get(classPathDir.getAbsolutePath(), TestHopsSSLConfiguration.class.getSimpleName() +
                ".ssl-server.xml").toFile();
            if (sslServerConf.exists()) {
                sslServerConf.delete();
            }
        }
    }
    
    @Test
    public void testExistingConfIsPreserved() throws Exception {
        String hostname = NetUtils.getLocalCanonicalHostname();
        String kstore = "someDir/" + hostname + "__kstore.jks";
        String kstorePass = "somePassword";
        String keyPass = "anotherPassword";
        String tstore = "someDir/" + hostname + "__tstore.jks";
        String tstorePass = "somePassword";
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), kstore);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), kstorePass);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue(), keyPass);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), tstore);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), tstorePass);

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("superuser");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {

            @Override
            public Object run() throws SSLCertificateException {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        Configuration factoryConf = hopsFactory.getConf();
        assertEquals(kstore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(kstorePass, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(keyPass, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(tstorePass, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }

    @Test
    public void testWithNoConfigInTemp() throws Exception {
        conf.set(HopsSSLSocketFactory.CryptoKeys.CLIENT_MATERIALIZE_DIR
            .getValue(), "/tmp");
        String kstore = touchFile("/tmp/project__user__kstore.jks");
        String tstore = touchFile("/tmp/project__user__tstore.jks");
        String password = "a_strong_password";
        Path passwdFile = Paths.get("/tmp", "project__user__cert.key");
        touchFile(passwdFile.toString());
        FileUtils.writeStringToFile(passwdFile.toFile(), password, false);
        
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("project__user");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws SSLCertificateException {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        Configuration factoryConf = hopsFactory.getConf();
        assertEquals(kstore,
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password,
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password,
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore,
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password,
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }

    @Test
    public void testWithNoConfigInClasspath() throws Exception {
        String cwd = System.getProperty("user.dir");
        touchFile(Paths.get(cwd, "k_certificate").toString());
        touchFile(Paths.get(cwd, "t_certificate").toString());
        final String password = "a_strong_password";
        Path passwdFile = Paths.get(cwd, "material_passwd");
        touchFile(passwdFile.toString());
        FileUtils.writeStringToFile(passwdFile.toFile(), password, false);
    
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("project__user");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws SSLCertificateException {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        Configuration factoryConf = hopsFactory.getConf();
        assertEquals("k_certificate",
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password,
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password,
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals("t_certificate",
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password,
                factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }

    @Test
    public void testBothConfigExisting() throws Exception {
        String tmp = System.getProperty("java.io.tmpdir");
        conf.set(HopsSSLSocketFactory.CryptoKeys.CLIENT_MATERIALIZE_DIR
            .getValue(), "/tmp");
        String kstore = Paths.get(tmp, "project__user__kstore.jks")
            .toString();
        String tstore = Paths.get(tmp, "project__user__tstore.jks")
            .toString();
        Path passwdFile = Paths.get(tmp, "project__user__cert.key");
        String password = "a_strong_password";
        touchFile(kstore);
        touchFile(tstore);
        touchFile(passwdFile.toString());
        FileUtils.writeStringToFile(passwdFile.toFile(), password, false);
        SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
        String hKstore = Paths.get(tmp, loader.getSuperKeystoreFilename("superuser")).toString();
        String hTstore = Paths.get(tmp, loader.getSuperTruststoreFilename("superuser")).toString();
        touchFile(hKstore);
        touchFile(hTstore);
        
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), hKstore);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), hTstore);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("project__user");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws SSLCertificateException {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        Configuration factoryConf = hopsFactory.getConf();
        assertEquals(kstore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys
            .TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys
            .TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    @Test
    public void testConfigurationWithMissingCertificatesNormalUser() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser
            ("project__user");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        rule.expect(SSLCertificateException.class);
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws SSLCertificateException {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });
    }
    
    @Test
    public void testConfigurationWithMissingCertificatesSuperUser() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("superuser");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        rule.expect(SSLCertificateException.class);
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws SSLCertificateException {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });
    }
    
    @Test
    public void testConfigurationLocalizedMaterialUserChangedCWD() throws Exception {
        File materialDir = Paths.get(BASEDIR, "pwd_dir").toFile();
        if (!materialDir.exists()) {
            materialDir.mkdirs();
        }
        
        MockEnvironmentVariablesService mockEnvService = new MockEnvironmentVariablesService();
        mockEnvService.setEnv("PWD", materialDir.getAbsolutePath());
        EnvironmentVariablesFactory.setInstance(mockEnvService);
        
        LOG.info("Mocked PWD is : " + EnvironmentVariablesFactory.getInstance().getEnv("PWD"));
        
        String keystore = Paths.get(materialDir.getAbsolutePath(), conf.get(SSLFactory.LOCALIZED_KEYSTORE_FILE_PATH_KEY,
          SSLFactory.DEFAULT_LOCALIZED_KEYSTORE_FILE_PATH)).toString();
      String truststore = Paths.get(materialDir.getAbsolutePath(), conf.get(SSLFactory.LOCALIZED_TRUSTSTORE_FILE_PATH_KEY,
          SSLFactory.DEFAULT_LOCALIZED_TRUSTSTORE_FILE_PATH)).toString();
      String passwd = Paths.get(materialDir.getAbsolutePath(), conf.get(SSLFactory.LOCALIZED_PASSWD_FILE_PATH_KEY,
          SSLFactory.DEFAULT_LOCALIZED_PASSWD_FILE_PATH)).toString();
        String password = "password";
        touchFile(keystore);
        touchFile(truststore);
        touchFile(passwd);
        FileUtils.writeStringToFile(new File(passwd), password);
        
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("some_user");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        Configuration factoryConf = hopsFactory.getConf();
        assertEquals(keystore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(truststore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    @Test
    public void testConfigurationWithEnvironmentVariable() throws Exception {
        String user = "project__user";
        File materialDir = Paths.get(BASEDIR, "crypto_material").toFile();
        if (!materialDir.exists()) {
            materialDir.mkdirs();
        }
        
        MockEnvironmentVariablesService mockEnvService = new MockEnvironmentVariablesService();
        mockEnvService.setEnv(HopsSSLSocketFactory.CRYPTO_MATERIAL_ENV_VAR, materialDir.getAbsolutePath());
        EnvironmentVariablesFactory.setInstance(mockEnvService);
        
        String keystore = Paths.get(materialDir.getAbsolutePath(), user
            + HopsSSLSocketFactory.KEYSTORE_SUFFIX).toString();
        String truststore = Paths.get(materialDir.getAbsolutePath(), user
            + HopsSSLSocketFactory.TRUSTSTORE_SUFFIX).toString();
        String passwd = Paths.get(materialDir.getAbsolutePath(), user
            + HopsSSLSocketFactory.PASSWD_FILE_SUFFIX).toString();
        String password = "some_password";
        touchFile(keystore);
        touchFile(truststore);
        touchFile(passwd);
        FileUtils.writeStringToFile(new File(passwd), password, false);
        
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("glassfish");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Void run() throws Exception {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        Configuration factoryConf = hopsFactory.getConf();
        assertEquals(keystore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(truststore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    @Test
    public void testNoConfigHostCertificates() throws Exception {
        String TMP = System.getProperty("java.io.tmpdir");
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("glassfish");
        SuperuserKeystoresLoader loader = new SuperuserKeystoresLoader(conf);
        Path kstore = Paths.get(TMP, loader.getSuperKeystoreFilename(ugi.getUserName()));
        Path tstore = Paths.get(TMP, loader.getSuperTruststoreFilename(ugi.getUserName()));
        Path passwd = Paths.get(TMP, loader.getSuperMaterialPasswdFilename(ugi.getUserName()));

        touchFile(kstore.toFile());
        touchFile(tstore.toFile());
        String password = "a_strong_password";
        FileUtils.writeStringToFile(passwd.toFile(), password);
        
        conf.set(CommonConfigurationKeysPublic.HOPS_TLS_SUPER_MATERIAL_DIRECTORY, TMP);
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("glassfish");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws SSLCertificateException {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        Configuration factoryConf = hopsFactory.getConf();
        assertEquals(kstore.toString(), factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore.toString(), factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    @Test
    public void testHostCertificateWithSuperuser() throws Exception {
        String hostname = NetUtils.getLocalCanonicalHostname();
        String kstore = "/tmp/" + hostname + "__kstore.jks";
        String tstore = "/tmp/" + hostname + "__tstore.jks";
        String pass = "anotherPassphrase";

        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), kstore);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), pass);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue(), pass);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), tstore);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), pass);

        String cwd = System.getProperty("user.dir");
        touchFile(Paths.get(cwd, "glassfish__kstore.jks").toString());
        touchFile(Paths.get(cwd, "glassfish__tstore.jks").toString());

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("glassfish");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("glassfish");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws SSLCertificateException {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        Configuration factoryConf = hopsFactory.getConf();
        assertEquals(kstore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(pass, factoryConf.get((HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue())));
        assertEquals(pass, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(pass, factoryConf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }

    @Test
    public void testConfigurationCloned() throws Exception {
        File materialDir = Paths.get(BASEDIR, "pwd_dir").toFile();
        if (!materialDir.exists()) {
            materialDir.mkdirs();
        }

        MockEnvironmentVariablesService mockEnvService = new MockEnvironmentVariablesService();
        mockEnvService.setEnv("PWD", materialDir.getAbsolutePath());
        EnvironmentVariablesFactory.setInstance(mockEnvService);

        LOG.info("Mocked PWD is : " + EnvironmentVariablesFactory.getInstance().getEnv("PWD"));

        String keystore = Paths.get(materialDir.getAbsolutePath(), conf.get(SSLFactory.LOCALIZED_KEYSTORE_FILE_PATH_KEY,
          SSLFactory.DEFAULT_LOCALIZED_KEYSTORE_FILE_PATH)).toString();
        String truststore = Paths.get(materialDir.getAbsolutePath(), conf.get(SSLFactory.LOCALIZED_TRUSTSTORE_FILE_PATH_KEY,
          SSLFactory.DEFAULT_LOCALIZED_TRUSTSTORE_FILE_PATH)).toString();
        String passwd = Paths.get(materialDir.getAbsolutePath(), conf.get(SSLFactory.LOCALIZED_PASSWD_FILE_PATH_KEY,
          SSLFactory.DEFAULT_LOCALIZED_PASSWD_FILE_PATH)).toString();
        String password = "password";
        touchFile(keystore);
        touchFile(truststore);
        touchFile(passwd);
        FileUtils.writeStringToFile(new File(passwd), password);

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("some_user");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        assertNull(conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertNull(conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertNull(conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertNull(conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertNull(conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    private String touchFile(String file) throws IOException {
        return touchFile(new File(file));
    }
    
    private String touchFile(File file) throws IOException {
        file.createNewFile();
        filesToPurge.add(file.getAbsolutePath());

        return file.getAbsolutePath();
    }

    private void purgeFiles() throws IOException {
        for (String file : filesToPurge) {
            File f = new File(file);
            f.delete();
        }
    }
}
