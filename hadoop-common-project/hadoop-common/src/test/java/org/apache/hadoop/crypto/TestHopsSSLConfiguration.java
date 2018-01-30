package org.apache.hadoop.crypto;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SSLCertificateException;
import org.apache.hadoop.util.envVars.EnvironmentVariables;
import org.apache.hadoop.util.envVars.EnvironmentVariablesFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileWriter;
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

public class TestHopsSSLConfiguration {
    private final Log LOG = LogFactory.getLog(TestHopsSSLConfiguration.class);
    private final String BASEDIR =
        System.getProperty("test.build.dir", "target/test-dir") + "/" +
            TestHopsSSLConfiguration.class.getSimpleName();
    private File baseDirFile;
    
    @Rule
    public final ExpectedException rule = ExpectedException.none();
    
    Configuration conf;
    HopsSSLSocketFactory hopsFactory;
    final List<String> filesToPurge = new ArrayList<>();

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
    
    @Test
    public void testExistingConfIsPreserved() throws Exception {
        String kstore = "someDir/project__user__kstore.jks";
        String kstorePass = "somePassword";
        String keyPass = "anotherPassword";
        String tstore = "someDir/project__user__tstore.jks";
        String tstorePass = "somePassword";
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), kstore);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), kstorePass);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue(), keyPass);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), tstore);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), tstorePass);

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

        assertEquals(kstore, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(kstorePass, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(keyPass, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(tstorePass, conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
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

        assertEquals(kstore,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore,
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password,
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
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
        
        assertEquals("k_certificate",
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals("t_certificate",
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password,
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
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
        String hostname = NetUtils.getLocalHostname();
        String hKstore = Paths.get(tmp, hostname + "__kstore.jks")
            .toString();
        String hTstore = Paths.get(tmp, hostname + "__tstore.jks")
            .toString();
        touchFile(hKstore);
        touchFile(hTstore);
        
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY
            .getValue(), "/tmp/" + hostname + "__kstore.jks");
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY
            .getValue(), "/tmp/" + hostname + "__tstore.jks");
        UserGroupInformation ugi = UserGroupInformation
            .createRemoteUser("project__user");
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
        
        assertEquals(kstore, conf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, conf.get(HopsSSLSocketFactory.CryptoKeys
            .TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys
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
        createServerSSLConfig("/tmp/kstore.jks", "pass", "/tmp/tstore.jks",
            "pass", conf);
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
    public void testWithNoConfigSuperuser() throws Exception {
        String hostname = NetUtils.getLocalHostname();
        String kstore = "/tmp/" + hostname + "__kstore.jks";
        touchFile(kstore);
        String pass = "adminpw";
        String tstore = "/tmp/" + hostname + "__tstore.jks";
        touchFile(tstore);

        createServerSSLConfig(kstore, pass, tstore, pass, conf);
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

        assertEquals(kstore,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(pass,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(pass,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore,
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(pass,
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
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
            public SSLCertificateException run() throws Exception {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });
        
        assertEquals(keystore, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(truststore, conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    // Mock system environment variables
    private class MockEnvironmentVariablesService implements EnvironmentVariables {
        private final Map<String, String> mockEnvVars;
        
        private MockEnvironmentVariablesService() {
            mockEnvVars = new HashMap<>();
        }
        
        private void setEnv(String variableName, String variableValue) {
            mockEnvVars.put(variableName, variableValue);
        }
        
        @Override
        public String getEnv(String variableName) {
            return mockEnvVars.get(variableName);
        }
    }
    
    @Test
    public void testNoConfigHostCertificates() throws Exception {
        String hostname = NetUtils.getLocalHostname();
        String kstore = "/tmp/" + hostname + "__kstore.jks";
        String tstore = "/tmp/" + hostname + "__tstore.jks";

        touchFile(kstore);
        touchFile(tstore);
        String password = "a_strong_password";
        
        createServerSSLConfig(kstore, password, tstore, password, conf);
    
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

        assertEquals(kstore, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(password, conf.get(HopsSSLSocketFactory.CryptoKeys
            .TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    private void createServerSSLConfig(String keystoreLocation, String keyStorePassword,
        String truststoreLocation, String trustStorePassword, Configuration conf) throws IOException {
        
        Configuration sslConf = new Configuration(false);
        
        File sslConfFile = new File(Paths.get(BASEDIR, "ssl-server.xml")
            .toString());
        conf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslConfFile.getAbsolutePath());
        filesToPurge.add(sslConfFile.toString());
        sslConf.set(
            FileBasedKeyStoresFactory.resolvePropertyName(
                SSLFactory.Mode.SERVER,
                FileBasedKeyStoresFactory.SSL_KEYSTORE_LOCATION_TPL_KEY), keystoreLocation);
        sslConf.set(
            FileBasedKeyStoresFactory.resolvePropertyName(
                SSLFactory.Mode.SERVER,
                FileBasedKeyStoresFactory.SSL_KEYSTORE_PASSWORD_TPL_KEY),
            keyStorePassword);
        sslConf.set(
            FileBasedKeyStoresFactory.resolvePropertyName(
                SSLFactory.Mode.SERVER,
                FileBasedKeyStoresFactory.SSL_TRUSTSTORE_LOCATION_TPL_KEY), truststoreLocation);
        sslConf.set(
            FileBasedKeyStoresFactory.resolvePropertyName(
                SSLFactory.Mode.SERVER,
                FileBasedKeyStoresFactory.SSL_TRUSTSTORE_PASSWORD_TPL_KEY),
            trustStorePassword);
        
        try (FileWriter fw = new FileWriter(sslConfFile, false)) {
            sslConf.writeXml(fw);
        }
    }
    
    @Test
    public void testHostCertificateWithSuperuser() throws Exception {
        String hostname = NetUtils.getLocalHostname();
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

        assertEquals(kstore, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals(pass, conf.get((HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue())));
        assertEquals(pass, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals(pass, conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    private String touchFile(String file) throws IOException {
        File fd = new File(file);
        fd.createNewFile();
        filesToPurge.add(fd.getAbsolutePath());

        return file;
    }

    private void purgeFiles() throws IOException {
        for (String file : filesToPurge) {
            File f = new File(file);
            f.delete();
        }
    }
}
