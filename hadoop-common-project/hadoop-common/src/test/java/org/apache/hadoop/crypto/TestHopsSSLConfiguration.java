package org.apache.hadoop.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Created by antonis on 1/13/17.
 */
public class TestHopsSSLConfiguration {
    Configuration conf;
    HopsSSLSocketFactory hopsFactory;
    final List<String> filesToPurge = new ArrayList<>();

    @Before
    public void setUp() {
        conf = new Configuration();
        hopsFactory = new HopsSSLSocketFactory();
        filesToPurge.clear();
    }

    @After
    public void tearDown() throws IOException {
        purgeFiles();
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
        ugi.doAs(new PrivilegedAction<Object>() {

            @Override
            public Object run() {
                hopsFactory.setConf(conf);
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
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("project__user");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        assertEquals(kstore,
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals("adminpw",
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals("adminpw",
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore,
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals("adminpw",
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }

    @Test
    public void testWithNoConfigInClasspath() throws Exception {
        String cwd = System.getProperty("user.dir");
        touchFile(Paths.get(cwd, "k_certificate").toString());
        touchFile(Paths.get(cwd, "t_certificate").toString());
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("project__user");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("superuser");
        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        assertEquals("k_certificate",
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals("adminpw",
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals("adminpw",
                conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals("t_certificate",
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals("adminpw",
                conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
    }

    @Test
    public void testBothConfigExisting() throws Exception {
        String tmp = System.getProperty("java.io.tmpdir");
        conf.set(HopsSSLSocketFactory.CryptoKeys.CLIENT_MATERIALIZE_DIR
            .getValue(), "/tmp");
        conf.set(HopsSSLSocketFactory.CryptoKeys.SERVICE_CERTS_DIR.getValue(),
            "/tmp");
        String kstore = Paths.get(tmp, "project__user__kstore.jks")
            .toString();
        String tstore = Paths.get(tmp, "project__user__tstore.jks")
            .toString();
        touchFile(kstore);
        touchFile(tstore);
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
        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });
        
        assertEquals(kstore, conf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals("adminpw", conf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals("adminpw", conf.get(HopsSSLSocketFactory.CryptoKeys
            .KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, conf.get(HopsSSLSocketFactory.CryptoKeys
            .TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals("adminpw", conf.get(HopsSSLSocketFactory.CryptoKeys
            .TRUST_STORE_PASSWORD_KEY.getValue()));
    }
    
    @Test
    public void testWithNoConfigSuperuser() throws Exception {
        conf.set(HopsSSLSocketFactory.CryptoKeys.SERVICE_CERTS_DIR.getValue()
            , "/tmp");
        String hostname = NetUtils.getLocalHostname();
        String kstore = "/tmp/" + hostname + "__kstore.jks";
        touchFile(kstore);
        String pass = "adminpw";
        String tstore = "/tmp/" + hostname + "__tstore.jks";
        touchFile(tstore);

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("glassfish");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("glassfish");
        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
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
    public void testNoConfigHostCertificates() throws Exception {
        conf.set(HopsSSLSocketFactory.CryptoKeys.SERVICE_CERTS_DIR.getValue(),
            "/tmp");
        String hostname = NetUtils.getLocalHostname();
        String kstore = "/tmp/" + hostname + "__kstore.jks";
        String tstore = "/tmp/" + hostname + "__tstore.jks";

        touchFile(kstore);
        touchFile(tstore);

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("glassfish");
        final Set<String> superusers = new HashSet<>(1);
        superusers.add("glassfish");
        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                hopsFactory.setConf(conf);
                hopsFactory.configureCryptoMaterial(null, superusers);
                return null;
            }
        });

        assertEquals(kstore, conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue()));
        assertEquals("adminpw", conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue()));
        assertEquals("adminpw", conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue()));
        assertEquals(tstore, conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue()));
        assertEquals("adminpw", conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue()));
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
        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                hopsFactory.setConf(conf);
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
