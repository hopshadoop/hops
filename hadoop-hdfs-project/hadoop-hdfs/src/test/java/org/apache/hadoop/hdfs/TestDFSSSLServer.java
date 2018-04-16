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
package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.HopsSSLTestUtils;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.SSLException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ipc.RemoteException;

@RunWith(Parameterized.class)
public class TestDFSSSLServer extends HopsSSLTestUtils {
    private final Log LOG = LogFactory.getLog(TestDFSSSLServer.class);

    MiniDFSCluster cluster;
    FileSystem dfs1, dfs2;
    private static String classpathDir;

    public TestDFSSSLServer(CERT_ERR error_mode) {
        super.error_mode = error_mode;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        classpathDir = KeyStoreTestUtil.getClasspathDir(TestDFSSSLServer.class);
    }
    
    @Before
    public void setUp() throws Exception {
        conf = new HdfsConfiguration();
        filesToPurge = prepareCryptoMaterial(conf, classpathDir);
        setCryptoConfig(conf, classpathDir);
        
        String testDataPath = System
                .getProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, "build/test/data");
        File testDataCluster1 = new File(testDataPath, "dfs_cluster");
        String c1Path = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);

        cluster = new MiniDFSCluster.Builder(conf).build();
        LOG.info("DFS cluster started");
    }

    @After
    public void tearDown() throws Exception {
        if (invoker != null) {
            invoker.join();
            invoker = null;
        }

        if (cluster != null) {
            cluster.shutdown();
        }

        if (dfs1 != null) {
            dfs1.close();
        }

        if (dfs2 != null) {
            dfs2.close();
        }
    }

    @Test
    public void testRpcCall() throws Exception {
        LOG.debug("testRpcCall");
        dfs1 = DistributedFileSystem.newInstance(conf);
        boolean exists = dfs1.exists(new Path("some_path"));
        LOG.debug("Does exist? " + exists);
        assertFalse(exists);
    }

    @Test
    public void testChecksum() throws Exception {
        LOG.debug("testChecksum");
        dfs1 = DistributedFileSystem.newInstance(conf);
        Path file = new Path("some_file");
        dfs1.create(file);
        boolean exists = dfs1.exists(file);
        assertTrue(exists);
        FileChecksum checksum = dfs1.getFileChecksum(file);
        LOG.debug("File checksum is: " + checksum.toString());
    }

    @Test
    public void testRpcCallNonValidCert() throws Exception {
        dfs1 = DistributedFileSystem.newInstance(conf);

        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(), err_clientKeyStore.toString());
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue(), passwd);
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(), err_clientTrustStore.toString());
        conf.set(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(), passwd);

        // Exception will be thrown later. JUnit does not execute the code
        // after the exception, so make the call in a separate thread
        invoker = new Thread(new Invoker(dfs1));
        invoker.start();

        if (error_mode.equals(CERT_ERR.NO_CA)) {
            rule.expect(SSLException.class);
        } else if (error_mode.equals(CERT_ERR.ERR_CN)) {
            rule.expect(RemoteException.class);            
        }
        dfs2 = DistributedFileSystem.newInstance(conf);
    }

    private class Invoker implements Runnable {
        private final FileSystem dfs;

        public Invoker(FileSystem dfs) {
            this.dfs = dfs;
        }

        @Override
        public void run() {
            try {
                TimeUnit.SECONDS.sleep(1);
                LOG.debug("Making RPC call from the correct client");
                Path file = new Path("some_file");
                dfs.create(file);
                boolean exists = dfs.exists(file);
                assertTrue("File: " + file.getName() + " should have been created", exists);
            } catch (Exception ex) {
                LOG.error(ex, ex);
            }
        }
    }
}
