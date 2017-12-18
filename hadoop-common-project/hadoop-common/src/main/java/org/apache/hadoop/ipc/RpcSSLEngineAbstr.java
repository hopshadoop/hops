/*
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
package org.apache.hadoop.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.HopsSSLSocketFactory;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class RpcSSLEngineAbstr implements RpcSSLEngine {

    private final static Log LOG = LogFactory.getLog(RpcSSLEngineAbstr.class);
    protected final SocketChannel socketChannel;
    protected final SSLEngine sslEngine;
    protected final static int KB = 1024;
    private final ExecutorService exec = Executors.newSingleThreadExecutor();

    /**
     *
     *          serverApp   clientApp
     *          Buffer      Buffer
     *
     *              |           ^
     *              |     |     |
     *              v     |     |
     *         +----+-----|-----+----+
     *         |          |          |
     *         |       SSL|Engine    |
     * wrap()  |          |          |  unwrap()
     *         | OUTBOUND | INBOUND  |
     *         |          |          |
     *         +----+-----|-----+----+
     *              |     |     ^
     *              |     |     |
     *              v           |
     *
     *          serverNet   clientNet
     *          Buffer      Buffer
     */
    protected ByteBuffer serverAppBuffer;
    protected ByteBuffer clientAppBuffer;
    protected ByteBuffer serverNetBuffer;
    protected ByteBuffer clientNetBuffer;

    public RpcSSLEngineAbstr(SocketChannel socketChannel, SSLEngine sslEngine) {
        this.socketChannel = socketChannel;
        this.sslEngine = sslEngine;
        //serverAppBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        serverAppBuffer = ByteBuffer.allocate(100 * KB);
        //clientAppBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        clientAppBuffer = ByteBuffer.allocate(100 * KB);
        //serverNetBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        serverNetBuffer = ByteBuffer.allocate(100 * KB);
        //clientNetBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        clientNetBuffer = ByteBuffer.allocate(100 * KB);
    }

    @Override
    public boolean doHandshake() throws IOException {
        LOG.debug("Starting TLS handshake with peer");

        SSLEngineResult result;
        SSLEngineResult.HandshakeStatus handshakeStatus;

        ByteBuffer serverAppBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        ByteBuffer clientAppBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        serverNetBuffer.clear();
        clientNetBuffer.clear();

        handshakeStatus = sslEngine.getHandshakeStatus();
        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
                && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    if (socketChannel.read(clientNetBuffer) < 0) {
                        if (sslEngine.isInboundDone() && sslEngine.isOutboundDone()) {
                            return false;
                        }
                        try {
                            sslEngine.closeInbound();
                        } catch (SSLException ex) {
                            //LOG.error(ex, ex);
                        }
                        sslEngine.closeOutbound();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    }
                    clientNetBuffer.flip();
                    try {
                        result = sslEngine.unwrap(clientNetBuffer, clientAppBuffer);
                        clientNetBuffer.compact();
                        handshakeStatus = result.getHandshakeStatus();
                    } catch (SSLException ex) {
                        LOG.error(ex, ex);
                        sslEngine.closeOutbound();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    }
                    switch (result.getStatus()) {
                        case OK:
                            break;
                        case BUFFER_OVERFLOW:
                            // clientAppBuffer is not large enough
                            clientAppBuffer = enlargeApplicationBuffer(clientAppBuffer);
                            break;
                        case BUFFER_UNDERFLOW:
                            // Not enough input data to unwrap or the input buffer is too small
                            clientNetBuffer = handleBufferUnderflow(clientNetBuffer);
                            break;
                        case CLOSED:
                            if (sslEngine.isOutboundDone()) {
                                return false;
                            } else {
                                sslEngine.closeOutbound();
                                handshakeStatus = sslEngine.getHandshakeStatus();
                                break;
                            }
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                    break;
                case NEED_WRAP:
                    serverNetBuffer.clear();
                    try {
                        result = sslEngine.wrap(serverAppBuffer, serverNetBuffer);
                        handshakeStatus = result.getHandshakeStatus();
                    } catch (SSLException ex) {
                        LOG.error(ex, ex);
                        sslEngine.closeOutbound();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    }
                    switch (result.getStatus()) {
                        case OK:
                            serverNetBuffer.flip();
                            while (serverNetBuffer.hasRemaining()) {
                                socketChannel.write(serverNetBuffer);
                            }
                            break;
                        case BUFFER_OVERFLOW:
                            serverNetBuffer = enlargePacketBuffer(serverNetBuffer);
                            break;
                        case BUFFER_UNDERFLOW:
                            throw new SSLException("Buffer overflow occurred after a wrap.");
                        case CLOSED:
                            try {
                                serverNetBuffer.flip();
                                while (serverNetBuffer.hasRemaining()) {
                                    socketChannel.write(serverNetBuffer);
                                }
                                clientNetBuffer.clear();
                            } catch (Exception ex) {
                                LOG.error(ex, ex);
                                handshakeStatus = sslEngine.getHandshakeStatus();
                            }
                            break;
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                    break;
                case NEED_TASK:
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        exec.execute(task);
                    }
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;
                case FINISHED:
                    break;
                case NOT_HANDSHAKING:
                    break;
                default:
                    throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
            }
        }

        return true;
    }

    @Override
    public void close() throws IOException {
        sslEngine.closeOutbound();
        doHandshake();
        if (exec != null) {
            exec.shutdown();
        }
    }

    public abstract int write(WritableByteChannel channel, ByteBuffer buffer)
            throws IOException;

    public abstract int read(ReadableByteChannel channel, ByteBuffer buffer, Server.Connection connection)
        throws IOException;

    public static SSLContext initializeSSLContext(Configuration conf) throws IOException {
        SSLContext sslCtx = null;
        try {
            String enabledProtocol = conf.get(HopsSSLSocketFactory.CryptoKeys
                .SOCKET_ENABLED_PROTOCOL.getValue(),
                HopsSSLSocketFactory.CryptoKeys.SOCKET_ENABLED_PROTOCOL
                    .getDefaultValue());
            sslCtx = SSLContext.getInstance(enabledProtocol);

            String keyStoreFilePath = conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getValue(),
                    HopsSSLSocketFactory.CryptoKeys.KEY_STORE_FILEPATH_KEY.getDefaultValue());
            String keyStorePassword = conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getValue(),
                    HopsSSLSocketFactory.CryptoKeys.KEY_STORE_PASSWORD_KEY.getDefaultValue());
            String keyPassword = conf.get(HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getValue(),
                    HopsSSLSocketFactory.CryptoKeys.KEY_PASSWORD_KEY.getDefaultValue());
            String trustStoreFilePath = conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getValue(),
                    HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_FILEPATH_KEY.getDefaultValue());
            String trustStorePassword = conf.get(HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getValue(),
                    HopsSSLSocketFactory.CryptoKeys.TRUST_STORE_PASSWORD_KEY.getDefaultValue());

            sslCtx.init(createKeyManager(keyStoreFilePath, keyStorePassword, keyPassword),
                    createTrustManager(trustStoreFilePath, trustStorePassword),
                    new SecureRandom());
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            handleException(ex);
        }

        return sslCtx;
    }

    // Helper method to initialize key managers needed by SSLContext
    private static KeyManager[] createKeyManager(String filePath, String keyStorePasswd, String keyPasswd)
        throws IOException {

        KeyManager[] keyManagers = null;
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(filePath), keyStorePasswd.toCharArray());

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(keyStore, keyPasswd.toCharArray());

            keyManagers = kmf.getKeyManagers();
        } catch (KeyStoreException | NoSuchAlgorithmException
                | CertificateException | UnrecoverableKeyException ex) {
            handleException(ex);
        }

        return keyManagers;
    }

    // Helper method to initialize trust manager needed by SSLContext
    private static TrustManager[] createTrustManager(String filePath, String keyStorePasswd)
            throws IOException {
        TrustManager[] trustManagers = null;

        try {
            KeyStore trustStore = KeyStore.getInstance("JKS");
            trustStore.load(new FileInputStream(filePath), keyStorePasswd.toCharArray());

            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(trustStore);

            trustManagers = tmf.getTrustManagers();
        } catch (KeyStoreException | NoSuchAlgorithmException
                | CertificateException ex) {
            handleException(ex);
        }

        return trustManagers;
    }

    private static void handleException(Throwable ex) throws IOException {
        String errorPrefix = "Error while initializing cryptographic material ";
        LOG.error(errorPrefix + ex, ex);
        throw new IOException(errorPrefix, ex);
    }

    protected ByteBuffer enlargeApplicationBuffer(ByteBuffer buffer) {
        return enlargeBuffer(buffer, sslEngine.getSession().getApplicationBufferSize());
    }

    protected ByteBuffer enlargePacketBuffer(ByteBuffer buffer) {
        return enlargeBuffer(buffer, sslEngine.getSession().getPacketBufferSize());
    }

    protected ByteBuffer handleBufferUnderflow(ByteBuffer buffer) {
        // If there is no size issue, return the same buffer and let the
        // peer read more data
        if (sslEngine.getSession().getPacketBufferSize() < buffer.limit()) {
            return buffer;
        } else {
            ByteBuffer newBuffer = enlargePacketBuffer(buffer);
            buffer.flip();
            newBuffer.put(buffer);
            return newBuffer;
        }
    }

    private ByteBuffer enlargeBuffer(ByteBuffer buffer, int sessionProposedCapacity) {
        if (sessionProposedCapacity > buffer.capacity()) {
            return ByteBuffer.allocate(sessionProposedCapacity);
        } else {
            return ByteBuffer.allocate(buffer.capacity() * 2);
        }
    }
}
