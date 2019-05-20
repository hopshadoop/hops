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
import org.apache.hadoop.fs.CommonConfigurationKeys;

import javax.net.ssl.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class RpcSSLEngineAbstr implements RpcSSLEngine {

    private final static Log LOG = LogFactory.getLog(RpcSSLEngineAbstr.class);
    protected final SocketChannel socketChannel;
    protected final SSLEngine sslEngine;
    private final Configuration conf;
    private final long handshakeTimeoutMS;
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

    public RpcSSLEngineAbstr(SocketChannel socketChannel, SSLEngine sslEngine, Configuration conf) {
        this.socketChannel = socketChannel;
        this.sslEngine = sslEngine;
        this.conf = conf;
        handshakeTimeoutMS = conf.getLong(CommonConfigurationKeys.IPC_SERVER_TLS_HANDSHAKE_TIMEOUT_MS,
            CommonConfigurationKeys.IPC_SERVER_TLS_HANDSHAKE_TIMEOUT_MS_DEFAULT);
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
    public Configuration getConf() {
        return conf;
    }
    
    private String getRemoteHost() {
        try {
            SocketAddress remoteAddress = socketChannel.getRemoteAddress();
            if (remoteAddress == null) {
                return "unknown";
            }
            if (remoteAddress instanceof InetSocketAddress) {
                InetSocketAddress inetRemoteAddress = (InetSocketAddress) remoteAddress;
                return inetRemoteAddress.getHostString() + ":" + inetRemoteAddress.getPort();
            }
            return "unknown";
        } catch (IOException ex) {
            return "unknown";
        }
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

        TimeWatch timer = TimeWatch.start();
        
        handshakeStatus = sslEngine.getHandshakeStatus();
        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
                && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            if (timer.elapsedIn(TimeUnit.MILLISECONDS) > handshakeTimeoutMS) {
                if (LOG.isWarnEnabled()) {
                    String remoteHost = getRemoteHost();
                    LOG.warn("Connection with " + remoteHost + " has been handshaking for too long. Closing the " +
                        "connection!");
                }
                throw new SSLException("TLS handshake time-out. Handshaking for more than " + handshakeTimeoutMS +
                    " milliseconds!");
            }
            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    int inBytes = socketChannel.read(clientNetBuffer);
                    if (inBytes > 0) {
                        timer.reset();
                    } else if (inBytes < 0) {
                        if (sslEngine.isInboundDone() && sslEngine.isOutboundDone()) {
                            return false;
                        }
                        try {
                            sslEngine.closeInbound();
                        } catch (SSLException ex) {
                            LOG.warn(ex, ex);
                            throw ex;
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
                        LOG.warn(ex, ex);
                        sslEngine.closeOutbound();
                        throw ex;
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
                        LOG.warn(ex, ex);
                        sslEngine.closeOutbound();
                        throw ex;
                    }
                    switch (result.getStatus()) {
                        case OK:
                            serverNetBuffer.flip();
                            while (serverNetBuffer.hasRemaining()) {
                                socketChannel.write(serverNetBuffer);
                            }
                            timer.reset();
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
                                timer.reset();
                                clientNetBuffer.clear();
                            } catch (Exception ex) {
                                LOG.warn(ex, ex);
                                throw ex;
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
        doHandshake();
        if (exec != null) {
            try {
                exec.shutdown();
                if (!exec.awaitTermination(20L, TimeUnit.MILLISECONDS)) {
                    exec.shutdownNow();
                }
            } catch (InterruptedException ex) {
                exec.shutdownNow();
            }
        }
    }

    public abstract int write(WritableByteChannel channel, ByteBuffer buffer)
            throws IOException;

    public abstract int read(ReadableByteChannel channel, ByteBuffer buffer, Server.Connection connection)
        throws IOException;
    

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
