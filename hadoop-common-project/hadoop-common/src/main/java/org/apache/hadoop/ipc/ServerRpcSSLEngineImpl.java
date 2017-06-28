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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.cert.X509Certificate;

public class ServerRpcSSLEngineImpl extends RpcSSLEngineAbstr {
    private final Log LOG = LogFactory.getLog(ServerRpcSSLEngineImpl.class);
    private final int KB = 1024;
    private final int MB = 1024 * KB;
    
    private final int MAX_BUFFER_SIZE = 5 * MB;
    
    public ServerRpcSSLEngineImpl(SocketChannel socketChannel, SSLEngine sslEngine) {
        super(socketChannel, sslEngine);
    }

    @Override
    public int write(WritableByteChannel channel, ByteBuffer buffer)
            throws IOException {
        serverAppBuffer.clear();
        if (serverAppBuffer.capacity() < buffer.capacity()) {
            LOG.error("ServerAppBuffer capacity: " + serverAppBuffer.capacity()
                + " Buffer size: " + buffer.capacity());
            serverAppBuffer = ByteBuffer.allocate(Math.min(buffer.capacity(),
                MAX_BUFFER_SIZE));
        }
        serverAppBuffer.put(buffer);
        serverAppBuffer.flip();

        int bytesWritten = 0;
        while (serverAppBuffer.hasRemaining()) {
            serverNetBuffer.clear();
            SSLEngineResult result = sslEngine.wrap(serverAppBuffer, serverNetBuffer);
            switch (result.getStatus()) {
                case OK:
                    serverNetBuffer.flip();
                    while (serverNetBuffer.hasRemaining()) {
                        bytesWritten += channel.write(serverNetBuffer);
                    }
                    //return bytesWritten;
                    break;
                case BUFFER_OVERFLOW:
                    serverNetBuffer = enlargePacketBuffer(serverNetBuffer);
                    break;
                case BUFFER_UNDERFLOW:
                    throw new SSLException("Buffer underflow should not happen after wrap");
                case CLOSED:
                    sslEngine.closeOutbound();
                    doHandshake();
                    return -1;
                default:
                    throw new IllegalStateException("Invalid SSL state: " + result.getStatus());
            }
        }
        return bytesWritten;
    }
    
    @Override
    public int read(ReadableByteChannel channel, ByteBuffer buffer)
        throws IOException {
        int netRead = channel.read(clientNetBuffer);
        if (netRead == -1) {
            return -1;
        }
        
        int read = 0;
        SSLEngineResult unwrapResult;
        do {
            clientNetBuffer.flip();
            unwrapResult = sslEngine.unwrap(clientNetBuffer, clientAppBuffer);
            clientNetBuffer.compact();
            
            if (unwrapResult.getStatus().equals(SSLEngineResult.Status.OK)) {
                read += unwrapResult.bytesProduced();
                clientAppBuffer.flip();
                while (clientAppBuffer.hasRemaining()) {
                    buffer.put(clientAppBuffer.get());
                }
                clientAppBuffer.compact();
            } else if (unwrapResult.getStatus().equals(SSLEngineResult.Status
                .BUFFER_UNDERFLOW)) {
                read += unwrapResult.bytesProduced();
                break;
            } else if (unwrapResult.getStatus().equals(SSLEngineResult.Status
                .BUFFER_OVERFLOW)) {
                clientAppBuffer = enlargeApplicationBuffer(clientAppBuffer);
            } else if (unwrapResult.getStatus().equals(SSLEngineResult.Status
                .CLOSED)) {
                sslEngine.closeOutbound();
                doHandshake();
                read = -1;
                break;
            } else {
                throw new IOException("SSLEngine UNWRAP invalid status: " +
                    unwrapResult.getStatus());
            }
        } while (clientNetBuffer.position() != 0);
        
        return read;
    }

    public X509Certificate getClientCertificate() throws SSLPeerUnverifiedException {
        // The first certificate is always the peer's own certificate
        // https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLSession.html#getPeerCertificates--
        return (X509Certificate) sslEngine.getSession().getPeerCertificates()[0];
    }
}
