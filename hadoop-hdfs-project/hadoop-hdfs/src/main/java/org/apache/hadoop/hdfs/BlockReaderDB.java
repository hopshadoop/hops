package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by salman on 3/29/16.
 */
public class BlockReaderDB implements  BlockReader{
    public static final Log LOG = LogFactory.getLog(BlockReaderDB.class);

    private  final InetSocketAddress dnAddr;
    private  final DatanodeInfo  dnInfo;
    private  final LocatedBlock locBlock;
    private  final byte[] data;
    private  final int startOffset;
    private  final ByteArrayInputStream bis;

    public BlockReaderDB(InetSocketAddress dnAddr, DatanodeInfo dnInfo, LocatedBlock locBlock, byte[] data,
                         final int startOffset) {
        this.dnAddr = dnAddr;
        this.dnInfo = dnInfo;
        this.locBlock = locBlock;
        this.data = data;
        bis = new ByteArrayInputStream(data);

        this.startOffset  = startOffset;
        if(startOffset>0){
            long skipped = bis.skip(startOffset);
            assert  skipped == startOffset;
        }

    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
//      LOG.debug("Stuffed Inode:  BlockReaderDB Read called. Off: "+off+" len: "+len);
      return bis.read(buf, off, len);
    }

    /**
     * Skip the given number of bytes
     *
     * @param n
     */
    @Override
    public long skip(long n) throws IOException {
        return bis.skip(n);
    }

    @Override
    public void close() throws IOException {
//      LOG.debug("Stuffed Inode:  closing the BlockReaderDB");
      bis.close();
    }

    /**
     * Read exactly the given amount of data, throwing an exception
     * if EOF is reached before that amount
     *
     * @param buf
     * @param readOffset
     * @param amtToRead
     */
    @Override
    public void readFully(byte[] buf, int readOffset, int amtToRead) throws IOException {
//      LOG.debug("Stuffed Inode:  BlockReader readFully called. readOffset: "+readOffset+" amtToRead: "+amtToRead);
      int amountRead = bis.read(buf, readOffset, amtToRead);
      if(amountRead < amtToRead){
        throw new IOException("Premature EOF from inputStream");
      }
    }

    /**
     * Similar to {@link #readFully(byte[], int, int)} except that it will
     * not throw an exception on EOF. However, it differs from the simple
     * {@link #read(byte[], int, int)} call in that it is guaranteed to
     * read the data if it is available. In other words, if this call
     * does not throw an exception, then either the buffer has been
     * filled or the next call will return EOF.
     *
     * @param buf
     * @param offset
     * @param len
     */
    @Override
    public int readAll(byte[] buf, int offset, int len) throws IOException {
//      LOG.debug("Stuffed Inode:  BlockReaderDB readAll called. Offset: "+offset+" len: "+len);
      return bis.read(buf, offset, len);
    }
    /**
     * Take the socket used to talk to the DN.
     */
    @Override
    public Socket takeSocket() {
      return null;
    }

    /**
     * Whether the BlockReader has reached the end of its input stream
     * and successfully sent a status code back to the datanode.
     */
    @Override
    public boolean hasSentStatusCode() {
      return false;
    }

    /**
     * @return a reference to the streams this block reader is using.
     */
    @Override
    public IOStreamPair getStreams() {
      return null;
    }

    /**
     * Reads up to buf.remaining() bytes into buf. Callers should use
     * buf.limit(..) to control the size of the desired read.
     * <p/>
     * After a successful call, buf.position() and buf.limit() should be
     * unchanged, and therefore any data can be immediately read from buf.
     * buf.mark() may be cleared or updated.
     * <p/>
     * In the case of an exception, the values of buf.position() and buf.limit()
     * are undefined, and callers should be prepared to recover from this
     * eventuality.
     * <p/>
     * Many implementations will throw {@link UnsupportedOperationException}, so
     * callers that are not confident in support for this method from the
     * underlying filesystem should be prepared to handle that exception.
     * <p/>
     * Implementations should treat 0-length requests as legitimate, and must not
     * signal an error upon their receipt.
     *
     * @param buf the ByteBuffer to receive the results of the read operation. Up to
     *            buf.limit() - buf.position() bytes may be read.
     * @return the number of bytes available to read from buf
     * @throws IOException if there is some error performing the read
     */
    @Override
    public int read(ByteBuffer buf) throws IOException {
      int amountToRead = buf.remaining();
      int initialPosition = buf.position();
      byte buffer[] = new byte[amountToRead];
      int actuallyRead = bis.read(buffer);
      if(actuallyRead > 0){
        buf.put(buffer);
        buf.position(initialPosition);
      }
      return actuallyRead;
    }

}
