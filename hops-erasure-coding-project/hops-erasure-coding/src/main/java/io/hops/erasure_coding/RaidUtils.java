/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.erasure_coding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class RaidUtils {
  public static Progressable NULL_PROGRESSABLE = new Progressable() {
    /**
     * Do nothing.
     **/
    @Override
    public void progress() {
    }
  };

  public static int readTillEnd(InputStream in, byte[] buf, boolean eofOK,
      long endOffset, int toRead) throws IOException {
    int numRead = 0;
    while (numRead < toRead) {
      int readLen = toRead - numRead;
      if (in instanceof DFSDataInputStream) {
        int available = (int) (endOffset - ((DFSDataInputStream) in).getPos());
        if (available < readLen) {
          readLen = available;
        }
      }
      int nread = readLen > 0 ? in.read(buf, numRead, readLen) : 0;
      if (nread < 0) {
        if (eofOK) {
          // EOF hit, fill with zeros
          Arrays.fill(buf, numRead, toRead, (byte) 0);
          break;
        } else {
          // EOF hit, throw.
          throw new IOException("Premature EOF");
        }
      } else if (nread == 0) {
        // reach endOffset, fill with zero;
        Arrays.fill(buf, numRead, toRead, (byte) 0);
        break;
      } else {
        numRead += nread;
      }
    }
    
    // return 0 if we read a ZeroInputStream
    if (in instanceof ZeroInputStream) {
      return 0;
    }
    return numRead;
  }

  public static void copyBytes(InputStream in, OutputStream out, byte[] buf,
      long count) throws IOException {
    for (long bytesRead = 0; bytesRead < count; ) {
      int toRead = Math.min(buf.length, (int) (count - bytesRead));
      IOUtils.readFully(in, buf, 0, toRead);
      bytesRead += toRead;
      out.write(buf, 0, toRead);
    }
  }

  /**
   * Parse a condensed configuration option and set key:value pairs.
   *
   * @param conf
   *     the configuration object.
   * @param optionKey
   *     the name of condensed option. The value corresponding
   *     to this should be formatted as key:value,key:value...
   */
  public static void parseAndSetOptions(Configuration conf, String optionKey) {
    String optionValue = conf.get(optionKey);
    if (optionValue != null) {
      BaseEncodingManager.LOG.info("Parsing option " + optionKey);
      // Parse the option value to get key:value pairs.
      String[] keyValues = optionValue.trim().split(",");
      for (String keyValue : keyValues) {
        String[] fields = keyValue.trim().split(":");
        String key = fields[0].trim();
        String value = fields[1].trim();
        conf.set(key, value);
      }
    } else {
      BaseEncodingManager.LOG.error("Option " + optionKey + " not found");
    }
  }

  public static void closeStreams(InputStream[] streams) throws IOException {
    if (streams == null) {
      return;
    }
    
    for (InputStream stm : streams) {
      if (stm != null) {
        stm.close();
      }
    }
  }

  public static class ZeroInputStream extends InputStream
      implements Seekable, PositionedReadable {
    private long endOffset;
    private long pos;

    public ZeroInputStream(long endOffset) {
      this.endOffset = endOffset;
      this.pos = 0;
    }

    @Override
    public int read() throws IOException {
      if (pos < endOffset) {
        pos++;
        return 0;
      }
      return -1;
    }

    @Override
    public int available() throws IOException {
      return (int) (endOffset - pos);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public void seek(long seekOffset) throws IOException {
      if (seekOffset < endOffset) {
        pos = seekOffset;
      } else {
        throw new IOException("Illegal Offset" + pos);
      }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int count = 0;
      for (; position < endOffset && count < length; position++) {
        buffer[offset + count] = 0;
        count++;
      }
      return count;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int count = 0;
      for (; position < endOffset && count < length; position++) {
        buffer[offset + count] = 0;
        count++;
      }
      if (count < length) {
        throw new IOException("Premature EOF");
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    public List<ByteBuffer> readFullyScatterGather(long position, int length)
        throws IOException {
      throw new IOException("ScatterGather not implemeted for Raid.");
    }
  }
}
