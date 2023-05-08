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
package org.apache.hadoop.io.compress.snappy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor;
import org.apache.hadoop.test.MultithreadedTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSnappyCompressorDecompressor {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestSnappyCompressorDecompressor.class);

  @Before
  public void before() {
  }

  @Test
  public void testSnappyCompressorSetInputNullPointerException() {
    try {
      SnappyCompressor compressor = new SnappyCompressor();
      compressor.setInput(null, 0, 10);
      fail("testSnappyCompressorSetInputNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // excepted
    } catch (Exception ex) {
      fail("testSnappyCompressorSetInputNullPointerException ex error !!!");
    }
  }

  @Test
  public void testSnappyDecompressorSetInputNullPointerException() {
    try {
      SnappyDecompressor decompressor = new SnappyDecompressor();
      decompressor.setInput(null, 0, 10);
      fail("testSnappyDecompressorSetInputNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testSnappyDecompressorSetInputNullPointerException ex error !!!");
    }
  }

  @Test
  public void testSnappyCompressorSetInputAIOBException() {
    try {
      SnappyCompressor compressor = new SnappyCompressor();
      compressor.setInput(new byte[] {}, -5, 10);
      fail("testSnappyCompressorSetInputAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception ex) {
      fail("testSnappyCompressorSetInputAIOBException ex error !!!");
    }
  }

  @Test
  public void testSnappyDecompressorSetInputAIOUBException() {
    try {
      SnappyDecompressor decompressor = new SnappyDecompressor();
      decompressor.setInput(new byte[] {}, -5, 10);
      fail("testSnappyDecompressorSetInputAIOUBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testSnappyDecompressorSetInputAIOUBException ex error !!!");
    }
  }

  @Test
  public void testSnappyCompressorCompressNullPointerException() {
    try {
      SnappyCompressor compressor = new SnappyCompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      compressor.setInput(bytes, 0, bytes.length);
      compressor.compress(null, 0, 0);
      fail("testSnappyCompressorCompressNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testSnappyCompressorCompressNullPointerException ex error !!!");
    }
  }

  @Test
  public void testSnappyDecompressorCompressNullPointerException() {
    try {
      SnappyDecompressor decompressor = new SnappyDecompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      decompressor.setInput(bytes, 0, bytes.length);
      decompressor.decompress(null, 0, 0);
      fail("testSnappyDecompressorCompressNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testSnappyDecompressorCompressNullPointerException ex error !!!");
    }
  }

  @Test
  public void testSnappyCompressorCompressAIOBException() {
    try {
      SnappyCompressor compressor = new SnappyCompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      compressor.setInput(bytes, 0, bytes.length);
      compressor.compress(new byte[] {}, 0, -1);
      fail("testSnappyCompressorCompressAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testSnappyCompressorCompressAIOBException ex error !!!");
    }
  }
  
  @Test
  public void testSnappyDecompressorCompressAIOBException() {
    try {
      SnappyDecompressor decompressor = new SnappyDecompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      decompressor.setInput(bytes, 0, bytes.length);
      decompressor.decompress(new byte[] {}, 0, -1);
      fail("testSnappyDecompressorCompressAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testSnappyDecompressorCompressAIOBException ex error !!!");
    }
  }    

  @Test
  public void testSnappyCompressDecompress() throws Exception {
    int BYTE_SIZE = 1024 * 54;
    byte[] bytes = BytesGenerator.get(BYTE_SIZE);
    SnappyCompressor compressor = new SnappyCompressor();
    compressor.setInput(bytes, 0, bytes.length);
    assertTrue("SnappyCompressDecompress getBytesRead error !!!",
        compressor.getBytesRead() > 0);
    assertEquals(
        "SnappyCompressDecompress getBytesWritten before compress error !!!",
        0, compressor.getBytesWritten());

    // snappy compression may increase data size.
    // This calculation comes from "Snappy::MaxCompressedLength(size_t)"
    int maxSize = 32 + BYTE_SIZE + BYTE_SIZE / 6;
    byte[] compressed = new byte[maxSize];
    int cSize = compressor.compress(compressed, 0, compressed.length);
    LOG.info("input size: {}", BYTE_SIZE);
    LOG.info("compressed size: {}", cSize);
    assertTrue(
        "SnappyCompressDecompress getBytesWritten after compress error !!!",
        compressor.getBytesWritten() > 0);

    SnappyDecompressor decompressor = new SnappyDecompressor();
    // set as input for decompressor only compressed data indicated with cSize
    decompressor.setInput(compressed, 0, cSize);
    byte[] decompressed = new byte[BYTE_SIZE];
    decompressor.decompress(decompressed, 0, decompressed.length);

    assertTrue("testSnappyCompressDecompress finished error !!!",
        decompressor.finished());
    Assert.assertArrayEquals(bytes, decompressed);
    compressor.reset();
    decompressor.reset();
    assertEquals("decompressor getRemaining error !!!",
        0, decompressor.getRemaining());
  }

  @Test
  public void testCompressorDecompressorEmptyStreamLogic() {
    ByteArrayInputStream bytesIn = null;
    ByteArrayOutputStream bytesOut = null;
    byte[] buf = null;
    BlockDecompressorStream blockDecompressorStream = null;
    try {
      // compress empty stream
      bytesOut = new ByteArrayOutputStream();
      BlockCompressorStream blockCompressorStream = new BlockCompressorStream(
          bytesOut, new SnappyCompressor(), 1024, 0);
      // close without write
      blockCompressorStream.close();

      // check compressed output
      buf = bytesOut.toByteArray();
      assertEquals("empty stream compressed output size != 4", 4, buf.length);

      // use compressed output as input for decompression
      bytesIn = new ByteArrayInputStream(buf);

      // create decompression stream
      blockDecompressorStream = new BlockDecompressorStream(bytesIn,
          new SnappyDecompressor(), 1024);

      // no byte is available because stream was closed
      assertEquals("return value is not -1", -1, blockDecompressorStream.read());
    } catch (Exception e) {
      fail("testCompressorDecompressorEmptyStreamLogic ex error !!!"
          + e.getMessage());
    } finally {
      if (blockDecompressorStream != null)
        try {
          bytesIn.close();
          bytesOut.close();
          blockDecompressorStream.close();
        } catch (IOException e) {
        }
    }
  }

  @Test
  public void testSnappyBlockCompression() {
    int BYTE_SIZE = 1024 * 50;
    int BLOCK_SIZE = 512;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] block = new byte[BLOCK_SIZE];
    byte[] bytes = BytesGenerator.get(BYTE_SIZE);
    try {
      // Use default of 512 as bufferSize and compressionOverhead of
      // (1% of bufferSize + 12 bytes) = 18 bytes (zlib algorithm).
      SnappyCompressor compressor = new SnappyCompressor();
      int off = 0;
      int len = BYTE_SIZE;
      int maxSize = BLOCK_SIZE - 18;
      if (BYTE_SIZE > maxSize) {
        do {
          int bufLen = Math.min(len, maxSize);
          compressor.setInput(bytes, off, bufLen);
          compressor.finish();
          while (!compressor.finished()) {
            compressor.compress(block, 0, block.length);
            out.write(block);
          }
          compressor.reset();
          off += bufLen;
          len -= bufLen;
        } while (len > 0);
      }
      assertTrue("testSnappyBlockCompression error !!!",
          out.toByteArray().length > 0);
    } catch (Exception ex) {
      fail("testSnappyBlockCompression ex error !!!");
    }
  }

  @Test
  // The buffer size is smaller than the input.
  public void testSnappyCompressDecompressWithSmallBuffer() throws Exception {
    int inputSize = 1024 * 50;
    int bufferSize = 512;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[bufferSize];
    byte[] input = BytesGenerator.get(inputSize);

    SnappyCompressor compressor = new SnappyCompressor();
    compressor.setInput(input, 0, inputSize);
    compressor.finish();
    while (!compressor.finished()) {
      int len = compressor.compress(buffer, 0, buffer.length);
      out.write(buffer, 0, len);
    }
    byte[] compressed = out.toByteArray();
    assertThat(compressed).hasSizeGreaterThan(0);
    out.reset();

    SnappyDecompressor decompressor = new SnappyDecompressor();
    decompressor.setInput(compressed, 0, compressed.length);
    while (!decompressor.finished()) {
      int len = decompressor.decompress(buffer, 0, buffer.length);
      out.write(buffer, 0, len);
    }
    byte[] decompressed = out.toByteArray();

    assertThat(decompressed).isEqualTo(input);
  }

  private void compressDecompressLoop(int rawDataSize) throws IOException {
    byte[] rawData = BytesGenerator.get(rawDataSize);    
    byte[] compressedResult = new byte[rawDataSize+20];
    int directBufferSize = Math.max(rawDataSize*2, 64*1024);    
    SnappyCompressor compressor = new SnappyCompressor(directBufferSize);
    compressor.setInput(rawData, 0, rawDataSize);
    int compressedSize = compressor.compress(compressedResult, 0, compressedResult.length);
    SnappyDirectDecompressor decompressor = new SnappyDirectDecompressor();
   
    ByteBuffer inBuf = ByteBuffer.allocateDirect(compressedSize);
    ByteBuffer outBuf = ByteBuffer.allocateDirect(rawDataSize);

    inBuf.put(compressedResult, 0, compressedSize);
    inBuf.flip();    

    ByteBuffer expected = ByteBuffer.wrap(rawData);
    
    outBuf.clear();
    while(!decompressor.finished()) {
      decompressor.decompress(inBuf, outBuf);
      if (outBuf.remaining() == 0) {
        outBuf.flip();
        while (outBuf.remaining() > 0) {        
          assertEquals(expected.get(), outBuf.get());
        }
        outBuf.clear();
      }
    }
    outBuf.flip();
    while (outBuf.remaining() > 0) {        
      assertEquals(expected.get(), outBuf.get());
    }
    outBuf.clear();
    
    assertEquals(0, expected.remaining());
  }
  
  @Test
  public void testSnappyDirectBlockCompression() {
    int[] size = new int[] {
        4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024
    };
    try {
      for (int i = 0; i < size.length; i++) {
        compressDecompressLoop(size[i]);
      }
    } catch (IOException ex) {
      fail("testSnappyDirectBlockCompression ex !!!" + ex);
    }
  }

  @Test
  public void testSnappyCompressorDecopressorLogicWithCompressionStreams() {
    int BYTE_SIZE = 1024 * 100;
    byte[] bytes = BytesGenerator.get(BYTE_SIZE);
    int bufferSize = 262144;
    int compressionOverhead = (bufferSize / 6) + 32;
    DataOutputStream deflateOut = null;
    DataInputStream inflateIn = null;
    try {
      DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
      CompressionOutputStream deflateFilter = new BlockCompressorStream(
          compressedDataBuffer, new SnappyCompressor(bufferSize), bufferSize,
          compressionOverhead);
      deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));

      deflateOut.write(bytes, 0, bytes.length);
      deflateOut.flush();
      deflateFilter.finish();

      DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
      deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
          compressedDataBuffer.getLength());

      CompressionInputStream inflateFilter = new BlockDecompressorStream(
          deCompressedDataBuffer, new SnappyDecompressor(bufferSize),
          bufferSize);

      inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));

      byte[] result = new byte[BYTE_SIZE];
      inflateIn.read(result);

      Assert.assertArrayEquals(
          "original array not equals compress/decompressed array", result,
          bytes);
    } catch (IOException e) {
      fail("testSnappyCompressorDecopressorLogicWithCompressionStreams ex error !!!");
    } finally {
      try {
        if (deflateOut != null)
          deflateOut.close();
        if (inflateIn != null)
          inflateIn.close();
      } catch (Exception e) {
      }
    }
  }

  static final class BytesGenerator {
    private BytesGenerator() {
    }

    private static final byte[] CACHE = new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4,
        0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF };
    private static final Random rnd = new Random(12345l);

    public static byte[] get(int size) {
      byte[] array = (byte[]) Array.newInstance(byte.class, size);
      for (int i = 0; i < size; i++)
        array[i] = CACHE[rnd.nextInt(CACHE.length - 1)];
      return array;
    }
  }

  @Test
  public void testSnappyCompressDecompressInMultiThreads() throws Exception {
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
    for(int i=0;i<10;i++) {
      ctx.addThread( new MultithreadedTestUtil.TestingThread(ctx) {
        @Override
        public void doWork() throws Exception {
          testSnappyCompressDecompress();
        }
      });
    }
    ctx.startThreads();

    ctx.waitFor(60000);
  }

  @Test
  public void testSnappyCompatibility() throws Exception {
    // HADOOP-17125. Using snappy-java in SnappyCodec. These strings are raw
    // data and compressed data using previous native Snappy codec. We use
    // updated Snappy codec to decode it and check if it matches.
    String rawData = "010a06030a040a0c0109020c0a010204020d02000b010701080605" +
            "080b090902060a080502060a0d06070908080a0c0105030904090d050908000" +
            "40c090c0d0d0804000d00040b0b0d010d060907020a030a0c09000409050801" +
            "07040d0c01060a0b09070a04000b01040b09000e0e00020b06050b060e030e0" +
            "a07050d06050d";
    String compressed = "8001f07f010a06030a040a0c0109020c0a010204020d02000b0" +
            "10701080605080b090902060a080502060a0d06070908080a0c010503090409" +
            "0d05090800040c090c0d0d0804000d00040b0b0d010d060907020a030a0c090" +
            "0040905080107040d0c01060a0b09070a04000b01040b09000e0e00020b0605" +
            "0b060e030e0a07050d06050d";

    byte[] rawDataBytes = Hex.decodeHex(rawData);
    byte[] compressedBytes = Hex.decodeHex(compressed);

    ByteBuffer inBuf = ByteBuffer.allocateDirect(compressedBytes.length);
    inBuf.put(compressedBytes, 0, compressedBytes.length);
    inBuf.flip();

    ByteBuffer outBuf = ByteBuffer.allocateDirect(rawDataBytes.length);
    ByteBuffer expected = ByteBuffer.wrap(rawDataBytes);

    SnappyDecompressor.SnappyDirectDecompressor decompressor =
            new SnappyDecompressor.SnappyDirectDecompressor();

    outBuf.clear();
    while(!decompressor.finished()) {
      decompressor.decompress(inBuf, outBuf);
      if (outBuf.remaining() == 0) {
        outBuf.flip();
        while (outBuf.remaining() > 0) {
          assertEquals(expected.get(), outBuf.get());
        }
        outBuf.clear();
      }
    }
    outBuf.flip();
    while (outBuf.remaining() > 0) {
      assertEquals(expected.get(), outBuf.get());
    }
    outBuf.clear();
    assertEquals(0, expected.remaining());
  }
}
