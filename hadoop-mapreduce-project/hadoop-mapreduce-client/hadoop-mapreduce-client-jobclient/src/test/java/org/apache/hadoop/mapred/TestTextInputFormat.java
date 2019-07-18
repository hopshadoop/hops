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

package org.apache.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.BitSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestTextInputFormat {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestTextInputFormat.class);

  private static int MAX_LENGTH = 10000;
  
  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null; 
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir = localFs.makeQualified(new Path(
      System.getProperty("test.build.data", "/tmp"),
      "TestTextInputFormat"));

  @Test (timeout=500000)
  public void testFormat() throws Exception {
    JobConf job = new JobConf(defaultConf);
    Path file = new Path(workDir, "test.txt");

    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
    
    int seed = new Random().nextInt();
    LOG.info("seed = "+seed);
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);

    // for a variety of lengths
    for (int length = 0; length < MAX_LENGTH;
         length+= random.nextInt(MAX_LENGTH/10)+1) {

      LOG.debug("creating; entries = " + length);

      // create a file with length entries
      Writer writer = new OutputStreamWriter(localFs.create(file));
      try {
        for (int i = 0; i < length; i++) {
          writer.write(Integer.toString(i));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }

      // try splitting the file in a variety of sizes
      TextInputFormat format = new TextInputFormat();
      format.configure(job);
      LongWritable key = new LongWritable();
      Text value = new Text();
      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH/20)+1;
        LOG.debug("splitting: requesting = " + numSplits);
        InputSplit[] splits = format.getSplits(job, numSplits);
        LOG.debug("splitting: got =        " + splits.length);

        if (length == 0) {
           assertEquals("Files of length 0 are not returned from FileInputFormat.getSplits().", 
                        1, splits.length);
           assertEquals("Empty file length == 0", 0, splits[0].getLength());
        }

        // check each split
        BitSet bits = new BitSet(length);
        for (int j = 0; j < splits.length; j++) {
          LOG.debug("split["+j+"]= " + splits[j]);
          RecordReader<LongWritable, Text> reader =
            format.getRecordReader(splits[j], job, reporter);
          try {
            int count = 0;
            while (reader.next(key, value)) {
              int v = Integer.parseInt(value.toString());
              LOG.debug("read " + v);
              if (bits.get(v)) {
                LOG.warn("conflict with " + v + 
                         " in split " + j +
                         " at position "+reader.getPos());
              }
              assertFalse("Key in multiple partitions.", bits.get(v));
              bits.set(v);
              count++;
            }
            LOG.debug("splits["+j+"]="+splits[j]+" count=" + count);
          } finally {
            reader.close();
          }
        }
        assertEquals("Some keys in no partition.", length, bits.cardinality());
      }

    }
  }

  @Test (timeout=900000)
  public void testSplitableCodecs() throws IOException {
    JobConf conf = new JobConf(defaultConf);
    int seed = new Random().nextInt();
    // Create the codec
    CompressionCodec codec = null;
    try {
      codec = (CompressionCodec)
      ReflectionUtils.newInstance(conf.getClassByName("org.apache.hadoop.io.compress.BZip2Codec"), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    Path file = new Path(workDir, "test"+codec.getDefaultExtension());

    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;
    LOG.info("seed = "+seed);
    Random random = new Random(seed);
    FileSystem localFs = FileSystem.getLocal(conf);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(conf, workDir);

    final int MAX_LENGTH = 500000;

    // for a variety of lengths
    for (int length = MAX_LENGTH / 2; length < MAX_LENGTH;
        length += random.nextInt(MAX_LENGTH / 4)+1) {

      for (int i = 0; i < 3; i++) {
        int numSplits = random.nextInt(MAX_LENGTH / 2000) + 1;
        verifyPartitions(length, numSplits, file, codec, conf);
      }
    }

    // corner case when we have byte alignment and position of stream are same
    verifyPartitions(471507, 218, file, codec, conf);
    verifyPartitions(473608, 110, file, codec, conf);

    // corner case when split size is small and position of stream is before
    // the first BZip2 block
    verifyPartitions(100, 20, file, codec, conf);
    verifyPartitions(100, 25, file, codec, conf);
    verifyPartitions(100, 30, file, codec, conf);
    verifyPartitions(100, 50, file, codec, conf);
    verifyPartitions(100, 100, file, codec, conf);
  }

  // Test a corner case when position of stream is right after BZip2 marker
  @Test (timeout=900000)
  public void testSplitableCodecs2() throws IOException {
    JobConf conf = new JobConf(defaultConf);
    // Create the codec
    CompressionCodec codec = null;
    try {
      codec = (CompressionCodec)
      ReflectionUtils.newInstance(conf.getClassByName("org.apache.hadoop.io.compress.BZip2Codec"), conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Illegal codec!");
    }
    Path file = new Path(workDir, "test"+codec.getDefaultExtension());

    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(conf, workDir);

    int length = 250000;
    LOG.info("creating; entries = " + length);
    // create a file with length entries
    Writer writer =
        new OutputStreamWriter(codec.createOutputStream(localFs.create(file)));
    try {
      for (int i = 0; i < length; i++) {
        writer.write(Integer.toString(i));
        writer.write("\n");
      }
    } finally {
      writer.close();
    }

    // Test split positions around a block boundary where the block does
    // not start on a byte boundary.
    for (long splitpos = 203418; splitpos < 203430; ++splitpos) {
      TextInputFormat format = new TextInputFormat();
      format.configure(conf);
      LOG.info("setting block size of the input file to " + splitpos);
      conf.setLong("mapreduce.input.fileinputformat.split.minsize", splitpos);
      LongWritable key = new LongWritable();
      Text value = new Text();
      InputSplit[] splits = format.getSplits(conf, 2);
      LOG.info("splitting: got =        " + splits.length);

      // check each split
      BitSet bits = new BitSet(length);
      for (int j = 0; j < splits.length; j++) {
        LOG.debug("split[" + j + "]= " + splits[j]);
        RecordReader<LongWritable, Text> reader =
            format.getRecordReader(splits[j], conf, Reporter.NULL);
        try {
          int counter = 0;
          while (reader.next(key, value)) {
            int v = Integer.parseInt(value.toString());
            LOG.debug("read " + v);
            if (bits.get(v)) {
              LOG.warn("conflict with " + v + " in split " + j +
                  " at position " + reader.getPos());
            }
            assertFalse("Key in multiple partitions.", bits.get(v));
            bits.set(v);
            counter++;
          }
          if (counter > 0) {
            LOG.info("splits[" + j + "]=" + splits[j] + " count=" + counter);
          } else {
            LOG.debug("splits[" + j + "]=" + splits[j] + " count=" + counter);
          }
        } finally {
          reader.close();
        }
      }
      assertEquals("Some keys in no partition.", length, bits.cardinality());
    }
  }

  private void verifyPartitions(int length, int numSplits, Path file,
      CompressionCodec codec, JobConf conf) throws IOException {

    LOG.info("creating; entries = " + length);


    // create a file with length entries
    Writer writer =
        new OutputStreamWriter(codec.createOutputStream(localFs.create(file)));
    try {
      for (int i = 0; i < length; i++) {
        writer.write(Integer.toString(i));
        writer.write("\n");
      }
    } finally {
      writer.close();
    }

    // try splitting the file in a variety of sizes
    TextInputFormat format = new TextInputFormat();
    format.configure(conf);
    LongWritable key = new LongWritable();
    Text value = new Text();
    LOG.info("splitting: requesting = " + numSplits);
    InputSplit[] splits = format.getSplits(conf, numSplits);
    LOG.info("splitting: got =        " + splits.length);


    // check each split
    BitSet bits = new BitSet(length);
    for (int j = 0; j < splits.length; j++) {
      LOG.debug("split["+j+"]= " + splits[j]);
      RecordReader<LongWritable, Text> reader =
              format.getRecordReader(splits[j], conf, Reporter.NULL);
      try {
        int counter = 0;
        while (reader.next(key, value)) {
          int v = Integer.parseInt(value.toString());
          LOG.debug("read " + v);
          if (bits.get(v)) {
            LOG.warn("conflict with " + v +
                    " in split " + j +
                    " at position "+reader.getPos());
          }
          assertFalse("Key in multiple partitions.", bits.get(v));
          bits.set(v);
          counter++;
        }
        if (counter > 0) {
          LOG.info("splits["+j+"]="+splits[j]+" count=" + counter);
        } else {
          LOG.debug("splits["+j+"]="+splits[j]+" count=" + counter);
        }
      } finally {
        reader.close();
      }
    }
    assertEquals("Some keys in no partition.", length, bits.cardinality());
  }

  private static LineReader makeStream(String str) throws IOException {
    return new LineReader(new ByteArrayInputStream
                                             (str.getBytes("UTF-8")), 
                                           defaultConf);
  }
  private static LineReader makeStream(String str, int bufsz) throws IOException {
    return new LineReader(new ByteArrayInputStream
                                             (str.getBytes("UTF-8")), 
                                           bufsz);
  }

  @Test (timeout=5000)
  public void testUTF8() throws Exception {
    LineReader in = makeStream("abcd\u20acbdcd\u20ac");
    Text line = new Text();
    in.readLine(line);
    assertEquals("readLine changed utf8 characters", 
                 "abcd\u20acbdcd\u20ac", line.toString());
    in = makeStream("abc\u200axyz");
    in.readLine(line);
    assertEquals("split on fake newline", "abc\u200axyz", line.toString());
  }

  /**
   * Test readLine for various kinds of line termination sequneces.
   * Varies buffer size to stress test.  Also check that returned
   * value matches the string length.
   *
   * @throws Exception
   */
  @Test (timeout=5000)
  public void testNewLines() throws Exception {
    final String STR = "a\nbb\n\nccc\rdddd\r\r\r\n\r\neeeee";
    final int STRLENBYTES = STR.getBytes().length;
    Text out = new Text();
    for (int bufsz = 1; bufsz < STRLENBYTES+1; ++bufsz) {
      LineReader in = makeStream(STR, bufsz);
      int c = 0;
      c += in.readLine(out); //"a"\n
      assertEquals("line1 length, bufsz:"+bufsz, 1, out.getLength());
      c += in.readLine(out); //"bb"\n
      assertEquals("line2 length, bufsz:"+bufsz, 2, out.getLength());
      c += in.readLine(out); //""\n
      assertEquals("line3 length, bufsz:"+bufsz, 0, out.getLength());
      c += in.readLine(out); //"ccc"\r
      assertEquals("line4 length, bufsz:"+bufsz, 3, out.getLength());
      c += in.readLine(out); //dddd\r
      assertEquals("line5 length, bufsz:"+bufsz, 4, out.getLength());
      c += in.readLine(out); //""\r
      assertEquals("line6 length, bufsz:"+bufsz, 0, out.getLength());
      c += in.readLine(out); //""\r\n
      assertEquals("line7 length, bufsz:"+bufsz, 0, out.getLength());
      c += in.readLine(out); //""\r\n
      assertEquals("line8 length, bufsz:"+bufsz, 0, out.getLength());
      c += in.readLine(out); //"eeeee"EOF
      assertEquals("line9 length, bufsz:"+bufsz, 5, out.getLength());
      assertEquals("end of file, bufsz: "+bufsz, 0, in.readLine(out));
      assertEquals("total bytes, bufsz: "+bufsz, c, STRLENBYTES);
    }
  }

  /**
   * Test readLine for correct interpretation of maxLineLength
   * (returned string should be clipped at maxLineLength, and the
   * remaining bytes on the same line should be thrown out).
   * Also check that returned value matches the string length.
   * Varies buffer size to stress test.
   *
   * @throws Exception
   */
  @Test (timeout=5000)
  public void testMaxLineLength() throws Exception {
    final String STR = "a\nbb\n\nccc\rdddd\r\neeeee";
    final int STRLENBYTES = STR.getBytes().length;
    Text out = new Text();
    for (int bufsz = 1; bufsz < STRLENBYTES+1; ++bufsz) {
      LineReader in = makeStream(STR, bufsz);
      int c = 0;
      c += in.readLine(out, 1);
      assertEquals("line1 length, bufsz: "+bufsz, 1, out.getLength());
      c += in.readLine(out, 1);
      assertEquals("line2 length, bufsz: "+bufsz, 1, out.getLength());
      c += in.readLine(out, 1);
      assertEquals("line3 length, bufsz: "+bufsz, 0, out.getLength());
      c += in.readLine(out, 3);
      assertEquals("line4 length, bufsz: "+bufsz, 3, out.getLength());
      c += in.readLine(out, 10);
      assertEquals("line5 length, bufsz: "+bufsz, 4, out.getLength());
      c += in.readLine(out, 8);
      assertEquals("line5 length, bufsz: "+bufsz, 5, out.getLength());
      assertEquals("end of file, bufsz: " +bufsz, 0, in.readLine(out));
      assertEquals("total bytes, bufsz: "+bufsz, c, STRLENBYTES);
    }
  }

  @Test (timeout=5000)
  public void testMRMaxLine() throws Exception {
    final int MAXPOS = 1024 * 1024;
    final int MAXLINE = 10 * 1024;
    final int BUF = 64 * 1024;
    final InputStream infNull = new InputStream() {
      int position = 0;
      final int MAXPOSBUF = 1024 * 1024 + BUF; // max LRR pos + LineReader buf
      @Override
      public int read() {
        ++position;
        return 0;
      }
      @Override
      public int read(byte[] b) {
        assertTrue("Read too many bytes from the stream", position < MAXPOSBUF);
        Arrays.fill(b, (byte) 0);
        position += b.length;
        return b.length;
      }
      public void reset() {
        position=0;
      }
    };
    final LongWritable key = new LongWritable();
    final Text val = new Text();
    LOG.info("Reading a line from /dev/null");
    final Configuration conf = new Configuration(false);
    conf.setInt(org.apache.hadoop.mapreduce.lib.input.
                LineRecordReader.MAX_LINE_LENGTH, MAXLINE);
    conf.setInt("io.file.buffer.size", BUF); // used by LRR
    // test another constructor 
     LineRecordReader lrr = new LineRecordReader(infNull, 0, MAXPOS, conf);
    assertFalse("Read a line from null", lrr.next(key, val));
    infNull.reset();
     lrr = new LineRecordReader(infNull, 0L, MAXLINE, MAXPOS);
    assertFalse("Read a line from null", lrr.next(key, val));
    
    
  }

  private static void writeFile(FileSystem fs, Path name, 
                                CompressionCodec codec,
                                String contents) throws IOException {
    OutputStream stm;
    if (codec == null) {
      stm = fs.create(name);
    } else {
      stm = codec.createOutputStream(fs.create(name));
    }
    stm.write(contents.getBytes());
    stm.close();
  }
  
  private static final Reporter voidReporter = Reporter.NULL;
  
  private static List<Text> readSplit(TextInputFormat format, 
                                      InputSplit split, 
                                      JobConf job) throws IOException {
    List<Text> result = new ArrayList<Text>();
    RecordReader<LongWritable, Text> reader =
      format.getRecordReader(split, job, voidReporter);
    LongWritable key = reader.createKey();
    Text value = reader.createValue();
    while (reader.next(key, value)) {
      result.add(value);
      value = reader.createValue();
    }
    reader.close();
    return result;
  }
  
  /**
   * Test using the gzip codec for reading
   */
  @Test (timeout=5000)
  public void testGzip() throws IOException {
    JobConf job = new JobConf(defaultConf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip, 
              "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "this is a test\nof gzip\n");
    FileInputFormat.setInputPaths(job, workDir);
    TextInputFormat format = new TextInputFormat();
    format.configure(job);
    InputSplit[] splits = format.getSplits(job, 100);
    assertEquals("compressed splits == 2", 2, splits.length);
    FileSplit tmp = (FileSplit) splits[0];
    if (tmp.getPath().getName().equals("part2.txt.gz")) {
      splits[0] = splits[1];
      splits[1] = tmp;
    }
    List<Text> results = readSplit(format, splits[0], job);
    assertEquals("splits[0] length", 6, results.size());
    assertEquals("splits[0][5]", " dog", results.get(5).toString());
    results = readSplit(format, splits[1], job);
    assertEquals("splits[1] length", 2, results.size());
    assertEquals("splits[1][0]", "this is a test", 
                 results.get(0).toString());    
    assertEquals("splits[1][1]", "of gzip", 
                 results.get(1).toString());    
  }

  /**
   * Test using the gzip codec and an empty input file
   */
  @Test (timeout=5000)
  public void testGzipEmpty() throws IOException {
    JobConf job = new JobConf(defaultConf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "empty.gz"), gzip, "");
    FileInputFormat.setInputPaths(job, workDir);
    TextInputFormat format = new TextInputFormat();
    format.configure(job);
    InputSplit[] splits = format.getSplits(job, 100);
    assertEquals("Compressed files of length 0 are not returned from FileInputFormat.getSplits().",
                 1, splits.length);
    List<Text> results = readSplit(format, splits[0], job);
    assertEquals("Compressed empty file length == 0", 0, results.size());
  }
  
  private static String unquote(String in) {
    StringBuffer result = new StringBuffer();
    for(int i=0; i < in.length(); ++i) {
      char ch = in.charAt(i);
      if (ch == '\\') {
        ch = in.charAt(++i);
        switch (ch) {
        case 'n':
          result.append('\n');
          break;
        case 'r':
          result.append('\r');
          break;
        default:
          result.append(ch);
          break;
        }
      } else {
        result.append(ch);
      }
    }
    return result.toString();
  }

  /**
   * Parse the command line arguments into lines and display the result.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    for(String arg: args) {
      System.out.println("Working on " + arg);
      LineReader reader = makeStream(unquote(arg));
      Text line = new Text();
      int size = reader.readLine(line);
      while (size > 0) {
        System.out.println("Got: " + line.toString());
        size = reader.readLine(line);
      }
      reader.close();
    }
  }
}
