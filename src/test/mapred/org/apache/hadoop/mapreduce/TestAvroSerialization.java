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
package org.apache.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import org.apache.avro.util.Utf8;
import org.apache.avro.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.avro.WordCountKey;
import org.apache.hadoop.mapreduce.avro.WordCountVal;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobdata.AvroGenericJobData;
import org.apache.hadoop.mapreduce.lib.jobdata.AvroSpecificJobData;
import org.apache.hadoop.mapreduce.lib.jobdata.AvroReflectJobData;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Test the aspects of the MapReduce framework where serialization can be
 * conducted via Avro instead of (e.g.) WritablESerialization.
 */
public class TestAvroSerialization extends TestCase {

  private static String TEST_ROOT_DIR =
    new File(System.getProperty("test.build.data", "/tmp")).toURI()
    .toString().replace(' ', '+');

  private final Path INPUT_DIR = new Path(TEST_ROOT_DIR + "/input");
  private final Path OUTPUT_DIR = new Path(TEST_ROOT_DIR + "/out");
  private final Path INPUT_FILE = new Path(INPUT_DIR , "inp");

  // MapReduce classes using AvroGenericSerialization.

  static class GenericWordCountMapper
      extends Mapper<LongWritable, Text, Utf8, Long> {

    public void map(LongWritable key, Text value,
        Context context) throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(value.toString());
      while (st.hasMoreTokens()) {
        context.write(new Utf8(st.nextToken()), 1L);
      }
    }
  }

  static class GenericSumReducer extends Reducer<Utf8, Long, Text, LongWritable> {

    public void reduce(Utf8 key, Iterable<Long> values,
        Context context) throws IOException, InterruptedException {

      long sum = 0;
      for (long val : values) {
        sum += val;
      }
      context.write(new Text(key.toString()), new LongWritable(sum));
    }
  }

  // MapReduce classes that use AvroSpecificSerialization.

  static class SpecificWordCountMapper
      extends Mapper<LongWritable, Text, WordCountKey, WordCountVal> {

    public void map(LongWritable key, Text value,
        Context context) throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(value.toString());
      WordCountKey outkey = new WordCountKey();
      WordCountVal outval = new WordCountVal();
      while (st.hasMoreTokens()) {
        outkey.word = new Utf8(st.nextToken());
        outval.subcount = 1;
        context.write(outkey, outval);
      }
    }
  }

  static class SpecificSumReducer
      extends Reducer<WordCountKey, WordCountVal, Text, LongWritable> {

    public void reduce(WordCountKey key, Iterable<WordCountVal> values,
        Context context) throws IOException, InterruptedException {

      long sum = 0;
      for (WordCountVal val : values) {
        sum += val.subcount;
      }
      context.write(new Text(key.word.toString()), new LongWritable(sum));
    }
  }

  // MapReduce classes that use AvroReflectSerialization.

  static class ReflectableWordCountKey {
    public Utf8 word = new Utf8("");
  }

  static class ReflectableWordCountVal {
    public long subcount;
  }

  static class ReflectableWordCountMapper extends Mapper<LongWritable,
      Text, ReflectableWordCountKey, ReflectableWordCountVal> {

    public void map(LongWritable key, Text value,
        Context context) throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(value.toString());
      ReflectableWordCountKey outkey = new ReflectableWordCountKey();
      ReflectableWordCountVal outval = new ReflectableWordCountVal();
      while (st.hasMoreTokens()) {
        outkey.word = new Utf8(st.nextToken());
        outval.subcount = 1;
        context.write(outkey, outval);
      }
    }
  }

  static class ReflectableSumReducer
      extends Reducer<ReflectableWordCountKey, ReflectableWordCountVal,
      Text, LongWritable> {

    public void reduce(ReflectableWordCountKey key,
        Iterable<ReflectableWordCountVal> values,
        Context context) throws IOException, InterruptedException {

      long sum = 0;
      for (ReflectableWordCountVal val : values) {
        sum += val.subcount;
      }
      context.write(new Text(key.word.toString()), new LongWritable(sum));
    }
  }

  private void cleanAndCreateInput(FileSystem fs) throws IOException {
    fs.delete(INPUT_FILE, true);
    fs.delete(OUTPUT_DIR, true);

    OutputStream os = fs.create(INPUT_FILE);

    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();
  }

  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    cleanAndCreateInput(fs);
  }

  private Job createJob() throws IOException {
    Configuration conf = new Configuration();
    Job job = new Job(conf);
    job.setJarByClass(TestAvroSerialization.class);
    job.setInputFormatClass(TextInputFormat.class);

    // Final output types are still Writable-based.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    FileInputFormat.setInputPaths(job, INPUT_DIR);
    FileOutputFormat.setOutputPath(job, OUTPUT_DIR);

    return job;
  }

  private void checkResults() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(OUTPUT_DIR, 
                      new Utils.OutputFileUtils.OutputFilesFilter()));
    assertEquals(1, outputFiles.length);
    InputStream is = fs.open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    assertEquals("a\t1", reader.readLine());
    assertEquals("b\t1", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }

  // Test that we can use AvroGenericSerialization for intermediate data
  public void testGenericIntermediateData() throws Exception {
    Job job = createJob();
    job.setJobName("AvroGenericSerialization");

    job.setMapperClass(GenericWordCountMapper.class);
    job.setReducerClass(GenericSumReducer.class);

    // Set intermediate types based on Avro schemas.
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valSchema = Schema.create(Schema.Type.LONG);
    AvroGenericJobData.setMapOutputKeySchema(job.getConfiguration(),
        keySchema);
    AvroGenericJobData.setMapOutputValueSchema(job.getConfiguration(),
        valSchema);

    job.waitForCompletion(false);

    checkResults();
  }

  public void testSpecificIntermediateData() throws Exception {
    Job job = createJob();
    job.setJobName("AvroSpecificSerialization");

    job.setMapperClass(SpecificWordCountMapper.class);
    job.setReducerClass(SpecificSumReducer.class);

    // Set intermediate types based on specific-records.
    AvroSpecificJobData.setMapOutputKeyClass(job.getConfiguration(),
        WordCountKey.class);
    AvroSpecificJobData.setMapOutputValueClass(job.getConfiguration(),
        WordCountVal.class);

    job.waitForCompletion(false);

    checkResults();
  }

  public void testReflectIntermediateData() throws Exception {
    Job job = createJob();
    job.setJobName("AvroReflectSerialization");

    job.setMapperClass(ReflectableWordCountMapper.class);
    job.setReducerClass(ReflectableSumReducer.class);

    // Set intermediate types based on reflection records.
    job.getConfiguration().set("avro.reflect.pkgs", "org.apache.hadoop.mapreduce");
    AvroReflectJobData.setMapOutputKeyClass(job.getConfiguration(),
        ReflectableWordCountKey.class);
    AvroReflectJobData.setMapOutputValueClass(job.getConfiguration(),
        ReflectableWordCountVal.class);

    job.waitForCompletion(false);

    checkResults();
  }
}
