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
package org.apache.hadoop.mapreduce.task.reduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MROutputFiles;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl.CompressAwarePath;
import org.apache.hadoop.test.Whitebox;
import org.junit.Assert;
import org.junit.Test;

public class TestMergeManager {

  @Test(timeout=10000)
  public void testMemoryMerge() throws Exception {
    final int TOTAL_MEM_BYTES = 10000;
    final int OUTPUT_SIZE = 7950;
    JobConf conf = new JobConf();
    conf.setFloat(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, 1.0f);
    conf.setLong(MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES, TOTAL_MEM_BYTES);
    conf.setFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT, 0.8f);
    conf.setFloat(MRJobConfig.SHUFFLE_MERGE_PERCENT, 0.9f);
    TestExceptionReporter reporter = new TestExceptionReporter();
    CyclicBarrier mergeStart = new CyclicBarrier(2);
    CyclicBarrier mergeComplete = new CyclicBarrier(2);
    StubbedMergeManager mgr = new StubbedMergeManager(conf, reporter,
        mergeStart, mergeComplete);

    // reserve enough map output to cause a merge when it is committed
    MapOutput<Text, Text> out1 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertTrue("Should be a memory merge",
                      (out1 instanceof InMemoryMapOutput));
    InMemoryMapOutput<Text, Text> mout1 = (InMemoryMapOutput<Text, Text>)out1;
    fillOutput(mout1);
    MapOutput<Text, Text> out2 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertTrue("Should be a memory merge",
                      (out2 instanceof InMemoryMapOutput));
    InMemoryMapOutput<Text, Text> mout2 = (InMemoryMapOutput<Text, Text>)out2;
    fillOutput(mout2);

    // next reservation should be a WAIT
    MapOutput<Text, Text> out3 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertEquals("Should be told to wait", null, out3);

    // trigger the first merge and wait for merge thread to start merging
    // and free enough output to reserve more
    mout1.commit();
    mout2.commit();
    mergeStart.await();

    Assert.assertEquals(1, mgr.getNumMerges());

    // reserve enough map output to cause another merge when committed
    out1 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertTrue("Should be a memory merge",
                       (out1 instanceof InMemoryMapOutput));
    mout1 = (InMemoryMapOutput<Text, Text>)out1;
    fillOutput(mout1);
    out2 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertTrue("Should be a memory merge",
                       (out2 instanceof InMemoryMapOutput));
    mout2 = (InMemoryMapOutput<Text, Text>)out2;
    fillOutput(mout2);

    // next reservation should be null
    out3 = mgr.reserve(null, OUTPUT_SIZE, 0);
    Assert.assertEquals("Should be told to wait", null, out3);

    // commit output *before* merge thread completes
    mout1.commit();
    mout2.commit();

    // allow the first merge to complete
    mergeComplete.await();

    // start the second merge and verify
    mergeStart.await();
    Assert.assertEquals(2, mgr.getNumMerges());

    // trigger the end of the second merge
    mergeComplete.await();

    Assert.assertEquals(2, mgr.getNumMerges());
    Assert.assertEquals("exception reporter invoked",
        0, reporter.getNumExceptions());
  }

  private void fillOutput(InMemoryMapOutput<Text, Text> output) throws IOException {
    BoundedByteArrayOutputStream stream = output.getArrayStream();
    int count = stream.getLimit();
    for (int i=0; i < count; ++i) {
      stream.write(i);
    }
  }

  private static class StubbedMergeManager extends MergeManagerImpl<Text, Text> {
    private TestMergeThread mergeThread;

    public StubbedMergeManager(JobConf conf, ExceptionReporter reporter,
        CyclicBarrier mergeStart, CyclicBarrier mergeComplete) {
      super(null, conf, mock(LocalFileSystem.class), null, null, null, null,
          null, null, null, null, reporter, null, mock(MapOutputFile.class));
      mergeThread.setSyncBarriers(mergeStart, mergeComplete);
    }

    @Override
    protected MergeThread<InMemoryMapOutput<Text, Text>, Text, Text> createInMemoryMerger() {
      mergeThread = new TestMergeThread(this, getExceptionReporter());
      return mergeThread;
    }

    public int getNumMerges() {
      return mergeThread.getNumMerges();
    }
  }

  private static class TestMergeThread
  extends MergeThread<InMemoryMapOutput<Text,Text>, Text, Text> {
    private AtomicInteger numMerges;
    private CyclicBarrier mergeStart;
    private CyclicBarrier mergeComplete;

    public TestMergeThread(MergeManagerImpl<Text, Text> mergeManager,
        ExceptionReporter reporter) {
      super(mergeManager, Integer.MAX_VALUE, reporter);
      numMerges = new AtomicInteger(0);
    }

    public synchronized void setSyncBarriers(
        CyclicBarrier mergeStart, CyclicBarrier mergeComplete) {
      this.mergeStart = mergeStart;
      this.mergeComplete = mergeComplete;
    }

    public int getNumMerges() {
      return numMerges.get();
    }

    @Override
    public void merge(List<InMemoryMapOutput<Text, Text>> inputs)
        throws IOException {
      synchronized (this) {
        numMerges.incrementAndGet();
        for (InMemoryMapOutput<Text, Text> input : inputs) {
          manager.unreserve(input.getSize());
        }
      }

      try {
        mergeStart.await();
        mergeComplete.await();
      } catch (InterruptedException e) {
      } catch (BrokenBarrierException e) {
      }
    }
  }

  private static class TestExceptionReporter implements ExceptionReporter {
    private List<Throwable> exceptions = new ArrayList<Throwable>();

    @Override
    public void reportException(Throwable t) {
      exceptions.add(t);
      t.printStackTrace();
    }

    public int getNumExceptions() {
      return exceptions.size();
    }
  }

  @Test
  public void testIoSortDefaults() {
    final JobConf jobConf = new JobConf();
    assertEquals(10, jobConf.getInt(MRJobConfig.IO_SORT_FACTOR, 100));
    assertEquals(100, jobConf.getInt(MRJobConfig.IO_SORT_MB, 10));
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Test(timeout=10000)
  public void testOnDiskMerger() throws IOException, URISyntaxException,
    InterruptedException {
    JobConf jobConf = new JobConf();
    final int SORT_FACTOR = 5;
    jobConf.setInt(MRJobConfig.IO_SORT_FACTOR, SORT_FACTOR);

    MapOutputFile mapOutputFile = new MROutputFiles();
    FileSystem fs = FileSystem.getLocal(jobConf);
    MergeManagerImpl<IntWritable, IntWritable> manager =
      new MergeManagerImpl<IntWritable, IntWritable>(null, jobConf, fs, null
        , null, null, null, null, null, null, null, null, null, mapOutputFile);

    MergeThread<MapOutput<IntWritable, IntWritable>, IntWritable, IntWritable>
      onDiskMerger = (MergeThread<MapOutput<IntWritable, IntWritable>,
        IntWritable, IntWritable>) Whitebox.getInternalState(manager,
          "onDiskMerger");
    int mergeFactor = (Integer) Whitebox.getInternalState(onDiskMerger,
      "mergeFactor");

    // make sure the io.sort.factor is set properly
    assertEquals(mergeFactor, SORT_FACTOR);

    // Stop the onDiskMerger thread so that we can intercept the list of files
    // waiting to be merged.
    onDiskMerger.suspend();

    //Send the list of fake files waiting to be merged
    Random rand = new Random();
    for(int i = 0; i < 2*SORT_FACTOR; ++i) {
      Path path = new Path("somePath");
      CompressAwarePath cap = new CompressAwarePath(path, 1l, rand.nextInt());
      manager.closeOnDiskFile(cap);
    }

    //Check that the files pending to be merged are in sorted order.
    LinkedList<List<CompressAwarePath>> pendingToBeMerged =
      (LinkedList<List<CompressAwarePath>>) Whitebox.getInternalState(
        onDiskMerger, "pendingToBeMerged");
    assertTrue("No inputs were added to list pending to merge",
      pendingToBeMerged.size() > 0);
    for(int i = 0; i < pendingToBeMerged.size(); ++i) {
      List<CompressAwarePath> inputs = pendingToBeMerged.get(i);
      for(int j = 1; j < inputs.size(); ++j) {
        assertTrue("Not enough / too many inputs were going to be merged",
          inputs.size() > 0 && inputs.size() <= SORT_FACTOR);
        assertTrue("Inputs to be merged were not sorted according to size: ",
          inputs.get(j).getCompressedSize()
          >= inputs.get(j-1).getCompressedSize());
      }
    }

  }

  @Test
  public void testLargeMemoryLimits() throws Exception {
    final JobConf conf = new JobConf();
    // Xmx in production
    conf.setLong(MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES,
        8L * 1024 * 1024 * 1024);

    // M1 = Xmx fraction for map outputs
    conf.setFloat(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, 1.0f);

    // M2 = max M1 fraction for a single maple output
    conf.setFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT, 0.95f);

    // M3 = M1 fraction at which in memory merge is triggered
    conf.setFloat(MRJobConfig.SHUFFLE_MERGE_PERCENT, 1.0f);

    // M4 = M1 fraction of map outputs remaining in memory for a reduce
    conf.setFloat(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT, 1.0f);

    final MergeManagerImpl<Text, Text> mgr = new MergeManagerImpl<Text, Text>(
        null, conf, mock(LocalFileSystem.class), null, null, null, null, null,
        null, null, null, null, null, new MROutputFiles());
    assertTrue("Large shuffle area unusable: " + mgr.memoryLimit,
        mgr.memoryLimit > Integer.MAX_VALUE);
    final long maxInMemReduce = mgr.getMaxInMemReduceLimit();
    assertTrue("Large in-memory reduce area unusable: " + maxInMemReduce,
        maxInMemReduce > Integer.MAX_VALUE);
    assertEquals("maxSingleShuffleLimit to be capped at Integer.MAX_VALUE",
        Integer.MAX_VALUE, mgr.maxSingleShuffleLimit);
    verifyReservedMapOutputType(mgr, 10L, "MEMORY");
    verifyReservedMapOutputType(mgr, 1L + Integer.MAX_VALUE, "DISK");
  }

  private void verifyReservedMapOutputType(MergeManagerImpl<Text, Text> mgr,
      long size, String expectedShuffleMode) throws IOException {
    final TaskAttemptID mapId = TaskAttemptID.forName("attempt_0_1_m_1_1");
    final MapOutput<Text, Text> mapOutput = mgr.reserve(mapId, size, 1);
    assertEquals("Shuffled bytes: " + size, expectedShuffleMode,
        mapOutput.getDescription());
    mgr.unreserve(size);
  }

  @Test
  public void testZeroShuffleMemoryLimitPercent() throws Exception {
    final JobConf jobConf = new JobConf();
    jobConf.setFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT, 0.0f);
    final MergeManagerImpl<Text, Text> mgr =
        new MergeManagerImpl<>(null, jobConf, mock(LocalFileSystem.class),
            null, null, null, null, null, null, null, null, null, null,
            new MROutputFiles());
    verifyReservedMapOutputType(mgr, 10L, "DISK");
  }
}
