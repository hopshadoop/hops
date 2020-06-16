/*
 * Copyright (C) 2015 hops.io.
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
package io.hops.metadata;

import com.google.common.collect.Lists;
import io.hops.common.CountersQueue;
import io.hops.metadata.common.entity.IntVariable;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestHdfsVariables {

  private static final int NUM_CONCURRENT_THREADS = 100;

  private enum CounterType{
    INodeId,
    BlockId,
    QuotaId
  }

  @Test
  public void testIncrementINodeIdCounter() throws Exception {
    testIncrementCounter(CounterType.INodeId, 100);
  }

  @Test
  public void testUpdateIntToLongCounter() throws Exception {
    IntVariable intVar = new IntVariable(10);
    LongVariable longVar = new LongVariable(-1);
    longVar.setValue(intVar.getBytes());
    assertEquals(intVar.getValue().longValue(), longVar.getValue().longValue());
  }
  
  @Test
  public void testIncrementBlockIdCounter() throws Exception {
    testIncrementCounter(CounterType.BlockId, 100);
  }

  @Test
  public void testIncrementQuotaIdCounter() throws Exception{
    testIncrementCounter(CounterType.QuotaId, 100);
  }

  @Test
  public void testIncrementalCounterOverflow() throws Exception {
    try {
      testIncrementCounter(CounterType.INodeId, Long.MAX_VALUE / 5);
      fail("overflow exception was expected");
    }catch (ExecutionException ex){
      if(!ex.getCause().getMessage().contains("overflow")){
        throw ex;
      }
    }
  }

  void testIncrementCounter(final CounterType counterType, final long increment)
      throws Exception{
    Configuration conf = new HdfsConfiguration();
    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();

    ExecutorService executor = Executors.newFixedThreadPool
        (NUM_CONCURRENT_THREADS);

    List<CounterIncrementer> tasks = Lists.newArrayListWithExpectedSize
        (NUM_CONCURRENT_THREADS);

    for(int i=0; i< NUM_CONCURRENT_THREADS; i++){
      tasks.add(new CounterIncrementer(counterType, increment));
    }

    List<Future<CountersQueue.Counter>> results = executor.invokeAll(tasks);

    executor.shutdown();

    executor.awaitTermination(30, TimeUnit.SECONDS);

    List<CountersQueue.Counter> counters = Lists.newArrayListWithExpectedSize
        (results.size());
    for(Future<CountersQueue.Counter> e : results){
      CountersQueue.Counter counter = e.get();
      assertTrue("incrementCounter shouldn't return null at any time",
          counter != null);
      counters.add(counter);
    }

    Collections.sort(counters, new Comparator<CountersQueue.Counter>() {
      @Override
      public int compare(CountersQueue.Counter o1, CountersQueue.Counter o2) {
        int compare = Long.compare(o1.getStart(), o2.getStart());
        assertTrue("Counter ranges shouldn't have the same start", compare !=
            0);
        return compare;
      }
    });

    for(int i=0; i< counters.size() - 1; i++){
      CountersQueue.Counter c1 = counters.get(i);
      CountersQueue.Counter c2 = counters.get(i+1);

      System.out.println(c1);

      assertEquals("Counters have a range", c1.getStart() + increment,
          c1.getEnd());

      long currentVal=0;
      while (c1.hasNext()){
        currentVal = c1.next();
      }

      assertEquals("Counter increment should be exclude the last element",
          c1.getEnd() - 1,  currentVal);
      assertEquals("Counters should be sequential",c2.getStart(), c1.getEnd());

      assertEquals("Counters should be incremental", c1.getStart() + increment,
          c2.getStart());
    }
  }

  private static class CounterIncrementer
      implements Callable<CountersQueue.Counter>{

    private final CounterType counterType;
    private final long increment;
    public CounterIncrementer(CounterType counterType, long increment){
      this.counterType = counterType;
      this.increment = increment;
    }

    @Override
    public CountersQueue.Counter call() throws Exception {
      switch (counterType){
        case INodeId:
          return HdfsVariables.incrementINodeIdCounter(increment);
        case BlockId:
          return HdfsVariables.incrementBlockIdCounter(increment);
        case QuotaId:
          return HdfsVariables.incrementQuotaUpdateIdCounter(increment);
      }
      return null;
    }
  }

  @Test
  public void testCountersQueue(){
    final int start = 0;
    final int end  = 100000;
    final int inc = 1000;
    final int incGabs = inc * 3;

    CountersQueue queue = new CountersQueue();

    int size = 0;
    for(int i=start; i<end; i+= incGabs){
      CountersQueue.Counter counter = new CountersQueue.Counter(i, i+inc);
      System.out.println("add " + counter);
      queue.addCounter(counter);
      size += inc;
    }

    assertTrue("CountersQueue should have " + size + " Elements", queue.has(size) &&
        !queue.has(size+1));

    int index = 1;
    long current = start;
    while(queue.has(size) && size != 0){

      long e = queue.next();
      System.out.println("got " + e);
      assertEquals(current, e);

      if(index == inc){
        index = 1;
        current += (incGabs - inc);
      }else {
        index++;
      }

      current++;

      size--;
    }

    assertFalse("CountersQueue shouldn't have 0 elements", queue.has(size));

    try
    {
      queue.next();
      fail("CountersQueue should have failed with empty exception");
    }catch (CountersQueue.EmptyCountersQueueException ex){

    }

  }

}
