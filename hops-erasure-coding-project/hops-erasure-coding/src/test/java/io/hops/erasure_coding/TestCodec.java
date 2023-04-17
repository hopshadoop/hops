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

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;

public class TestCodec extends TestCase {
  String jsonStr = "    [\n" +
      "      {   \n" +
      "        \"id\"            : \"rs\",\n" +
      "        \"parity_dir\"    : \"/raidrs\",\n" +
      "        \"stripe_length\" : 10,\n" +
      "        \"parity_length\" : 4,\n" +
      "        \"priority\"      : 300,\n" +
      "        \"erasure_code\"  : \"io.hops.erasure_coding" +
      ".ReedSolomonCode\",\n" +
      "        \"description\"   : \"ReedSolomonCode code\",\n" +
      "      },  \n" +
      "      {   \n" +
      "        \"id\"            : \"xor\",\n" +
      "        \"parity_dir\"    : \"/raid\",\n" +
      "        \"stripe_length\" : 10, \n" +
      "        \"parity_length\" : 1,\n" +
      "        \"priority\"      : 100,\n" +
      "        \"erasure_code\"  : \"io.hops.erasure_coding.XORCode\",\n" +
      "      },  \n" +
      "      {   \n" +
      "        \"id\"            : \"sr\",\n" +
      "        \"parity_dir\"    : \"/raidsr\",\n" +
      "        \"stripe_length\" : 10, \n" +
      "        \"parity_length\" : 5, \n" +
      "        \"degree\"        : 2,\n" +
      "        \"erasure_code\"  : \"io.hops.erasure_coding" +
      ".SimpleRegeneratingCode\",\n" +
      "        \"priority\"      : 200,\n" +
      "        \"description\"   : \"SimpleRegeneratingCode code\",\n" +
      "      },  \n" +
          "      {   \n"
       + "        \"id\"            : \"nrs\",\n"
            + "        \"parity_dir\"    : \"/raidnrs\",\n"
            + "        \"stripe_length\" : 10, \n"
            + "        \"parity_length\" : 4, \n"
            + "        \"erasure_code\"  : \"io.hops.erasure_coding.NativeReedSolomonCode\",\n"
            + "        \"priority\"      : 50,\n"
            + "        \"description\"   : \"Native ReedSolomonCode code\",\n"
            + "      } \n"+
      "    ]\n";

  @Test
    public void testCreation() throws Exception {
        Configuration conf = new Configuration();

        conf.set(DFSConfigKeys.ERASURE_CODING_CODECS_KEY, jsonStr);
        Codec.initializeCodecs(conf);

        assertEquals("xor", Codec.getCodec("xor").id);
        assertEquals("rs", Codec.getCodec("rs").id);
        assertEquals("sr", Codec.getCodec("sr").id);
        assertEquals("nrs", Codec.getCodec("nrs").id);

        List<Codec> codecs = Codec.getCodecs();

        assertEquals(4, codecs.size());

        assertEquals("rs", codecs.get(0).id);
        assertEquals(10, codecs.get(0).stripeLength);
        assertEquals(4, codecs.get(0).parityLength);
        assertEquals(300, codecs.get(0).priority);
        assertEquals("/raidrs", codecs.get(0).parityDirectory);
        assertEquals("ReedSolomonCode code", codecs.get(0).description);

        assertEquals("sr", codecs.get(1).id);
        assertEquals(10, codecs.get(1).stripeLength);
        assertEquals(5, codecs.get(1).parityLength);
        assertEquals(200, codecs.get(1).priority);
        assertEquals("/raidsr", codecs.get(1).parityDirectory);
        assertEquals("SimpleRegeneratingCode code", codecs.get(1).description);

        assertEquals("xor", codecs.get(2).id);
        assertEquals(10, codecs.get(2).stripeLength);
        assertEquals(1, codecs.get(2).parityLength);
        assertEquals(100, codecs.get(2).priority);
        assertEquals("/raid", codecs.get(2).parityDirectory);
        assertEquals(null, codecs.get(2).description);

        assertEquals("nrs", codecs.get(3).id);
        assertEquals(10, codecs.get(3).stripeLength);
        assertEquals(4, codecs.get(3).parityLength);
        assertEquals(50, codecs.get(3).priority);
        assertEquals("/raidnrs", codecs.get(3).parityDirectory);
        assertEquals("Native ReedSolomonCode code", codecs.get(3).description);

        assertTrue(codecs.get(0).createErasureCode(conf) instanceof ReedSolomonCode);
        assertTrue(codecs.get(2).createErasureCode(conf) instanceof XORCode);
        assertTrue(codecs.get(3).createErasureCode(conf) instanceof NativeReedSolomonCode);
    }

    @Test
    public void testMultiThreadCreation()
            throws InterruptedException, ExecutionException {
        final Configuration conf = new Configuration();

        conf.set(DFSConfigKeys.ERASURE_CODING_CODECS_KEY, jsonStr);

        int numThreads = 100;
        ExecutorService excutor = Executors.newFixedThreadPool(numThreads);
        Future<Boolean>[] futures = new Future[numThreads];
        for (int i = 0; i < numThreads; i++) {
            futures[i] = excutor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Codec.initializeCodecs(conf);
                    return true;
                }
            });
        }

        for (int i = 0; i < numThreads; i++) {
            assertTrue(futures[i].get());
        }
    }
}
