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
package io.hops.erasure_coding;

import io.hops.metadata.hdfs.entity.EncodingPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.util.Random;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Util {

  public enum Codecs {
    XOR,
    RS,
    SRC,
    NRS
  }

  public static final String JSON_CODEC_ARRAY = "[\n" +
      "  {\n" +
      "    \"id\" : \"xor\",\n" +
      "    \"parity_dir\" : \"/raid\",\n" +
      "    \"stripe_length\" : 10,\n" +
      "    \"parity_length\" : 1,\n" +
      "    \"priority\" : 100,\n" +
      "    \"erasure_code\" : \"io.hops.erasure_coding.XORCode\",\n" +
      "    \"description\" : \"XOR code\",\n" +
      "    \"simulate_block_fix\" : false\n" +
      "  },\n" +
      "  {\n" +
      "    \"id\" : \"rs\",\n" +
      "    \"parity_dir\" : \"/raidrs\",\n" +
      "    \"stripe_length\" : 10,\n" +
      "    \"parity_length\" : 4,\n" +
      "    \"priority\" : 300,\n" +
      "    \"erasure_code\" : \"io.hops.erasure_coding.ReedSolomonCode\",\n" +
      "    \"description\" : \"ReedSolomonCode code\",\n" +
      "    \"simulate_block_fix\" : false\n" +
      "  },\n" +
      "  {\n" +
      "    \"id\" : \"src\",\n" +
      "    \"parity_dir\" : \"/raidsrc\",\n" +
      "    \"stripe_length\" : 10,\n" +
      "    \"parity_length\" : 6,\n" +
      "    \"parity_length_src\" : 2,\n" +
      "    \"erasure_code\" : \"io.hops.erasure_coding.SimpleRegeneratingCode\",\n" +
      "    \"priority\" : 200,\n" +
      "    \"description\" : \"SimpleRegeneratingCode code\",\n" +
      "    \"simulate_block_fix\" : false\n" +
      "  },\n" +
      "      {   \n"
       + "        \"id\"            : \"nrs\",\n"
            + "        \"parity_dir\"    : \"/raidnrs\",\n"
            + "        \"stripe_length\" : 10, \n"
            + "        \"parity_length\" : 4, \n"
            + "        \"erasure_code\"  : \"io.hops.erasure_coding.NativeReedSolomonCode\",\n"
            + "        \"priority\"      : 50,\n"
            + "        \"description\"   : \"Native ReedSolomonCode code\",\n"
            + "      } \n"+
      "]";

  public static void createRandomFile(DistributedFileSystem dfs, Path path,
      long seed, int blockCount, int blockSize) throws IOException {
    FSDataOutputStream out = dfs.create(path);
    byte[] buffer = Util.randomBytes(seed, blockCount, blockSize);
    out.write(buffer, 0, buffer.length);
    out.close();
  }

  public static void createRandomFile(DistributedFileSystem dfs, Path path,
      long seed, int blockCount, int blockSize, EncodingPolicy policy)
      throws IOException {
    FSDataOutputStream out = dfs.create(path, policy);
    byte[] buffer = Util.randomBytes(seed, blockCount, blockSize);
    out.write(buffer, 0, buffer.length);
    out.close();
  }

  public static byte[] randomBytes(long seed, int blockCount, int blockSize) {
    return randomBytes(seed, blockCount * blockSize);
  }

  public static byte[] randomBytes(long seed, int size) {
    final byte[] b = new byte[size];
    final Random rand = new Random(seed);
    rand.nextBytes(b);
    return b;
  }

  public static boolean encodeFile(Configuration conf,
      DistributedFileSystem dfs, Codec codec, Path sourceFile, Path parityPath)
      throws IOException {
    LocalEncodingManager encodingManager = new LocalEncodingManager(conf);

    BaseEncodingManager.Statistics stats = new BaseEncodingManager.Statistics();
    return encodingManager.doFileRaid(conf, sourceFile, parityPath, codec,
        stats, RaidUtils.NULL_PROGRESSABLE, 1, 1, false);
  }

  public static Codec getCodec(Codecs codec) {
    
    try {
      JSONArray jsonArray = (JSONArray)new JSONParser().parse(JSON_CODEC_ARRAY);
      return new Codec((JSONObject) jsonArray.get(codec.ordinal()));
    } catch (ParseException ex) {
      //should not happen we know that the codect arry is corect
    }
    return null;
  }
}
