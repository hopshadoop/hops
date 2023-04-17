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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * A class with the information of a raid codec.
 * A raid codec has the information of
 * 1. Which ErasureCode used
 * 2. Stripe and parity length
 * 3. Parity directory location
 * 4. Codec priority
 */
public class Codec implements Serializable {

  public static final Log LOG = LogFactory.getLog(Codec.class);
  
  public static final String ERASURE_CODE_KEY_PREFIX =
      "hdfs.raid.erasure.code.";

  /**
   * Used by ErasureCode.init() to get Code specific extra parameters.
   */
  public final JSONObject json;

  /**
   * id of the codec. Used by policy in raid.xml
   */
  public final String id;

  /**
   * Number of blocks in one stripe
   */
  public final int stripeLength;

  /**
   * Number of parity blocks of the codec for one stripe
   */
  public final int parityLength;

  /**
   * The full class name of the ErasureCode used
   */
  public final String erasureCodeClass;

  /**
   * Human readable description of the codec
   */
  public final String description;

  /**
   * Where to store the parity files
   */
  public final String parityDirectory;

  /**
   * Priority of the codec.
   * <p/>
   * Purge parity files:
   * When parity files of two Codecs exists, the parity files of the lower
   * priority codec will be purged.
   * <p/>
   * Generating parity files:
   * When a source files are under two policies, the policy with a higher
   * codec priority will be triggered.
   */
  public final int priority;

  private static List<Codec> codecs;
  private static Map<String, Codec> idToCodec;

  /**
   * Get single instantce of the list of codecs ordered by priority.
   */
  public static List<Codec> getCodecs() {
    return Codec.codecs;
  }

  /**
   * Get the instance of the codec by id
   */
  public static Codec getCodec(String id) {
    return idToCodec.get(id);
  }

  static {
    try {
      Configuration.addDefaultResource("hdfs-default.xml");
      Configuration.addDefaultResource("hdfs-site.xml");
      Configuration.addDefaultResource("erasure-coding-default.xml");
      Configuration.addDefaultResource("erasure-coding-site.xml");
      initializeCodecs(new Configuration());
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize erasure coding codecs",
          e);
    }
  }

  public static void initializeCodecs(Configuration conf) throws IOException {
    try {
      String source = conf.get(DFSConfigKeys.ERASURE_CODING_CODECS_KEY);
      if (source == null) {
        codecs = Collections.emptyList();
        idToCodec = Collections.emptyMap();
        if (LOG.isDebugEnabled()) {
          LOG.info("No codec is specified");
        }
        return;
      }
      JSONArray jsonArray = (JSONArray) new JSONParser().parse(source);
      List<Codec> localCodecs = new ArrayList<>();
      Map<String, Codec> localIdToCodec = new HashMap<>();
      for (int i = 0; i < jsonArray.size(); ++i) {
        Codec codec = new Codec((JSONObject)jsonArray.get(i));
        localIdToCodec.put(codec.id, codec);
        localCodecs.add(codec);
      }
      Collections.sort(localCodecs, new Comparator<Codec>() {
        @Override
        public int compare(Codec c1, Codec c2) {
          // Higher priority on top
          return c2.priority - c1.priority;
        }
      });
      codecs = Collections.unmodifiableList(localCodecs);
      idToCodec = Collections.unmodifiableMap(localIdToCodec);
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  public Codec(JSONObject json) {
    this.json = json;
    this.id = (String) json.get("id");
    this.parityLength = ((Long) json.get("parity_length")).intValue();
    this.stripeLength = ((Long) json.get("stripe_length")).intValue();
    this.erasureCodeClass = (String) json.get("erasure_code");
    this.parityDirectory = (String) json.get("parity_dir");
    this.priority = ((Long) json.get("priority")).intValue();
    this.description = getJSONString(json, "description", "");
    checkDirectory(parityDirectory);
  }

  /**
   * Make sure the direcotry string has the format "/a/b/c"
   */
  private void checkDirectory(String d) {
    if (!d.startsWith(Path.SEPARATOR)) {
      throw new IllegalArgumentException("Bad directory:" + d);
    }
    if (d.endsWith(Path.SEPARATOR)) {
      throw new IllegalArgumentException("Bad directory:" + d);
    }
  }

  static private String getJSONString(JSONObject json, String key,
      String defaultResult) {
    String result = defaultResult;
    result = (String) json.get(key);
    return result;
  }

  public ErasureCode createErasureCode(Configuration conf) {
    // Create the scheduler
    Class<?> erasureCode = null;
    try {
      erasureCode = conf.getClass(ERASURE_CODE_KEY_PREFIX + this.id,
          conf.getClassByName(this.erasureCodeClass));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    ErasureCode code =
        (ErasureCode) ReflectionUtils.newInstance(erasureCode, conf);
    code.init(this);
    return code;
  }

  @Override
  public String toString() {
    if (json == null) {
      return "Test codec " + id;
    } else {
      return json.toString();
    }
  }
  
  public String getParityPrefix() {
    String prefix = this.parityDirectory;
    if (!prefix.endsWith(Path.SEPARATOR)) {
      prefix += Path.SEPARATOR;
    }
    return prefix;
  }

  public int getStripeLength() {
    return stripeLength;
  }

  public int getParityLength() {
    return parityLength;
  }

  public String getId() {
    return id;
  }
}
