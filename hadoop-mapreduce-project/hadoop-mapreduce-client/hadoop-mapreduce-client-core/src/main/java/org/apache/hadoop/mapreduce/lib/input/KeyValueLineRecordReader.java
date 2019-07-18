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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class treats a line in the input as a key/value pair separated by a 
 * separator character. The separator can be specified in config file 
 * under the attribute name mapreduce.input.keyvaluelinerecordreader.key.value.separator. The default
 * separator is the tab character ('\t').
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyValueLineRecordReader extends RecordReader<Text, Text> {
  public static final String KEY_VALUE_SEPARATOR =
      "mapreduce.input.keyvaluelinerecordreader.key.value.separator";
  /**
   * @deprecated Use {@link #KEY_VALUE_SEPARATOR}
   */
  @Deprecated
  public static final String KEY_VALUE_SEPERATOR = KEY_VALUE_SEPARATOR;

  private final LineRecordReader lineRecordReader;

  private byte separator = (byte) '\t';

  private Text innerValue;

  private Text key;
  
  private Text value;
  
  public Class getKeyClass() { return Text.class; }
  
  public KeyValueLineRecordReader(Configuration conf)
    throws IOException {
    
    lineRecordReader = new LineRecordReader();
    String sepStr = conf.get(KEY_VALUE_SEPARATOR, "\t");
    this.separator = (byte) sepStr.charAt(0);
  }

  public void initialize(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {
    lineRecordReader.initialize(genericSplit, context);
  }
  
  public static int findSeparator(byte[] utf, int start, int length, 
      byte sep) {
    for (int i = start; i < (start + length); i++) {
      if (utf[i] == sep) {
        return i;
      }
    }
    return -1;
  }

  public static void setKeyValue(Text key, Text value, byte[] line,
      int lineLen, int pos) {
    if (pos == -1) {
      key.set(line, 0, lineLen);
      value.set("");
    } else {
      key.set(line, 0, pos);
      value.set(line, pos + 1, lineLen - pos - 1);
    }
  }
  /** Read key/value pair in a line. */
  public synchronized boolean nextKeyValue()
    throws IOException {
    byte[] line = null;
    int lineLen = -1;
    if (lineRecordReader.nextKeyValue()) {
      innerValue = lineRecordReader.getCurrentValue();
      line = innerValue.getBytes();
      lineLen = innerValue.getLength();
    } else {
      return false;
    }
    if (line == null)
      return false;
    if (key == null) {
      key = new Text();
    }
    if (value == null) {
      value = new Text();
    }
    int pos = findSeparator(line, 0, lineLen, this.separator);
    setKeyValue(key, value, line, lineLen, pos);
    return true;
  }
  
  public Text getCurrentKey() {
    return key;
  }

  public Text getCurrentValue() {
    return value;
  }

  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }
  
  public synchronized void close() throws IOException { 
    lineRecordReader.close();
  }
}
