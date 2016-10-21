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
package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

/**
 * Erasure decoding step, a wrapper of all the necessary information to perform
 * a decoding step involved in the whole process of decoding a block group.
 */
@InterfaceAudience.Private
public class ErasureDecodingStep extends AbstractErasureCodingStep {
  private int[] erasedIndexes;
  private RawErasureDecoder rawDecoder;

  /**
   * The constructor with all the necessary info.
   * @param inputBlocks
   * @param erasedIndexes the indexes of erased blocks in inputBlocks array
   * @param outputBlocks
   * @param rawDecoder
   */
  public ErasureDecodingStep(ECBlock[] inputBlocks, int[] erasedIndexes,
                             ECBlock[] outputBlocks,
                             RawErasureDecoder rawDecoder) {
    super(inputBlocks, outputBlocks);
    this.erasedIndexes = erasedIndexes;
    this.rawDecoder = rawDecoder;
  }

  @Override
  public void performCoding(ECChunk[] inputChunks, ECChunk[] outputChunks) {
    rawDecoder.decode(inputChunks, erasedIndexes, outputChunks);
  }

}
