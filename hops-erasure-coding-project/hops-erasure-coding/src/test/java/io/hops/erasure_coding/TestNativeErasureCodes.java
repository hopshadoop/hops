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

import static io.hops.erasure_coding.TestGaloisField.GF;
import java.io.IOException;
import java.nio.ByteBuffer;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.junit.Test;

public class TestNativeErasureCodes extends TestCase {
  final int TEST_CODES = 100;
  final int TEST_TIMES = 1000;
  final Random RAND = new Random();

   @Test
  public void testEC(){
         ErasureCodeNative.checkNativeCodeLoaded();
  }
  
  public void setZero(byte[] b){
      for(int i=0;i<b.length;i++){
          b[i] = 0x00;
      }
  }
  public void testEncodeDecode() throws IOException {
    long overallEncode = 0L;
    long overallDecode = 0L;
    
    for (int n = 0; n < TEST_CODES; n++) {
      int stripeSize = 10;//RAND.nextInt(99) + 1; // 1, 2, 3, ... 100
      int paritySize = 4;//RAND.nextInt(9) + 1; //1, 2, 3, 4, ... 10
      ErasureCode ec = new ReedSolomonCode(stripeSize, paritySize);
      for (int m = 0; m < TEST_TIMES; m++) {
          
        byte[][] inputs = new byte[stripeSize][1024];
        byte[][] outputs = new byte[paritySize][1024];
        
        byte[][] outputData = new byte[stripeSize+paritySize][1024];
        byte[][] outputCopy = new byte[stripeSize+paritySize][1024];
        
        for(int i=0;i<stripeSize;i++){
            byte[] b = new byte[1024];
            RAND.nextBytes(b);
            inputs[i] = b;
            outputData[i+paritySize] = Arrays.copyOf(b, b.length);
            outputCopy[i+paritySize] = Arrays.copyOf(b, b.length);
        }
        
        long startTime = System.currentTimeMillis();
        ec.encodeBulk(inputs, outputs);
        long stopTime = System.currentTimeMillis();
        overallEncode += (stopTime - startTime);
       
        for(int i=0;i<paritySize;i++){
           outputData[i] = Arrays.copyOf(outputs[i],outputs.length);
           outputCopy[i] = Arrays.copyOf(outputs[i],outputs.length);
        }


        
        int erasedLen = 4;//paritySize == 1 ? 1 : RAND.nextInt(paritySize - 1) + 1;
        int[] erasedLocations = randomErasedLocation(erasedLen, inputs.length);
        for (int i = 0; i < erasedLocations.length; i++) {
            erasedLocations[i] += paritySize;
            setZero(outputData[erasedLocations[i]]);
        }
        
        int[] erasedValues = new int[erasedLen];
        byte[][] writeBufs = new byte[erasedLen][1024];
        
        
        startTime = System.currentTimeMillis();
        ((ReedSolomonCode)ec).decodeBulk(outputData, writeBufs, erasedLocations);
        stopTime = System.currentTimeMillis();
        overallDecode += (stopTime - startTime);
                       
      }
    }
    
     System.out.println("Encoding " + (overallEncode) + " milliseconds");
     System.out.println("Decoding " + (overallDecode) + " milliseconds");
  }
  
  @Test
  public void testNativeEncodeDecode() throws IOException {
      
    long overallEncode = 0L;
    long overallDecode = 0L;
      
      
    for (int n = 0; n < TEST_CODES; n++) {
      int stripeSize = 10;//RAND.nextInt(99) + 1; // 1, 2, 3, ... 100
      int paritySize = 4;//RAND.nextInt(9) + 1; //1, 2, 3, 4, ... 10
      NativeRSRawErasureCoderFactory factory = new NativeRSRawErasureCoderFactory();
      NativeRSRawEncoder enc = (NativeRSRawEncoder) factory.createEncoder(new ErasureCoderOptions(stripeSize, paritySize));
      NativeRSRawDecoder dec = (NativeRSRawDecoder) factory.createDecoder(new ErasureCoderOptions(stripeSize, paritySize));
      
      for (int m = 0; m < TEST_TIMES; m++) {
      
          
        int symbolMax = (int) Math.pow(2, (int) Math.round(Math.log(GF.getFieldSize()) / Math.log(2)));
        
        
        int[] message = new int[stripeSize];
        for (int i = 0; i < stripeSize; i++) {
          message[i] = RAND.nextInt(symbolMax) + 2;
        }
        
        
        int[] parity = new int[paritySize];
        
        /* Native Encode Starts */
        ByteBuffer[] encodeData = new ByteBuffer[message.length];
        ByteBuffer[] parityData = new ByteBuffer[parity.length];
        
        int[] inputOffsets = new int[encodeData.length];
        int[] outputOffsets = new int[parityData.length];
        
        for(int i=0; i<message.length;i++){
            encodeData[i] = ByteBuffer.allocateDirect(1024);
            encodeData[i].putInt(message[i]);
            for(int j=0; j<255;j++){
                encodeData[i].putInt(RAND.nextInt(symbolMax) + 2);
            }
            encodeData[i].flip();
        }
        
        for(int i=0; i<parity.length; i++){
            parityData[i] = ByteBuffer.allocateDirect(1024);
        }
        
        long startTime = System.currentTimeMillis();
        enc.performEncodeImpl(encodeData, inputOffsets, 1024, parityData, outputOffsets);
        long stopTime = System.currentTimeMillis();
        overallEncode += (stopTime - startTime);
        
        
        /* Native Encode Ends here*/
        
        
        /* Native Decode Starts here*/
        int[] data = new int[stripeSize + paritySize];
        int[] copy = new int[data.length];    
         for (int i = 0; i < stripeSize; i++) {
          data[i] = message[i];
          copy[i] = message[i];
        }
        for (int i = 0; i < paritySize; i++) {
          data[i+stripeSize] = parity[i];
          copy[i+stripeSize] = parity[i];
        }
        int erasedLen = 4;//paritySize == 1 ? 1 : RAND.nextInt(paritySize - 1) + 1;
        int[] erasedLocations = randomErasedLocation(erasedLen, message.length);
        for (int i = 0; i < erasedLocations.length; i++) {
          data[erasedLocations[i]] = 0;
        }
        int[] erasedValues = new int[erasedLen];
      
        //Native Decode
        ByteBuffer[] decodeData = new ByteBuffer[stripeSize+paritySize];
        ByteBuffer[] recoverData = new ByteBuffer[erasedValues.length];
        
        inputOffsets = new int[decodeData.length];
        outputOffsets = new int[recoverData.length];
        
        for(int i=0; i<stripeSize;i++){
            if(data[i] == 0){
                decodeData[i] = null;
                continue;
            }
            decodeData[i] = encodeData[i];
            decodeData[i].flip();
        }
        for(int i = stripeSize; i < stripeSize + paritySize; i++){
            decodeData[i] = parityData[i-stripeSize];
            decodeData[i].flip();
        }
        for(int i=0; i<erasedValues.length;i++){
            recoverData[i] = ByteBuffer.allocateDirect(1024);
        }
        
        startTime = System.currentTimeMillis();
        dec.performDecodeImpl(decodeData, inputOffsets, 1024, erasedLocations, recoverData, outputOffsets);       
        stopTime = System.currentTimeMillis();
        overallDecode += (stopTime - startTime);
        
        for(int i=0; i<recoverData.length;i++){
            erasedValues[i] = recoverData[i].getInt();
        }
        
        /* Native Decode Ends here */
        
        for (int i = 0; i < erasedLen; i++) {
            
            StringBuffer sb = new StringBuffer();
            sb.append("\nC ");
            for(int j=0; j< data.length; j++){
                  sb.append(" " + copy[j]);
            }
            
            sb.append("\nD ");
            
            for(int j=0; j< data.length; j++){
                  sb.append(" " + data[j]);
            }
            
            assertEquals("Decode failed " + sb , copy[erasedLocations[i]],erasedValues[i]);
            
        }
      }
      enc.release();
      dec.release();
      
    }
    
    System.out.println("Encoding " + (overallEncode) + " milliseconds");
    System.out.println("Decoding " + (overallDecode) + " milliseconds");
  }

  public void testRSPerformance() {
    int stripeSize = 10;
    int paritySize = 4;
    ErasureCode ec = new ReedSolomonCode(stripeSize, paritySize);
    int symbolMax = (int) Math.pow(2, ec.symbolSize());
    byte[][] message = new byte[stripeSize][];
    int bufsize = 1024 * 1024 * 10;
    for (int i = 0; i < stripeSize; i++) {
      message[i] = new byte[bufsize];
      for (int j = 0; j < bufsize; j++) {
        message[i][j] = (byte) RAND.nextInt(symbolMax);
      }
    }
    byte[][] parity = new byte[paritySize][];
    for (int i = 0; i < paritySize; i++) {
      parity[i] = new byte[bufsize];
    }
    long encodeStart = System.currentTimeMillis();
    int[] tmpIn = new int[stripeSize];
    int[] tmpOut = new int[paritySize];
    for (int i = 0; i < bufsize; i++) {
      // Copy message.
      for (int j = 0; j < stripeSize; j++) {
        tmpIn[j] = 0x000000FF & message[j][i];
      }
      ec.encode(tmpIn, tmpOut);
      // Copy parity.
      for (int j = 0; j < paritySize; j++) {
        parity[j][i] = (byte) tmpOut[j];
      }
    }
    long encodeEnd = System.currentTimeMillis();
    float encodeMSecs = (encodeEnd - encodeStart);
    System.out.println("Time to encode rs = " + encodeMSecs +
        "msec (" + message[0].length / (1000 * encodeMSecs) + " MB/s)");

    // Copy erased array.
    int[] data = new int[paritySize + stripeSize];
    // 4th location is the 0th symbol in the message
    int[] erasedLocations = new int[]{4, 1, 5, 7};
    int[] erasedValues = new int[erasedLocations.length];
    byte[] copy = new byte[bufsize];
    for (int j = 0; j < bufsize; j++) {
      copy[j] = message[0][j];
      message[0][j] = 0;
    }

    long decodeStart = System.currentTimeMillis();
    for (int i = 0; i < bufsize; i++) {
      // Copy parity first.
      for (int j = 0; j < paritySize; j++) {
        data[j] = 0x000000FF & parity[j][i];
      }
      // Copy message. Skip 0 as the erased symbol
      for (int j = 1; j < stripeSize; j++) {
        data[j + paritySize] = 0x000000FF & message[j][i];
      }
      // Use 0, 2, 3, 6, 8, 9, 10, 11, 12, 13th symbol to reconstruct the data
      ec.decode(data, erasedLocations, erasedValues);
      message[0][i] = (byte) erasedValues[0];
    }
    long decodeEnd = System.currentTimeMillis();
    float decodeMSecs = (decodeEnd - decodeStart);
    System.out.println("Time to decode = " + decodeMSecs +
        "msec (" + message[0].length / (1000 * decodeMSecs) + " MB/s)");
    assertTrue("Decode failed", Arrays.equals(copy, message[0]));
  }

  public void testRSEncodeDecodeBulk() {
    // verify the production size.
    verifyRSEncodeDecodeBulk(10, 4);

    // verify a test size
    verifyRSEncodeDecodeBulk(3, 3);
  }

  public void verifyRSEncodeDecodeBulk(int stripeSize, int paritySize) {
    ReedSolomonCode rsCode = new ReedSolomonCode(stripeSize, paritySize);
    int symbolMax = (int) Math.pow(2, rsCode.symbolSize());
    byte[][] message = new byte[stripeSize][];
    byte[][] cpMessage = new byte[stripeSize][];
    int bufsize = 1024 * 1024 * 10;
    for (int i = 0; i < stripeSize; i++) {
      message[i] = new byte[bufsize];
      cpMessage[i] = new byte[bufsize];
      for (int j = 0; j < bufsize; j++) {
        message[i][j] = (byte) RAND.nextInt(symbolMax);
        cpMessage[i][j] = message[i][j];
      }
    }
    byte[][] parity = new byte[paritySize][];
    for (int i = 0; i < paritySize; i++) {
      parity[i] = new byte[bufsize];
    }

    // encode.
    rsCode.encodeBulk(cpMessage, parity);

    int erasedLocation = RAND.nextInt(stripeSize);
    byte[] copy = new byte[bufsize];
    for (int i = 0; i < bufsize; i++) {
      copy[i] = message[erasedLocation][i];
      message[erasedLocation][i] = (byte) 0;
    }

    // test decode
    byte[][] data = new byte[stripeSize + paritySize][];
    for (int i = 0; i < paritySize; i++) {
      data[i] = new byte[bufsize];
      for (int j = 0; j < bufsize; j++) {
        data[i][j] = parity[i][j];
      }
    }

    for (int i = 0; i < stripeSize; i++) {
      data[i + paritySize] = new byte[bufsize];
      for (int j = 0; j < bufsize; j++) {
        data[i + paritySize][j] = message[i][j];
      }
    }
    byte[][] writeBufs = new byte[1][];
    writeBufs[0] = new byte[bufsize];
    rsCode.decodeBulk(data, writeBufs, new int[]{erasedLocation + paritySize});
    assertTrue("Decode failed", Arrays.equals(copy, writeBufs[0]));
  }

  public void testXorPerformance() {
    Random RAND = new Random();
    int stripeSize = 10;
    byte[][] message = new byte[stripeSize][];
    int bufsize = 1024 * 1024 * 10;
    for (int i = 0; i < stripeSize; i++) {
      message[i] = new byte[bufsize];
      for (int j = 0; j < bufsize; j++) {
        message[i][j] = (byte) RAND.nextInt(256);
      }
    }
    byte[] parity = new byte[bufsize];

    long encodeStart = System.currentTimeMillis();
    for (int i = 0; i < bufsize; i++) {
      for (int j = 0; j < stripeSize; j++) {
        parity[i] ^= message[j][i];
      }
    }
    long encodeEnd = System.currentTimeMillis();
    float encodeMSecs = encodeEnd - encodeStart;
    System.out.println("Time to encode xor = " + encodeMSecs +
        " msec (" + message[0].length / (1000 * encodeMSecs) + "MB/s)");

    byte[] copy = new byte[bufsize];
    for (int j = 0; j < bufsize; j++) {
      copy[j] = message[0][j];
      message[0][j] = 0;
    }

    long decodeStart = System.currentTimeMillis();
    for (int i = 0; i < bufsize; i++) {
      for (int j = 1; j < stripeSize; j++) {
        message[0][i] ^= message[j][i];
      }
      message[0][i] ^= parity[i];
    }
    long decodeEnd = System.currentTimeMillis();
    float decodeMSecs = decodeEnd - decodeStart;
    System.out.println("Time to decode xor = " + decodeMSecs +
        " msec (" + message[0].length / (1000 * decodeMSecs) + "MB/s)");
    assertTrue("Decode failed", Arrays.equals(copy, message[0]));
  }


  public void testComputeErrorLocations() {
    for (int i = 0; i < TEST_TIMES; ++i) {
      verifyErrorLocations(10, 4, 1);
      verifyErrorLocations(10, 4, 2);
    }
  }

  public void verifyErrorLocations(int stripeSize, int paritySize, int errors) {
    int[] message = new int[stripeSize];
    int[] parity = new int[paritySize];
    Set<Integer> errorLocations = new HashSet<Integer>();
    for (int i = 0; i < message.length; ++i) {
      message[i] = RAND.nextInt(256);
    }
    while (errorLocations.size() < errors) {
      int loc = RAND.nextInt(stripeSize + paritySize);
      errorLocations.add(loc);
    }
    ReedSolomonCode codec = new ReedSolomonCode(stripeSize, paritySize);
    codec.encode(message, parity);
    int[] data = combineArrays(parity, message);
    for (Integer i : errorLocations) {
      data[i] = randError(data[i]);
    }
    Set<Integer> recoveredLocations = new HashSet<Integer>();
    boolean resolved = codec.computeErrorLocations(data, recoveredLocations);
    if (resolved) {
      assertEquals(errorLocations, recoveredLocations);
    }
  }

  private int randError(int actual) {
    while (true) {
      int r = RAND.nextInt(256);
      if (r != actual) {
        return r;
      }
    }
  }

  private int[] combineArrays(int[] array1, int[] array2) {
    int[] result = new int[array1.length + array2.length];
    for (int i = 0; i < array1.length; ++i) {
      result[i] = array1[i];
    }
    for (int i = 0; i < array2.length; ++i) {
      result[i + array1.length] = array2[i];
    }
    return result;
  }

  private int[] randomErasedLocation(int erasedLen, int dataLen) {
    int[] erasedLocations = new int[erasedLen];
    for (int i = 0; i < erasedLen; i++) {
      Set<Integer> s = new HashSet<Integer>();
      while (s.size() != erasedLen) {
        s.add(RAND.nextInt(dataLen));
      }
      int t = 0;
      for (int erased : s) {
        erasedLocations[t++] = erased;
      }
    }
    return erasedLocations;
  }
}
