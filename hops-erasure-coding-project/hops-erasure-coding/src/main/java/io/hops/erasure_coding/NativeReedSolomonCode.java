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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;

public class NativeReedSolomonCode extends ErasureCode {
  public static final Log LOG = LogFactory.getLog(NativeReedSolomonCode.class);

  private int stripeSize;
  private int paritySize;
  
  NativeRSRawErasureCoderFactory factory;
  NativeRSRawEncoder encoder;
  NativeRSRawDecoder decoder;
  
   
    public NativeReedSolomonCode() {
    }
    
    private void init(int stripeSize, int paritySize) {
        this.stripeSize = stripeSize;
        this.paritySize = paritySize;
        factory = new NativeRSRawErasureCoderFactory();
        encoder = (NativeRSRawEncoder) factory.createEncoder(new ErasureCoderOptions(this.stripeSize, this.paritySize));
        decoder = (NativeRSRawDecoder) factory.createDecoder(new ErasureCoderOptions(this.stripeSize, this.paritySize));
    }
    
    @Override
    public void encodeBulk(byte[][] inputs, byte[][] outputs) throws IOException {
        
        ByteBuffer[] binputs = new ByteBuffer[inputs.length];
        ByteBuffer[] boutputs = new ByteBuffer[outputs.length];
        
        int size = inputs[0].length;
        int[] inputOffsets = new int[inputs.length];
        int[] outputOffsets = new int[outputs.length];
        
        for(int i=0; i<binputs.length; i++){
            binputs[i] = ByteBuffer.allocateDirect(size);
            for(int j=0; j<size; j++){
                binputs[i].put(inputs[i][j]);
            }
            binputs[i].flip();
        }
        
        for(int i=0; i<boutputs.length; i++){
            boutputs[i] = ByteBuffer.allocateDirect(size);
        }
        
        encoder.performEncodeImpl(binputs, inputOffsets, size, boutputs, outputOffsets);
        
        for(int i=0; i<boutputs.length; i++){
            for(int j=0; j<size; j++){
                try{
                    byte b = boutputs[i].get();
                    outputs[i][j] = b;
                       
                }
                catch(Exception e){
                   System.out.println(e);
                }
            }
        }
    }
    
    @Override
    public void decodeBulk(byte[][] readBufs, byte[][] writeBufs,
    int[] erasedLocations, int[] locationsToRead, int[] locationsNotToRead) throws IOException {
        
        ByteBuffer[] breadBufs = new ByteBuffer[readBufs.length];
        ByteBuffer[] bwriteBufs = new ByteBuffer[locationsNotToRead.length];
        
        int size = readBufs[0].length;
        
        int[] inputOffsets = new int[readBufs.length];
        int[] outputOffsets = new int[locationsNotToRead.length];
        
        //init parities
        for(int i=0; i<paritySize; i++){
            breadBufs[i+stripeSize] = ByteBuffer.allocateDirect(size);
            for(int j=0; j<size; j++){
                breadBufs[i+stripeSize].put(readBufs[i][j]);
            }
            breadBufs[i+stripeSize].flip();
        }
        
        //init data
        for(int i=0; i<stripeSize; i++){
            breadBufs[i] = ByteBuffer.allocateDirect(size);
            for(int j=0; j<size; j++){
                breadBufs[i].put(readBufs[i+paritySize][j]);
            }
            breadBufs[i].flip();
        }
        
        int[] modifiedLocationsNotRead = new int[locationsNotToRead.length];
        
        for(int i=0; i<locationsNotToRead.length; i++){
            int location = locationsNotToRead[i];
            if (location < paritySize){
                //then this is parity
                breadBufs[location+stripeSize] = null; 
                modifiedLocationsNotRead[i] = location + stripeSize;
            }
            else{
                //then this is data
                breadBufs[location-paritySize] = null;
                modifiedLocationsNotRead[i] = location-paritySize;
            }
        }
        
        Arrays.sort(modifiedLocationsNotRead);
        
        //allocate writebufs
        for(int i=0; i < modifiedLocationsNotRead.length; i++){
            bwriteBufs[i] = ByteBuffer.allocateDirect(size);
        }
        
        decoder.performDecodeImpl(breadBufs, inputOffsets, size, modifiedLocationsNotRead, bwriteBufs, outputOffsets);
        
        for(int i=0; i<writeBufs.length; i++){
            for(int j=0; j<size; j++){
                writeBufs[i][j] = bwriteBufs[i].get();
            }
        }
    }
    
    @Override
    public void encode(int[] message, int[] parity) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void decode(int[] data, int[] erasedLocations, int[] erasedValues) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void decode(int[] data, int[] erasedLocations, int[] erasedValues, int[] locationsToRead, int[] locationsNotToRead) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void init(Codec codec) {
    init(codec.stripeLength, codec.parityLength);
    LOG.info("Initialized " + ReedSolomonCode.class +
        " stripeLength:" + codec.stripeLength +
        " parityLength:" + codec.parityLength);
    }
    
    @Override
    public int stripeSize() {
        return stripeSize;
    }

    @Override
    public int paritySize() {
        return paritySize;
    }

    @Override
    public int symbolSize() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public void releaseEncoder(){
        this.encoder.release();
    }
     
    public void releaseDecoder(){
        this.decoder.release();
    }
}
