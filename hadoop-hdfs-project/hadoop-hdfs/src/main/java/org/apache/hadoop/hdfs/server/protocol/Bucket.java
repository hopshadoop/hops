package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;

public class Bucket {
  
  private BlockListAsLongs blocks;

  private byte[] hash;
  
  public Bucket(){}
  
  public Bucket(BlockListAsLongs blocks){
    this.blocks = blocks;
  }

  public void setBlocks(BlockListAsLongs blocks) {
    this.blocks = blocks;
  }
  
  public BlockListAsLongs getBlocks() {
    return blocks;
  }

  public void setHash(byte[] hash){
    this.hash = hash;
  }

  public byte[] getHash(){
    return hash;
  }
}

