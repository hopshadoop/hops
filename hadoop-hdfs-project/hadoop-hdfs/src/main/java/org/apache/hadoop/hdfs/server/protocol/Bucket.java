package org.apache.hadoop.hdfs.server.protocol;

public class Bucket {
  
  private ReportedBlock[] blocks;

  private byte[] hash;
  
  public Bucket(){}
  
  public Bucket(ReportedBlock[] blocks){
    this.blocks = blocks;
  }

  public void setBlocks(ReportedBlock[] blocks) {
    this.blocks = blocks;
  }
  
  public ReportedBlock[] getBlocks() {
    return blocks;
  }

  public void setHash(byte[] hash){
    this.hash = hash;
  }

  public byte[] getHash(){
    return hash;
  }
}

