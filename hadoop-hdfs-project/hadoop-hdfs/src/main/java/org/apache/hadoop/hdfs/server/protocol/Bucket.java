package org.apache.hadoop.hdfs.server.protocol;

public class Bucket {
  
  private ReportedBlock[] blocks;
  
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
}

