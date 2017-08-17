package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.server.protocol.BlockReportBlock;

public class BlockReportBucket {
  
  private BlockReportBlock[] blocks;
  
  public BlockReportBucket(){}
  
  public BlockReportBucket(BlockReportBlock[] blocks){
    this.blocks = blocks;
  }
  public void setBlocks(BlockReportBlock[] blocks) {
    this.blocks = blocks;
  }
  
  public BlockReportBlock[] getBlocks() {
    return blocks;
  }
}

