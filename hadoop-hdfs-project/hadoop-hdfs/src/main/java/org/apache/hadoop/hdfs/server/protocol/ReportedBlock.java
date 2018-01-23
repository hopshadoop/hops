package org.apache.hadoop.hdfs.server.protocol;


public class ReportedBlock {
  private final long blockId;
  private final long generationStamp;
  private final long length;
  private final BlockReportBlockState state;
  
  public ReportedBlock(long blockId, long generationStamp, long length,
                       BlockReportBlockState state) {
    this.blockId = blockId;
    this.generationStamp = generationStamp;
    this.length = length;
    this.state = state;
  }
  
  public long getBlockId() {
    return blockId;
  }
  
  public long getGenerationStamp() {
    return generationStamp;
  }
  
  public long getLength() {
    return length;
  }
  
  public BlockReportBlockState getState() {return state;}
}
