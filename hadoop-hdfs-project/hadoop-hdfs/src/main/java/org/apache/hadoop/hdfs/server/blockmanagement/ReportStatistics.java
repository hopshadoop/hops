package org.apache.hadoop.hdfs.server.blockmanagement;

public class ReportStatistics{
  private int numBuckets;
  private int numBucketsMatching;
  private int numBlocks;
  private int numToRemove;
  private int numToInvalidate;
  private int numToCorrupt;
  private int numToUC;
  private int numToAdd;
  int numConsideredSafeIfInSafemode;

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public int getNumBucketsMatching() {
    return numBucketsMatching;
  }

  public void setNumBucketsMatching(int numBucketsMatching) {
    this.numBucketsMatching = numBucketsMatching;
  }

  public int getNumBlocks() {
    return numBlocks;
  }

  public void setNumBlocks(int numBlocks) {
    this.numBlocks = numBlocks;
  }

  public int getNumToRemove() {
    return numToRemove;
  }

  public void setNumToRemove(int numToRemove) {
    this.numToRemove = numToRemove;
  }

  public int getNumToInvalidate() {
    return numToInvalidate;
  }

  public void setNumToInvalidate(int numToInvalidate) {
    this.numToInvalidate = numToInvalidate;
  }

  public int getNumToCorrupt() {
    return numToCorrupt;
  }

  public void setNumToCorrupt(int numToCorrupt) {
    this.numToCorrupt = numToCorrupt;
  }

  public int getNumToUC() {
    return numToUC;
  }

  public void setNumToUC(int numToUC) {
    this.numToUC = numToUC;
  }

  public int getNumToAdd() {
    return numToAdd;
  }

  public void setNumToAdd(int numToAdd) {
    this.numToAdd = numToAdd;
  }

  public int getNumConsideredSafeIfInSafemode() {
    return numConsideredSafeIfInSafemode;
  }

  public void setNumConsideredSafeIfInSafemode(int numConsideredSafeIfInSafemode) {
    this.numConsideredSafeIfInSafemode = numConsideredSafeIfInSafemode;
  }

  @Override
  public String toString() {
    return "ReportStatistics{" +
            "numBuckets=" + numBuckets +
            ", numBlocks=" + numBlocks +
            ", numBucketsMatching=" + numBucketsMatching +
            ", numToRemove=" + numToRemove +
            ", numToInvalidate=" + numToInvalidate +
            ", numToCorrupt=" + numToCorrupt +
            ", numToUC=" + numToUC +
            ", numToAdd=" + numToAdd +
            ", numConsideredSafeIfInSafemode=" + numConsideredSafeIfInSafemode +
            '}';
  }
}

