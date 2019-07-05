package org.apache.hadoop.fs;

public enum CloudProvider {
  AWS("AWS");

  private String name;

  CloudProvider(String name) { name = name;}
  //GCE
}
