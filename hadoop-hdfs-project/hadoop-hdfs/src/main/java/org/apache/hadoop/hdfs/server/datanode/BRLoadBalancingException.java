package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

/**
 * Created by salman on 12/7/15.
 */
public class BRLoadBalancingException extends IOException {
    public BRLoadBalancingException(String msg){
        super(msg);
    }
}
