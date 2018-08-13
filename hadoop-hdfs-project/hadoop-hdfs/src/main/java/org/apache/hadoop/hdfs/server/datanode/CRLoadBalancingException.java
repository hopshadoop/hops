package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

/**
 * Created by salman on 12/7/15.
 */
public class CRLoadBalancingException extends IOException {
    public CRLoadBalancingException(String msg){
        super(msg);
    }
}
