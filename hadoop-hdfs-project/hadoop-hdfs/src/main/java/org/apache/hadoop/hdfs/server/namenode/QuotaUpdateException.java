package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

public class QuotaUpdateException extends IOException {

    public QuotaUpdateException(String message){
        super(message);
    }
}
