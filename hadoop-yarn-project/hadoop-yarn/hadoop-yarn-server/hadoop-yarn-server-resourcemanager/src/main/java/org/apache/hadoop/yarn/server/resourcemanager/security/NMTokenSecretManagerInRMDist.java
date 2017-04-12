package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

/**
 * Created by antonis on 8/22/16.
 */
public class NMTokenSecretManagerInRMDist extends NMTokenSecretManagerInRM {
    public NMTokenSecretManagerInRMDist(Configuration conf) {
        super(conf);
    }

    public void setCurrentMasterKey(MasterKey currentMasterKey) {
        super.writeLock.lock();
        try {
            if (currentMasterKey != super.currentMasterKey.getMasterKey()) {
                super.currentMasterKey = new MasterKeyData(currentMasterKey,
                        createSecretKey(currentMasterKey.getBytes().array()));
                clearApplicationNMTokenKeys();
            }
        } finally {
            super.writeLock.unlock();
        }
    }

    public void setNextMasterKey(MasterKey nextMasterKey) {
        super.writeLock.lock();
        try {
            if (nextMasterKey != super.nextMasterKey.getMasterKey()) {
                this.nextMasterKey = new MasterKeyData(nextMasterKey,
                        createSecretKey(nextMasterKey.getBytes().array()));
            }
        } finally {
            super.writeLock.unlock();
        }
    }
}
