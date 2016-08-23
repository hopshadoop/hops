package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

/**
 * Created by antonis on 8/22/16.
 */
public class RMContainerTokenSecretManagerDist extends RMContainerTokenSecretManager {
    public RMContainerTokenSecretManagerDist(Configuration conf) {
        super(conf);
    }

    public void setCurrentMasterKey(MasterKey currentMasterKey) {
        super.writeLock.lock();

        try {
            super.currentMasterKey = new MasterKeyData(currentMasterKey,
                    createSecretKey(currentMasterKey.getBytes().array()));
        } finally {
            super.writeLock.unlock();
        }
    }

    public void setNextMasterKey(MasterKey nextMasterKey) {
        super.writeLock.lock();

        try {
            super.nextMasterKey = new MasterKeyData(nextMasterKey,
                    createSecretKey(nextMasterKey.getBytes().array()));
        } finally {
            super.writeLock.unlock();
        }
    }
}
