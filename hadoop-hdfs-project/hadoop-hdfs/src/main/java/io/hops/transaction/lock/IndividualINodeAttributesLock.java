package io.hops.transaction.lock;

import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by pushparaj.motamari on 01/02/16.
 */
public class IndividualINodeAttributesLock extends Lock {

    private TransactionLockTypes.LockType lockType;
    private INodeIdentifier inodeIdentifier;

    IndividualINodeAttributesLock(TransactionLockTypes.LockType lockType,
                                  INodeIdentifier inodeIdentifier) {

        this.lockType = lockType;
        this.inodeIdentifier = inodeIdentifier;
    }
    @Override
    protected void acquire(TransactionLocks locks) throws IOException {
        ArrayList<INodeCandidatePrimaryKey> iNodeCandidatePrimaryKeys = new ArrayList<INodeCandidatePrimaryKey>();
        iNodeCandidatePrimaryKeys.add(new INodeCandidatePrimaryKey(inodeIdentifier.getInodeId()));
        acquireLockList(DEFAULT_LOCK_TYPE, INodeAttributes.Finder.ByINodeIds,iNodeCandidatePrimaryKeys );
    }

    @Override
    protected Type getType() {
        return Type.INodeAttribute;
    }
}
