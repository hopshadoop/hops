package org.apache.hadoop.hdfs.snapshots;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by pushparaj.motamari on 26/05/16.
 */

    @RunWith(Suite.class)
    @Suite.SuiteClasses({
            TestFileCreationWithSnapShot.class,
            TestFileAppend.class,
            TestDelete.class,
            TestRename.class,
            TestListing.class
    })

    public class TestSuite {
        // the class remains empty,
        // used only as a holder for the above annotations
    }

