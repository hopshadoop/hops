package org.apache.hadoop.hdfs.snapshots;

import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

/**
 * Created by pushparaj.motamari on 26/05/16.
 */
public class TestListing extends BaseTest {

    @Test
    public void withOutSnapshotTest() throws IOException, SQLException{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        createDirectory("/d2");
        createDirectory("/d2/d1");
        DirectoryListing directoryListing = cluster.getNameNodeRpc()
                .getListing("/", HdfsFileStatus.EMPTY_NAME, false);
    }
    @Test
    public void addFileAfterSnapshot()throws IOException, SQLException{
        createDirectory("/d1");
        takeSnapshotOnRoot();
        createAndWriteToFile("/d1/f2");
        DirectoryListing directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);

        assertTrue(directoryListing.getPartialListing().length==1);
        assertTrue(directoryListing.getPartialListing()[0].getLocalName().equalsIgnoreCase("f2"));
        dfs.rollBack("Admin");
        directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==0);

    }

    @Test
    public void addDirectoryAfterSnapshot() throws IOException, SQLException{
        createDirectory("/d1");
        takeSnapshotOnRoot();
        createDirectory("/d1/d2");
        DirectoryListing directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);

        assertTrue(directoryListing.getPartialListing().length==1);
        assertTrue(directoryListing.getPartialListing()[0].getLocalName().equalsIgnoreCase("d2"));
        dfs.rollBack("Admin");
        directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==0);
    }

    @Test
    public void deleteFileAfterSnapshot()throws IOException, SQLException{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        takeSnapshotOnRoot();
        dfs.delete(new Path("/d1/f2"), true);

        DirectoryListing directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==0);

        dfs.rollBack("Admin");

        directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==1);
        assertTrue(directoryListing.getPartialListing()[0].getLocalName().equalsIgnoreCase("f2"));

    }

    @Test
    public void deleteDirectoryAfterSnapshot()throws IOException, SQLException{
        createDirectory("/d1");
        createDirectory("/d1/d2");
        takeSnapshotOnRoot();
        dfs.delete(new Path("/d1/d2"), true);

        DirectoryListing directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==0);

        dfs.rollBack("Admin");

        directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==1);
        assertTrue(directoryListing.getPartialListing()[0].getLocalName().equalsIgnoreCase("d2"));
    }

    @Test
    public void renameFileAfterSnapshot()throws IOException, SQLException{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        createAndWriteToFile("/d1/f3");
        takeSnapshotOnRoot();
        dfs.rename(new Path("/d1/f2"), new Path("/d1/f3"), Options.Rename.OVERWRITE);

        DirectoryListing directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==1);
        assertTrue(directoryListing.getPartialListing()[0].getLocalName().equalsIgnoreCase("f3"));

        dfs.rollBack("Admin");

        directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==2);
        assertTrue(directoryListing.getPartialListing()[0].getLocalName().equalsIgnoreCase("f2"));
        assertTrue(directoryListing.getPartialListing()[1].getLocalName().equalsIgnoreCase("f3"));

    }

    @Test
    public void renameDirectoryAfterSnapshot()throws IOException, SQLException{
        createDirectory("/d1");
        createDirectory("/d1/d2");
        createDirectory("/d1/d3");
        takeSnapshotOnRoot();
        dfs.rename(new Path("/d1/d2"), new Path("/d1/d3"), Options.Rename.OVERWRITE);

        DirectoryListing directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==1);
        assertTrue(directoryListing.getPartialListing()[0].getLocalName().equalsIgnoreCase("d3"));

        dfs.rollBack("Admin");

        directoryListing = cluster.getNameNodeRpc()
                .getListing("/d1", HdfsFileStatus.EMPTY_NAME, false);
        assertTrue(directoryListing.getPartialListing().length==2);
        assertTrue(directoryListing.getPartialListing()[0].getLocalName().equalsIgnoreCase("d2"));
        assertTrue(directoryListing.getPartialListing()[1].getLocalName().equalsIgnoreCase("d3"));
    }
}
