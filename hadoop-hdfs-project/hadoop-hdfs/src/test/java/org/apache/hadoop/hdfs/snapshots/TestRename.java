/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.snapshots;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 *This class tests various cases of renaming
 * @author pushparaj
 */
public class TestRename extends BaseTest{
    @Test
    public void testRenameWithOutSnapshot() throws Exception {
        createDirectory("/d1/d2");
        createAndWriteToFile("/d1/d2/f2");
        createAndWriteToFile("/d1/d2/f3");
        dfs.rename(new Path("/d1/d2/f2"), new Path("/d1/d2/f3"),Options.Rename.OVERWRITE);
    }

    /* Rename file with in directory, where one of the files created after taking snapshot.
     * 
     */
    @Test
    public void testRenameFile1() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f1");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createAndWriteToFile("/d1/f2");
        dfs.rename(new Path("/d1/f2"), new Path("/d1/f3"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        Data dataAfterRollBack = getData();
        boolean validity = dataAfterRollBack.equals(dataBeforeSnapshot);
        assertTrue(validity);
    }
    /*
     * Rename a file in directory[created after taking snapshot] to a file in directory[created before taking snapshot]
     * and vice versa.
     */
    @Test
    public void testRenameFile2() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createAndWriteToFile("/d1/f3");
        dfs.rename(new Path("/d1/f3"), new Path("/d1/f2"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
    }
    
     /*
     * Rename a file in directory[created before taking snapshot] to a file in other directory[created before taking snapshot]
     * and vice versa.
     */
    @Test
    public void testRenameFile3() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createAndWriteToFile("/d1/f3");
        dfs.rename(new Path("/d1/f2"), new Path("/d1/f3"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
    }
    
     /*
     * Rename a file in directory[created after taking snapshot] to a file in directory[created before taking snapshot]
     * and vice versa.
     */
    @Test
    public void testRenameFile4() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createAndWriteToFile("/d1/f3");
        dfs.rename(new Path("/d1/f2"), new Path("/d1/f3"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
    }
    
     /*
     * Rename a file created before taking snapshot with in its parent directory.
     */
    @Test
    public void testRenameFile5() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        createAndWriteToFile("/d1/f3");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        dfs.rename(new Path("/d1/f2"), new Path("/d1/f4"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
    }
    
    
    /*
     * Move directory created before taking snapshot to a directory created before taking snapshot.
     */
    @Test
    public void testRenameDirectory1() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        createAndWriteToFile("/d1/f3");
        createDirectory("/d2");
        createAndWriteToFile("/d2/f4");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        dfs.rename(new Path("/d2/"), new Path("/d1/d2"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
    }
    
    /*
     *Move directory created before taking snapshot to a directory created after taking snapshot.
     */
    @Test
    public void testRenameDirectory2() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createDirectory("/d2");
        createAndWriteToFile("/d2/f3");
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.NONE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
     }
    
     /*
     *Move directory[A] created before taking snapshot to a directory[B] created after taking snapshot which contains the directory[A] with same name as the directory[A] we want to move.
     */
    @Test
    public void testRenameDirectory3() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createDirectory("/d2");
        createDirectory("/d2/d1");
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
     }
    
    
     /*
     *Move directory[A] created before taking snapshot to a directory[B] created before taking snapshot which contains the directory[A] with same name as the directory[A] we want to move.
     */
    @Test
    public void testRenameDirectory4() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        createDirectory("/d2");
        createDirectory("/d2/d1");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
     }
    
     /*
     *Move directory[A] created after taking snapshot to a directory[B] created after taking snapshot which contains the directory[A] with same name as the directory[A] we want to move.
     */
    @Test
    public void testRenameDirectory5() throws Exception{
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        createDirectory("/d2");
        createDirectory("/d2/d1");
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
     }
    
     /*
     *Move directory[A] created before taking snapshot to a directory[B] created after taking snapshot which contains the directory[A] with same name as the directory[A] we want to move.
     */
    @Test
    public void testRenameDirectory6() throws Exception{
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createDirectory("/d2");
        createDirectory("/d2/d1");
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.OVERWRITE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
     }
    
    /*
     *Move directory to a directory, both are created after taking snapshot.
     */
    @Test
    public void testRenameDirectory7() throws Exception{
        Data dataBeforeSnapshot = getData();
        takeSnapshotOnRoot();
        createDirectory("/d1");
        createAndWriteToFile("/d1/f2");
        createAndWriteToFile("/d1/f3");
        createDirectory("/d2");
        createAndWriteToFile("/d2/f4");
        createAndWriteToFile("/d2/f5");
        dfs.rename(new Path("/d1/"), new Path("/d2/d1"),Options.Rename.NONE);
        dfs.rollBack("Admin");
        assertTrue(fetchFreshDataAndValidate(dataBeforeSnapshot));
     }
    
    
}
