/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.filesystem;

import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ViewFsCarbonFileTest {

    private static ViewFSCarbonFile viewFSCarbonFile;
    private static FileStatus fileStatus;
    private static FileStatus fileStatusWithOutDirectoryPermission;
    private static String fileName;
    private static File file;


    @BeforeClass
    static public void setUp() {
        file = new File("Test.carbondata");
        if (!file.exists())
            try {
              file.createNewFile();
            } catch (IOException e) {
              e.printStackTrace();
            }
        try {
          FileOutputStream oFile = new FileOutputStream(file, true);
          oFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        fileStatus = new FileStatus(12L, true, 60, 120l, 180L, new Path(file.getAbsolutePath()));
        fileStatusWithOutDirectoryPermission = new FileStatus(12L, false, 60, 120l, 180L, new Path(file.getAbsolutePath()));
        fileName = file.getAbsolutePath();
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
    }

    @AfterClass
    static public void cleanUp() {
        file.delete();
    }

    @Test
    public void testRenameForceForException() throws IOException {

        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                throw new IOException();
            }

        };
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
        viewFSCarbonFile.renameForce(fileName);
    }

    @Test
    public void testListFilesWithOutDirectoryPermission() {
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatusWithOutDirectoryPermission);
        assertArrayEquals(viewFSCarbonFile.listFiles(), new CarbonFile[0]);
    }

    @Test
    public void testConstructorWithFilePath() {
        viewFSCarbonFile = new ViewFSCarbonFile(file.getAbsolutePath());
        assertTrue(viewFSCarbonFile instanceof ViewFSCarbonFile);
    }

    @Test
    public void testListFilesForNullListStatus() {
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatusWithOutDirectoryPermission);
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new ViewFileSystem();
            }

        };
        new MockUp<ViewFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                return null;
            }

        };
        new MockUp<ViewFileSystem>() {
            @Mock
            public boolean delete(Path var1, boolean var2) throws IOException {

                return true;
            }

        };
        //public boolean delete(Path var1, boolean var2) throws IOException;
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
        assertTrue(viewFSCarbonFile.listFiles().length == 0);
    }

    @Test
    public void testListDirectory() {
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new ViewFileSystem();
            }

        };
        new MockUp<ViewFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                return new FileStatus[]{new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName))};
            }

        };

        assertTrue(viewFSCarbonFile.listFiles().length == 1);
    }

    @Test
    public void testListFilesForException() throws IOException {
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatusWithOutDirectoryPermission);

        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(file.getAbsolutePath());
            }

        };
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                throw new IOException();
            }

        };
        new MockUp<ViewFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                throw new IOException();
            }

        };
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
        viewFSCarbonFile.listFiles();
    }

    @Test
    public void testListFilesWithCarbonFilter() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {

            @Override
            public boolean accept(CarbonFile file) {
                return true;
            }
        };
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
        assertTrue(viewFSCarbonFile.listFiles(carbonFileFilter).length == 1);
    }

    @Test
    public void testlistFilesWithoutFilter() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {

            @Override
            public boolean accept(CarbonFile file) {
                return false;
            }
        };
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new ViewFileSystem();
            }

        };
        new MockUp<ViewFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                return new FileStatus[]{new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName))};
            }

        };
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
        assertTrue(viewFSCarbonFile.listFiles(carbonFileFilter).length == 0);
    }

    @Test
    public void testGetParentFIle() {
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new DistributedFileSystem();
            }

        };
        new MockUp<Path>() {
            @Mock
            public Path getParent() {
                return new Path(file.getAbsolutePath()
                );
            }

        };
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(file.getAbsolutePath());
            }

        };
        new MockUp<DistributedFileSystem>() {
            @Mock
            public FileStatus getFileStatus(Path f) throws IOException {

                return new FileStatus(12L, true, 60, 120l, 180L, new Path(file.getAbsolutePath()));
            }

        };

        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
        assertFalse(viewFSCarbonFile.getParentFile().equals(null));
    }

    @Test
    public void testForNonDisributedSystem() {
        viewFSCarbonFile = new ViewFSCarbonFile(fileStatus);
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new WebHdfsFileSystem();
            }

        };
        assertFalse(viewFSCarbonFile.renameForce(fileName));
    }

    @Test
    public void testrenameForceForViewFileSystem() {
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new ViewFileSystem();
            }

        };
        new MockUp<ViewFileSystem>() {
            @Mock
            public boolean delete(Path f, boolean recursive) throws
                    IOException {
                return true;

            }

        };
        new MockUp<ViewFileSystem>() {
            @Mock
            public boolean rename(Path src, Path dst) throws IOException {
                return true;

            }

        };

        assertTrue(viewFSCarbonFile.renameForce(fileName));

    }
}
