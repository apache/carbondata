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
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
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

public class AlluxioCarbonFileTest {

    private static AlluxioCarbonFile alluxioCarbonFile;
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
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
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
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        alluxioCarbonFile.renameForce(fileName);
    }

    @Test
    public void testListFilesWithOutDirectoryPermission() {
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatusWithOutDirectoryPermission);
        assertArrayEquals(alluxioCarbonFile.listFiles(), new CarbonFile[0]);
    }

    @Test
    public void testConstructorWithFilePath() {
        alluxioCarbonFile = new AlluxioCarbonFile(file.getAbsolutePath());
        assertTrue(alluxioCarbonFile instanceof AlluxioCarbonFile);
    }

    @Test
    public void testListFilesForNullListStatus() {
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatusWithOutDirectoryPermission);
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new DistributedFileSystem();
            }

        };
        new MockUp<DistributedFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                return null;
            }

        };
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        assertTrue(alluxioCarbonFile.listFiles().length == 0);
    }

    @Test
    public void testListDirectory() {
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new DistributedFileSystem();
            }

        };
        new MockUp<DistributedFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                return new FileStatus[]{new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName))};
            }

        };

        assertTrue(alluxioCarbonFile.listFiles().length == 1);
    }

    @Test
    public void testListFilesForException() throws IOException {
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatusWithOutDirectoryPermission);

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
        new MockUp<DistributedFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                throw new IOException();
            }

        };
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        alluxioCarbonFile.listFiles();
    }

    @Test
    public void testListFilesWithCarbonFilter() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {

            @Override
            public boolean accept(CarbonFile file) {
                return true;
            }
        };
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        assertTrue(alluxioCarbonFile.listFiles(carbonFileFilter).length == 1);
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
                return new DistributedFileSystem();
            }

        };
        new MockUp<DistributedFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                return new FileStatus[]{new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName))};
            }

        };
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        assertTrue(alluxioCarbonFile.listFiles(carbonFileFilter).length == 0);
    }

    @Test
    public void testGetParentFile() {
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
            public FileStatus getFileStatus(Path path) throws IOException {

                return new FileStatus(12L, true, 60, 120l, 180L, new Path(file.getAbsolutePath()));
            }

        };

        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        assertFalse(alluxioCarbonFile.getParentFile().equals(null));
    }

    @Test
    public void testForNonDisributedSystem() {
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new WebHdfsFileSystem();
            }

        };
        assertFalse(alluxioCarbonFile.renameForce(fileName));
    }

    @Test
    public void testrenameForceForDisributedSystem() {
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new DistributedFileSystem();
            }

        };
        new MockUp<DistributedFileSystem>() {
            @Mock
            public void rename(Path src, Path dst, final Options.Rename... options) throws IOException {

            }

        };

        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        assertTrue(alluxioCarbonFile.renameForce(fileName));

    }
}
