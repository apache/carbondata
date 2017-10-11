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
import org.apache.carbondata.core.datastore.impl.CarbonS3FileSystem;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class S3CarbonFileTest {
    private static S3CarbonFile s3CarbonFile;
    private static FileStatus fileStatus = null;
    private static String fileName = null;
    private static Path path;
    private static Configuration configuration = null;
    private static ArrayList<FileStatus> listOfFiles = new ArrayList<FileStatus>();
    private static File file;

    @BeforeClass
    static public void setUp() throws IOException {
        file = new File("Test.carbondata");
        fileName = "s3a://Test.carbondata";
        path = new Path(fileName);
        listOfFiles.add(new FileStatus(12L, true, 20, 123L, 180L, new Path(fileName)));

        new MockUp<CarbonS3FileSystem>() {
            @Mock
            public void initialize(URI uri, Configuration conf) throws IOException {
            }

            @Mock
            public FileStatus getFileStatus(Path path) throws IOException {
                return fileStatus;
            }

            @Mock
            public FileStatus[] listStatus(Path path) throws IOException {
                return listOfFiles.toArray(new FileStatus[0]);
            }

            @Mock
            public boolean rename(Path src, Path dst) throws IOException {
                return true;
            }
        };
    }

    @Before
    public void start() {
        fileStatus = new FileStatus(12L, true, 60, 120L, 180L, new Path(fileName));
    }

    @After
    public void clean() {
        s3CarbonFile = null;
    }

    @Test
    public void testConstructorWithFilePath() {
        s3CarbonFile = new S3CarbonFile(fileName);
        assertTrue(s3CarbonFile instanceof S3CarbonFile);
    }

    @Test
    public void testConstructorWithPath() {
        s3CarbonFile = new S3CarbonFile(path);
        assertTrue(s3CarbonFile instanceof S3CarbonFile);
    }

    @Test
    public void testConstructorWithStatus() {
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return path;
            }
        };

        new MockUp<FileFactory>() {
            @Mock
            public Configuration getConfiguration() {
                return configuration;
            }
        };
        s3CarbonFile = new S3CarbonFile(fileStatus);
        assertTrue(s3CarbonFile instanceof S3CarbonFile && s3CarbonFile.exists());
    }

    @Test
    public void testListFilesWithCarbonFilter() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {

            @Override
            public boolean accept(CarbonFile file) {
                return true;
            }
        };
        s3CarbonFile = new S3CarbonFile(fileStatus);
        assertTrue(s3CarbonFile.listFiles(carbonFileFilter).length == 1);
    }

    @Test
    public void testListFilesWithoutCarbonFilter() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {

            @Override
            public boolean accept(CarbonFile file) {
                return false;
            }
        };
        s3CarbonFile = new S3CarbonFile(path);
        assertTrue(s3CarbonFile.listFiles(carbonFileFilter).length == 0);
    }

    @Test
    public void testGetParentFile() {
        new MockUp<Path>() {
            @Mock
            public Path getParent() {
                return new Path(file.getAbsolutePath());
            }
        };

        s3CarbonFile = new S3CarbonFile(fileStatus);
        assertFalse(s3CarbonFile.getParentFile().equals(null));
    }

    @Test
    public void testRenameToNew() {
        fileStatus = new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName));
        s3CarbonFile = new S3CarbonFile(path);
        assertTrue(s3CarbonFile.renameTo("test/bucketOne"));
    }

    @Test
    public void testRenameToGetException() {
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() throws IOException {
                throw new IOException();
            }
        };
        s3CarbonFile = new S3CarbonFile(path);
        assertFalse(s3CarbonFile.renameTo("test/bucketOne"));
    }

    @Test
    public void testRenameForce() {
        s3CarbonFile = new S3CarbonFile(path);
        assertTrue(s3CarbonFile.renameForce("test/bucketOne"));
        assertFalse(s3CarbonFile.createNewFile());
    }

    @Test
    public void testRenameForceException() {
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() throws IOException {
                throw new IOException();
            }
        };

        s3CarbonFile = new S3CarbonFile(path);
        s3CarbonFile.setLastModifiedTime(123654789L);
        assertEquals(s3CarbonFile.getLastModifiedTime(), 180);
        assertFalse(s3CarbonFile.renameForce("test/bucketOne"));
        assertFalse(s3CarbonFile.delete());
    }
}
