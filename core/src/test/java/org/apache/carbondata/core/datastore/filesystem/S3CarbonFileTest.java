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
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        fileStatus = null;
    }

    @Test
    public void testConstructorWithFilePath() {

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

        s3CarbonFile = new S3CarbonFile(fileName);
        assertTrue(s3CarbonFile instanceof S3CarbonFile);
    }

    @Test
    public void testConstructorWithPath() {

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

        s3CarbonFile = new S3CarbonFile(path);
        assertTrue(s3CarbonFile instanceof S3CarbonFile);
    }

    @Test
    public void testConstructorWithStatus() {

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

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

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(true, true, path);
            }
        };

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

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

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

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

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

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

        fileStatus = new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName));
        s3CarbonFile = new S3CarbonFile(path);
        assertTrue(s3CarbonFile.renameTo("test/bucketOne"));
    }

    @Test
    public void testRenameToGetException() {

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

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

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

        s3CarbonFile = new S3CarbonFile(path);
        assertTrue(s3CarbonFile.renameForce("test/bucketOne"));
        assertFalse(s3CarbonFile.createNewFile());
    }

    @Test
    public void testRenameForceException() {

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 5L, path, 44L);
            }
        };

        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() throws IOException {
                throw new IOException();
            }
        };

        s3CarbonFile = new S3CarbonFile(path);
        assertFalse(s3CarbonFile.renameForce("test/bucketOne"));
        assertFalse(s3CarbonFile.delete());
    }

    @Test
    public void testIsFileModified() {

        FileStatus fileStatus = new FileStatus(12L, true, 60, 120L, 180L, new Path("s3a://bucket/fileName"));
        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 180L, new Path("s3a://bucket/filename"), 44L);
            }
        };

        new MockUp<FileStatus>() {
            @Mock
            public long getModificationTime() {
                return 190L;
            }

            @Mock
            public Path getPath() {
                return new Path("s3a://bucket/filename");
            }
        };

        s3CarbonFile = new S3CarbonFile(fileStatus);
        assertTrue(s3CarbonFile.isFileModified(170L, 120L));
    }

    @Test
    public void testTruncateForException() {

        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.S3;
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType, boolean performFileCheck)
                    throws IOException {
                return true;
            }

            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                return new S3CarbonFile("s3a://bucket/filename");
            }

            @Mock
            public boolean createNewFile(String filePath, FileFactory.FileType fileType) throws IOException {
                return true;
            }

            @Mock
            public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType) throws IOException {
                throw new IOException("Exception");
            }
        };

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 180L, new Path("s3a://bucket/filename"), 44L);
            }

            @Mock
            public boolean delete(Path f, boolean recursive) throws IOException {
                return true;
            }
        };

        new MockUp<FileStatus>() {
            @Mock
            public long getModificationTime() {
                return 190L;
            }

            @Mock
            public Path getPath() {
                return new Path("s3a://bucket/filename");
            }
        };

        new MockUp<CarbonUtil>() {
            @Mock
            public void closeStreams(Closeable... streams) {
            }
        };

        s3CarbonFile = new S3CarbonFile(fileStatus);
        assertFalse(s3CarbonFile.truncate("/toTruncate", 120L));
    }

    @Test
    public void testTruncate() {

        new MockUp<S3AFileSystem>() {
            @Mock
            public S3AFileStatus getFileStatus(Path f) throws IOException {
                return new S3AFileStatus(5L, 180L, new Path("s3a://bucket/filename"), 44L);
            }

            @Mock
            public boolean delete(Path f, boolean recursive) throws IOException {
                return true;
            }
        };

        new MockUp<FileStatus>() {
            @Mock
            public long getModificationTime() {
                return 190L;
            }

            @Mock
            public Path getPath() {
                return new Path("s3a://bucket/filename");
            }
        };

        new MockUp<CarbonUtil>() {
            @Mock
            public void closeStreams(Closeable... streams) {
            }
        };

        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.S3;
            }

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType, boolean performFileCheck)
                    throws IOException {
                return true;
            }

            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                return new S3CarbonFile("s3a://bucket/filename");
            }

            @Mock
            public boolean createNewFile(String filePath, FileFactory.FileType fileType) throws IOException {
                return true;
            }

            @Mock
            public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType)
                    throws IOException {
                return new FSDataOutputStream(new S3AOutputStream(new Configuration(), null, null, "bucket", "key",
                        null, null, null, null));
            }

            @Mock
            public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType) throws IOException {
                return new FSDataInputStream(new S3AInputStream("bucket", "key", 120L, null, null));
            }
        };

        new MockUp<S3AInputStream>() {
            @Mock
            public synchronized int read(byte[] buf, int off, int len) throws IOException {
                return 120;
            }
        };

        new MockUp<S3AOutputStream>() {
            @Mock
            public void write(byte[] b, int off, int len) throws IOException {

            }
        };

        s3CarbonFile = new S3CarbonFile(fileStatus);
        assertTrue(s3CarbonFile.truncate("/toTruncate", 120L));
    }

}