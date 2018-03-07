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
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.util.Progressable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class HDFSCarbonFileTest {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(HDFSCarbonFileTest.class.getName());
    private static HDFSCarbonFile hdfsCarbonFile;
    private static FileStatus fileStatus = null;
    private static FileStatus fileStatusWithOutDirectoryPermission;
    private static String fileName = null;
    private static FileSystem fs = null;
    private static Path pt;

    @BeforeClass
    static public void setUp() throws IOException {
        Configuration config = new Configuration();
        // adding local hadoop configuration
        config.addResource(new Path("core-site.xml"));
        config.addResource(new Path("hdfs-site.xml"));
        fileName = "Test.carbondata"; //this path is HDFS path
        pt = new Path(fileName);
        fs = FileSystem.get(new Configuration(config));
        fs.create(pt);
        if (fs.exists(pt)) {
            OutputStream os = fs.create(pt,
                    new Progressable() {
                        public void progress() {
                            LOGGER.info("Started Writing to File===");
                        }
                    });
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            br.write("Hello World");
            br.close();
            os.close();
            fs.close();

            fileStatus = new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName));
            fileStatusWithOutDirectoryPermission = new FileStatus(12L, false, 60, 120l, 180L, new Path(fileName));
            hdfsCarbonFile = new HDFSCarbonFile(fileStatus);

        }
    }

    @AfterClass
    static public void cleanUp() {
        try {
            fs.delete(pt, true);
        } catch (IOException e) {
            LOGGER.error("Exception Occured" + e.getMessage());
        }
    }

    @Test
    public void testRenameForceForException() throws IOException {

        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                throw new IOException();
            }

        };
        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
        hdfsCarbonFile.renameForce(fileName);
    }

    @Test
    public void testListFilesWithOutDirectoryPermission() {
        hdfsCarbonFile = new HDFSCarbonFile(fileStatusWithOutDirectoryPermission);
        new MockUp<FileStatus>() {
            @Mock
            public boolean isDirectory() {
                return false;
            }

        };

        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new DistributedFileSystem();
            }

        };

        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(fileName);
            }

        };
        assertArrayEquals(hdfsCarbonFile.listFiles(), new CarbonFile[0]);
    }

    @Test
    public void testConstructorWithFilePath() {
        hdfsCarbonFile = new HDFSCarbonFile(fileName);
        assertTrue(hdfsCarbonFile instanceof HDFSCarbonFile);
    }

    @Test
    public void testListFilesForNullListStatus() {
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
        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
        assertEquals(hdfsCarbonFile.listFiles().length, 0);
    }

    @Test
    public void testListDirectory() {
        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
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

        assertEquals(hdfsCarbonFile.listFiles().length, 1);
    }

    @Test
    public void testListFilesForException() throws IOException {
        new HDFSCarbonFile(fileStatusWithOutDirectoryPermission);

        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(fileName);
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
        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
        hdfsCarbonFile.listFiles();
    }

    @Test
    public void testListFilesWithCarbonFilter() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {

            @Override
            public boolean accept(CarbonFile file) {
                return true;
            }
        };
        new MockUp<FileStatus>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }

        };

        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new DistributedFileSystem();
            }

        };

        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(fileName);
            }

        };
        new MockUp<DistributedFileSystem>() {
            @Mock
            public FileStatus[] listStatus(Path var1) throws IOException {

                return new FileStatus[]{new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName))};
            }

        };
        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
        assertEquals(hdfsCarbonFile.listFiles(carbonFileFilter).length, 1);
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
        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
        assertEquals(hdfsCarbonFile.listFiles(carbonFileFilter).length, 0);
    }

    @Test
    public void testGetParentFileForNull() {

        new MockUp<Path>() {
            @Mock
            public Path getParent() {
                return null;
            }

        };
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(fileName);
            }

        };
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new DistributedFileSystem();
            }

        };

        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(fileName);
            }

        };
        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
        assertEquals(hdfsCarbonFile.getParentFile(), null);
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
                return new Path(fileName);
            }

        };
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(fileName);
            }

        };
        new MockUp<DistributedFileSystem>() {
            @Mock
            public FileStatus getFileStatus(Path file) throws IOException {

                return new FileStatus(12L, true, 60, 120l, 180L, new Path(fileName));
            }

        };

        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
        assertTrue(hdfsCarbonFile.getParentFile() instanceof CarbonFile);
    }

    @Test
    public void testForNonDisributedSystem() {
        new HDFSCarbonFile(fileStatus);
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new WebHdfsFileSystem();
            }

        };
        new MockUp<WebHdfsFileSystem>(){
            @Mock
            public boolean rename(final Path src, final Path dst) throws IOException {
                return true;
            }
        };
        assertEquals(hdfsCarbonFile.renameForce(fileName), true);
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
        hdfsCarbonFile = new HDFSCarbonFile(fileStatus);
        assertEquals(hdfsCarbonFile.renameForce(fileName), true);

    }
}

