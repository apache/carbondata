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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

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
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(file.getAbsolutePath());
            }

        };
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return new DummyAlluxioFileSystem();
            }

        };
        new MockUp<DummyAlluxioFileSystem>() {
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
                return new DummyAlluxioFileSystem();
            }

        };
        new MockUp<DummyAlluxioFileSystem>() {
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
        new MockUp<DummyAlluxioFileSystem>() {
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
                return new DummyAlluxioFileSystem();
            }
        };
        new MockUp<DummyAlluxioFileSystem>() {
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
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(file.getAbsolutePath());
            }
        };
        new MockUp<Path>() {
            @Mock
            public Path getParent() {
                return new Path(file.getAbsolutePath()
                );
            }
        };
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        assertFalse(alluxioCarbonFile.getParentFile().equals(null));
    }

    //@Test
    public void testForNonDisributedSystem() {
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(file.getAbsolutePath());
            }
        };
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException {
                return fileStatus.getPath().getFileSystem(conf);
            }
        };
        new MockUp<FileSystem>() {
            @Mock
            public boolean delete(Path var1,boolean overwrite) throws IOException {
                return getMockInstance().delete(var1,overwrite);
            }
        };
        new MockUp<FileSystem>() {
            @Mock
            public boolean rename(Path var1,Path changeToName) throws IOException {
                return getMockInstance().rename(var1, changeToName);
            }
        };
        assertTrue(alluxioCarbonFile.renameForce(fileName));
    }

    //@Test
    public void testrenameForceForDisributedSystem() {
        new MockUp<FileStatus>() {
            @Mock
            public Path getPath() {
                return new Path(file.getAbsolutePath());
            }
        };
        new MockUp<Path>() {
            @Mock
            public FileSystem getFileSystem(Configuration conf) throws IOException, URISyntaxException {
                return new DummyAlluxioFileSystem();
            }
        };
        new MockUp<FileSystem>() {
            @Mock
            public boolean delete(Path var1,boolean overwrite) throws IOException {
                return getMockInstance().delete(var1,overwrite);
            }
        };
        new MockUp<FileSystem>() {
            @Mock
            public FSDataOutputStream create(Path var1,boolean overwrite) throws IOException {
                //return getMockInstance().create(var1,overwrite);
                return new FSDataOutputStream(new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {

                    }
                }, null);
            }
        };
        new MockUp<FileSystem>() {
            @Mock
            public FSDataInputStream open(Path var1) throws IOException {
                return new FSDataInputStream(new FSInputStream() {
                    @Override
                    public void seek(long l) throws IOException {

                    }

                    @Override
                    public long getPos() throws IOException {
                        return 0;
                    }

                    @Override
                    public boolean seekToNewSource(long l) throws IOException {
                        return false;
                    }

                    @Override
                    public int read() throws IOException {
                        return 0;
                    }
                });
            }
        };
        new MockUp<FSDataInputStream>() {
            @Mock
            public void close() throws IOException {
                getMockInstance().close();
            }
        };
        new MockUp<FSDataOutputStream>() {
            @Mock
            public void close() throws IOException {
                getMockInstance().close();
            }
        };
        alluxioCarbonFile = new AlluxioCarbonFile(fileStatus);
        assertTrue(alluxioCarbonFile.renameForce(fileName));
    }

    class DummyAlluxioFileSystem extends FileSystem {

        @Override
        public URI getUri() {
            return null;
        }

        @Override
        public FSDataInputStream open(Path path, int i) throws IOException {
            return open(path,i);
        }

        @Override
        public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable) throws IOException {
            return null;
        }

        @Override
        public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
            return null;
        }

        @Override
        public boolean rename(Path path, Path path1) throws IOException {
            return true;
        }

        @Override
        public boolean delete(Path path, boolean b) throws IOException {
            return true;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
            return new FileStatus[0];
        }

        @Override
        public void setWorkingDirectory(Path path) {

        }

        @Override
        public Path getWorkingDirectory() {
            return null;
        }

        @Override
        public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
            return false;
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            return null;
        }
    }
}
