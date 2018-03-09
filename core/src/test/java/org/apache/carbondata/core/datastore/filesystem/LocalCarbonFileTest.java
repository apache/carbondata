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

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import sun.nio.ch.FileChannelImpl;

import java.io.*;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class LocalCarbonFileTest {

    private static LocalCarbonFile localCarbonFile;
    private static File file;
    private static File dir;
    private static FileOutputStream oFile;

    @BeforeClass
    static public void setUp() {
        file = new File("TestLocalCarbonFile");
        dir = new File("TestLocalCarbonDir");
        if (!file.exists())
            try {
                file.createNewFile();
                dir.mkdir();
            } catch (IOException e) {
                e.printStackTrace();
            }
        try {
            oFile = new FileOutputStream(file, true);


            byte[] bytes = "core java api".getBytes();

            oFile.write(bytes);
            oFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            localCarbonFile = new LocalCarbonFile(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    static public void cleanUp() {
        file.delete();
        dir.delete();

    }

    @Test
    public void testListFilesWithCarbonFileFilterAndWithOutOutDirectoryPermission() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
                return false;
            }
        };
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return false;
            }


        };
        assertArrayEquals(localCarbonFile.listFiles(carbonFileFilter), new CarbonFile[0]);
    }

    @Test
    public void testListFilesWithOutDirPermission() {
        localCarbonFile = new LocalCarbonFile(file);
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return false;
            }
        };
        assertArrayEquals(localCarbonFile.listFiles(), new CarbonFile[0]);
    }

    @Test
    public void testCreateNewFileForException() throws IOException {
        localCarbonFile = new LocalCarbonFile(new File(""));
        assertTrue(!localCarbonFile.createNewFile());
    }

    @Test
    public void testCheckIfFileExists() throws IOException {
        localCarbonFile = new LocalCarbonFile(new File(""));
        assertTrue(!localCarbonFile.exists());
    }

    @Test
    public void testRenameForce() {
        localCarbonFile = new LocalCarbonFile(file);
        String destFile = "TestRename" + UUID.randomUUID().toString();
        assertTrue(localCarbonFile.renameForce(destFile));
        File file1 = new File(destFile);
        if (file1.exists()) {
            file1.delete();
        }
    }

    @Test
    public void testRenameTo() {
        localCarbonFile = new LocalCarbonFile(file);
        String destFile = "TestRename" + UUID.randomUUID().toString();
        assertTrue(!localCarbonFile.renameTo(destFile));
        File file1 = new File(destFile);
        if (file1.exists()) {
            file1.delete();
        }
    }

    @Test
    public void testsetLastModifiedTime() {
        localCarbonFile = new LocalCarbonFile(file);
        assertTrue(!localCarbonFile.setLastModifiedTime(50L));
    }

    @Test
    public void testtruncate() {
        localCarbonFile = new LocalCarbonFile(file);
        final int[] counter = {0};
        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                {
                    return FileFactory.FileType.LOCAL;
                }
            }
        };
        new MockUp<FileFactory>() {
            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                {
                    return true;
                }
            }
        };
        new MockUp<CarbonFile>() {
            @Mock
            boolean delete() {
                return true;
            }
        };
        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewFile(String filePath, FileFactory.FileType fileType) throws IOException {
                {
                    return true;
                }
            }
        };
        new MockUp<FileFactory>() {
            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                {
                    return new LocalCarbonFile(path);
                }
            }
        };
        new MockUp<CarbonFile>() {
            @Mock
            boolean delete() {
                return true;
            }
        };

        new MockUp<FileChannelImpl>() {
            @Mock
            public long transferFrom(ReadableByteChannel var1, long var2, long var4) throws IOException {
                if (counter[0] == 0) {
                    counter[0] = counter[0] + 1;
                    return 0L;
                } else {
                    return 1L;
                }
            }
        };
        new MockUp<CarbonFile>() {
            @Mock
            boolean renameForce(String changetoName) {
                return true;
            }
        };
        localCarbonFile = new LocalCarbonFile(file);
        assertTrue(localCarbonFile.truncate(file.getName(), 1L));
    }

    @Test
    public void testtruncateForException() throws IOException {
        localCarbonFile = new LocalCarbonFile(file);
        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                {
                    return FileFactory.FileType.LOCAL;
                }
            }
        };
        new MockUp<FileFactory>() {
            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException {
                {
                    return true;
                }
            }
        };
        new MockUp<FileFactory>() {
            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                {
                    return new LocalCarbonFile(path);
                }
            }
        };
        new MockUp<CarbonFile>() {
            @Mock
            boolean delete() {
                return true;
            }
        };
        new MockUp<FileFactory>() {
            @Mock
            public boolean createNewFile(String filePath, FileFactory.FileType fileType) throws IOException {
                {
                    throw new IOException();
                }
            }
        };


        localCarbonFile.truncate(file.getName(), 2L);
    }

    @Test
    public void testListFilesWithDirPermission() {
        localCarbonFile = new LocalCarbonFile(file);
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }
        };
        new MockUp<File>() {
            @Mock
            public File[] listFiles() {
                return null;
            }


        };
        localCarbonFile = new LocalCarbonFile(dir);
        assertTrue(localCarbonFile.listFiles().length == 0);
    }

    @Test
    public void testListFilesWithCarbonFileFilterAndDirectoryPermission() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
                return true;
            }
        };
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }
        };
        new MockUp<File>() {
            @Mock
            public File[] listFiles(FileFilter filter) {

                return new File[]{dir};
            }


        };

        localCarbonFile = new LocalCarbonFile(dir);

        assertTrue(localCarbonFile.listFiles(carbonFileFilter).length == 1);
    }

    @Test
    public void testListFilesForNullWithCarbonFileFilterAndDirectoryPermission() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
                return true;
            }
        };
        new MockUp<File>() {
            @Mock
            public File[] listFiles(FileFilter filter) {
                return null;
            }


        };
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return false;
            }

            @Mock
            public File[] listFiles(FileFilter filter) {
                return null;
            }


        };
        localCarbonFile = new LocalCarbonFile(dir);

        assertArrayEquals(localCarbonFile.listFiles(carbonFileFilter) , new CarbonFile[0]);
    }

    @Test
    public void testListFilesForEmptyFileArrayWithCarbonFileFilter() {
        CarbonFileFilter carbonFileFilter = new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
                return true;
            }
        };
        new MockUp<CarbonFileFilter>() {
            @Mock
            boolean accept(CarbonFile file) {
                return true;
            }
        };
        new MockUp<File>() {
            @Mock
            public File[] listFiles(FileFilter filter) {
                return null;
            }
        };
        new MockUp<File>() {
            @Mock
            public boolean isDirectory() {
                return true;
            }

            @Mock
            public File[] listFiles(FileFilter filter) {
                return null;
            }
        };
        localCarbonFile = new LocalCarbonFile(dir);

        assertTrue(localCarbonFile.listFiles(carbonFileFilter).length == 0);
    }

    @Test
    public void testFilesForConnicalPath() {

        new MockUp<File>() {
            @Mock
            public String getCanonicalPath() throws IOException {
                return "testFile";
            }


        };
        localCarbonFile = new LocalCarbonFile(dir);

        assertTrue(Objects.equals(localCarbonFile.getCanonicalPath(), "testFile"));
    }

    @Test
    public void testFilesForConnicalPathException() throws IOException {

        new MockUp<File>() {
            @Mock
            public String getCanonicalPath() throws IOException {
                throw new IOException();
            }


        };
        localCarbonFile = new LocalCarbonFile(dir);

        localCarbonFile.getCanonicalPath();
    }

    @Test
    public void testFilesForAbsolutePath() {

        new MockUp<File>() {
            @Mock
            public String getAbsolutePath() {
                return "testFile";
            }


        };
        localCarbonFile = new LocalCarbonFile(dir);

        assertEquals(localCarbonFile.getAbsolutePath(), "testFile");
    }

    @Test
    public void testFilesForGetPath() {

        new MockUp<File>() {
            @Mock
            public String getPath() {
                return "testFile";
            }


        };
        localCarbonFile = new LocalCarbonFile(dir);

        assertEquals(localCarbonFile.getPath(), "testFile");
    }

    @Test
    public void testFilesForFileExists() {

        localCarbonFile = new LocalCarbonFile(new File(""));
        assertEquals(localCarbonFile.exists(), false);
    }

    @Test
    public void testRenameForceForFileNotExists() {
        new MockUp<File>() {
            @Mock
            public boolean exists() {
                return false;
            }

            @Mock
            public boolean renameTo(File dest) {
                return true;
            }
        };

        localCarbonFile = new LocalCarbonFile("demo.txt");

        assertEquals(localCarbonFile.renameForce("renameToFile"), true);
    }
}
