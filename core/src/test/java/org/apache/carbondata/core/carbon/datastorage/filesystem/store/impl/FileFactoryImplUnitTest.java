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

package org.apache.carbondata.core.carbon.datastorage.filesystem.store.impl;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.datastore.impl.FileFactory;

import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileFactoryImplUnitTest {

  private static String filePath;

  @AfterClass
  public static void tearDown() {
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }

    File file1 = new File(".TestFileFactory.carbondata.crc");
    if (file1.exists()) {
      file1.delete();
    }
  }

  @BeforeClass public static void setUp() {
    filePath = "TestFileFactory.carbondata";
  }

  @Test public void testFileExistsForVIEWFSType() throws IOException {
    FileFactory.isFileExist("fakefilePath", FileFactory.FileType.VIEWFS);
  }

  @Test public void testFileExistsForDefaultType() throws IOException {
    FileFactory.isFileExist("fakefilePath", FileFactory.FileType.LOCAL);
  }

  @Test public void testFileExistsForDefaultTypeWithPerformFileCheck() throws IOException {
    assertTrue(FileFactory.isFileExist(filePath, FileFactory.FileType.LOCAL, true));
  }

  @Test public void testFileExistsForDefaultTypeWithOutPerformFileCheck() throws IOException {
    assertFalse(FileFactory.isFileExist("fakefilePath", FileFactory.FileType.LOCAL, false));
  }

  @Test public void testFileExistsForVIEWFSTypeWithPerformFileCheck() throws IOException {
    assertTrue(FileFactory.isFileExist(filePath, FileFactory.FileType.VIEWFS, true));
  }

  @Test public void testFileExistsForVIEWFSTypeWithOutPerformFileCheck() throws IOException {
    assertFalse(FileFactory.isFileExist("fakefilePath", FileFactory.FileType.VIEWFS, false));
  }

  @Test public void testCreateNewFileWithDefaultFileType() throws IOException {
    tearDown();
    assertTrue(FileFactory.createNewFile(filePath, FileFactory.FileType.LOCAL));
  }

  @Test public void testCreateNewLockFileWithDefaultFileType() throws IOException {
    tearDown();
    assertTrue(FileFactory.createNewLockFile(filePath, FileFactory.FileType.LOCAL));
  }

  @Test public void testCreateNewLockFileWithViewFsFileType() throws IOException {
    tearDown();
    assertTrue(FileFactory.createNewLockFile(filePath, FileFactory.FileType.VIEWFS));
  }

  @Test public void testCreateNewLockFileWithViewFsFileTypeWhenFileExists() throws IOException {
    assertFalse(FileFactory.createNewLockFile(filePath, FileFactory.FileType.VIEWFS));
  }

  @Test public void testCreateNewFileWithDefaultFileTypeWhenFileExists() throws IOException {
    assertFalse(FileFactory.createNewFile(filePath, FileFactory.FileType.LOCAL));
  }

  @Test public void testCreateNewFileWithVIEWFSFileType() throws IOException {
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
    assertTrue(FileFactory.createNewFile(filePath, FileFactory.FileType.VIEWFS));
  }

  @Test public void testCreateNewFileWithVIEWFSFileTypeWhenFileExists() throws IOException {
    assertFalse(FileFactory.createNewFile(filePath, FileFactory.FileType.VIEWFS));
  }

  @Test public void testMkDirWithVIEWFSFileType() throws IOException {
    new MockUp<FileSystem>() {
      @SuppressWarnings("unused") @Mock public boolean mkdirs(Path file) throws IOException {
        {
          return true;
        }
      }
    };
    tearDown();
    assertTrue(FileFactory.mkdirs(filePath, FileFactory.FileType.VIEWFS));
  }

  @Test public void testGetDataOutputStreamUsingAppendeForException() {
    try {
      FileFactory.getDataOutputStreamUsingAppend(filePath, FileFactory.FileType.VIEWFS);
    } catch (Exception exception) {
      assertEquals("Not supported", exception.getMessage());
    }
  }

  @Test public void getDataOutputStreamForVIEWFSType() throws IOException {
    assertNotNull(FileFactory.getDataOutputStream(filePath, FileFactory.FileType.VIEWFS));
  }

  @Test public void getDataOutputStreamForCFSType() throws IOException {
    assertNotNull(FileFactory.getDataOutputStream(filePath, FileFactory.FileType.CFS));
  }

  @Test public void getDataOutputStreamForLocalType() throws IOException {
    assertNotNull(FileFactory.getDataOutputStream(filePath, FileFactory.FileType.LOCAL));
  }

  @Test public void testGetCarbonFile() throws IOException {
    FileFactory.getDataOutputStream(filePath, FileFactory.FileType.VIEWFS);
    assertNotNull(FileFactory.getCarbonFile(filePath, FileFactory.FileType.HDFS));
  }
}

