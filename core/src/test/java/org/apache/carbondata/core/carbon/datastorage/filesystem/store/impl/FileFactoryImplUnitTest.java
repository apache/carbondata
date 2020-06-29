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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastore.impl.DefaultFileTypeProvider;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileFactoryImplUnitTest {

  private static String filePath;

  @AfterClass
  public static void tearDown() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1114
    cleanUp();
  }

  private static void cleanUp() {
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
    FileFactory.isFileExist("fakefilePath");
  }

  @Test public void testFileExistsForDefaultType() throws IOException {
    FileFactory.isFileExist("fakefilePath");
  }

  @Test public void testFileExistsForDefaultTypeWithPerformFileCheck() throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3404
    assertTrue(FileFactory.isFileExist(filePath, true));
  }

  @Test public void testFileExistsForDefaultTypeWithOutPerformFileCheck() throws IOException {
    assertFalse(FileFactory.isFileExist("fakefilePath", false));
  }

  @Test public void testFileExistsForVIEWFSTypeWithPerformFileCheck() throws IOException {
    assertTrue(FileFactory.isFileExist(filePath, true));
  }

  @Test public void testFileExistsForVIEWFSTypeWithOutPerformFileCheck() throws IOException {
    assertFalse(FileFactory.isFileExist("fakefilePath", false));
  }

  @Test public void testCreateNewFileWithDefaultFileType() throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1114
    cleanUp();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
    assertTrue(FileFactory.createNewFile(filePath));
  }

  @Test public void testCreateNewLockFileWithDefaultFileType() throws IOException {
    cleanUp();
    assertTrue(FileFactory.createNewLockFile(filePath));
  }

  @Test public void testCreateNewLockFileWithViewFsFileType() throws IOException {
    cleanUp();
    assertTrue(FileFactory.createNewLockFile(filePath));
  }

  @Test public void testCreateNewLockFileWithViewFsFileTypeWhenFileExists() throws IOException {
    assertFalse(FileFactory.createNewLockFile(filePath));
  }

  @Test public void testCreateNewFileWithDefaultFileTypeWhenFileExists() throws IOException {
    assertFalse(FileFactory.createNewFile(filePath));
  }

  @Test public void testCreateNewFileWithVIEWFSFileType() throws IOException {
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
    assertTrue(FileFactory.createNewFile(filePath));
  }

  @Test public void testCreateNewFileWithVIEWFSFileTypeWhenFileExists() throws IOException {
    assertFalse(FileFactory.createNewFile(filePath));
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
    assertTrue(FileFactory.mkdirs(filePath));
  }

  @Test public void testGetDataOutputStreamUsingAppendeForException() throws IOException {
    DataOutputStream outputStream = null;
    try {
      outputStream = FileFactory.getDataOutputStreamUsingAppend(filePath);
    } catch (Exception exception) {
      assertEquals("Not supported", exception.getMessage());
    } finally {
      if (null != outputStream) {
        outputStream.close();
      }
    }
  }

  @Test public void getDataOutputStreamForVIEWFSType() throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
    DataOutputStream outputStream = FileFactory.getDataOutputStream(filePath);
    assertNotNull(outputStream);
    outputStream.close();
  }

  @Test public void getDataOutputStreamForLocalType() throws IOException {
    DataOutputStream outputStream = FileFactory.getDataOutputStream(filePath);
    assertNotNull(outputStream);
    outputStream.close();
  }

  @Test public void testGetCarbonFile() throws IOException {
    FileFactory.getDataOutputStream(filePath);
    assertNotNull(FileFactory.getCarbonFile(filePath));
  }

  @Test public void testTruncateFile() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1572
    FileWriter writer = null;
    String path = null;
    try {
      // generate a file
      path = new File("target/truncatFile").getCanonicalPath();
      writer = new FileWriter(path);
      for (int i = 0; i < 4000; i++) {
        writer.write("test truncate file method");
      }
      writer.close();
      CarbonFile file = FileFactory.getCarbonFile(path);
      assertTrue(file.getSize() == 100000L);

      // truncate file to 4000 bytes
      FileFactory.truncateFile(
          path,
          4000);
      file = FileFactory.getCarbonFile(path);
      assertEquals(file.getSize(), 4000L);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(e.getMessage(), false);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (path != null) {
        try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
          FileFactory.deleteFile(path);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Test public void testCustomFS() throws IOException {

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3404
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CUSTOM_FILE_PROVIDER,
        "org.apache.carbondata.DummyFileProvider");

    FileFactory.setFileTypeInterface(new DefaultFileTypeProvider());

    String path = "testfs://dir1/file1";
    try {
      FileFactory.getFileType(path);
      fail("Expected validation error");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Path belongs to unsupported file system"));
    }

    try {
      FileFactory.getCarbonFile(path);
      fail("Expected validation error");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Path belongs to unsupported file system"));
    }

    // Update the conf to   TestFileProvider
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CUSTOM_FILE_PROVIDER, TestFileProvider.class.getName());
    FileFactory.setFileTypeInterface(new DefaultFileTypeProvider());

    try {
      Assert.assertSame(FileFactory.FileType.CUSTOM, FileFactory.getFileType(path));
      assertTrue(FileFactory.getCarbonFile(path) instanceof LocalCarbonFile);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unexpected error " + e);
    }

    //Reset the default configuration
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CUSTOM_FILE_PROVIDER, "");
    FileFactory.setFileTypeInterface(new DefaultFileTypeProvider());
  }

}
