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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class S3CarbonFile extends HDFSCarbonFile {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(HDFSCarbonFile.class.getName());

  public S3CarbonFile(String filePath) {
    super(filePath);
  }

  public S3CarbonFile(String filePath, Configuration hadoopConf) {
    super(filePath, hadoopConf);
  }

  public S3CarbonFile(Path path, Configuration hadoopConf) {
    super(path, hadoopConf);
  }

  /**
   The current implementaion of rename force for S3 is to read the contents of source and write
   as the destination file. This is done to avoid the overhead of deleting and recreating the file.
   */
  @Override
  public boolean renameForce(String changeToName) {
    DataInputStream dataInputStream = null;
    DataOutputStream dataOutputStream = null;
    try {
      String sourcePath = getAbsolutePath();
      dataInputStream = FileFactory.getDataInputStream(sourcePath,
          FileFactory.getFileType(sourcePath));
      dataOutputStream = getDataOutputStream(changeToName,
          FileFactory.getFileType(changeToName), CarbonCommonConstants.BYTEBUFFER_SIZE, false);
      byte[] byteArray = new byte[dataInputStream.available()];
      dataInputStream.readFully(byteArray);
      dataOutputStream.write(byteArray);
      return true;
    } catch (IOException e) {
      LOGGER.error("Exception occured:", e);
      return false;
    } finally {
      try {
        if (dataInputStream != null) {
          dataInputStream.close();
        }
        if (dataOutputStream != null) {
          dataOutputStream.close();
        }
        // delete the source file
        delete();
      } catch (IOException e) {
        LOGGER.error("Exception occured while closing streams:", e);
      }
    }
  }

  /**
   * @param listStatus
   * @return
   */
  @Override
  protected CarbonFile[] getFiles(FileStatus[] listStatus) {
    if (listStatus == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] files = new CarbonFile[listStatus.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new HDFSCarbonFile(listStatus[i]);
    }
    return files;
  }

  @Override
  public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType)
      throws IOException {
    return getDataOutputStream(path, fileType, CarbonCommonConstants.BYTEBUFFER_SIZE, true);
  }

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, boolean append) throws IOException {
    Path pt = new Path(path);
    FileSystem fileSystem = pt.getFileSystem(FileFactory.getConfiguration());
    FSDataOutputStream stream;
    if (append) {
      // append to a file only if file already exists else file not found
      // exception will be thrown by hdfs
      if (CarbonUtil.isFileExists(path)) {
        DataInputStream dataInputStream = fileSystem.open(pt);
        int count = dataInputStream.available();
        // create buffer
        byte[] byteStreamBuffer = new byte[count];
        int bytesRead = dataInputStream.read(byteStreamBuffer);
        dataInputStream.close();
        stream = fileSystem.create(pt, true, bufferSize);
        if (bytesRead > 0) {
          stream.write(byteStreamBuffer, 0, bytesRead);
        }
      } else {
        stream = fileSystem.create(pt, true, bufferSize);
      }
    } else {
      stream = fileSystem.create(pt, true, bufferSize);
    }
    return stream;
  }

  @Override
  public CarbonFile getParentFile() {
    Path parent = fileStatus.getPath().getParent();
    return null == parent ? null : new S3CarbonFile(parent, hadoopConf);
  }

}
