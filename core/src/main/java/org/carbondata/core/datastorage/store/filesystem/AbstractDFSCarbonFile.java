/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.datastorage.store.filesystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract  class AbstractDFSCarbonFile implements CarbonFile {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractDFSCarbonFile.class.getName());
  protected FileStatus fileStatus;
  protected FileSystem fs;

  public AbstractDFSCarbonFile(String filePath) {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    try {
      fs = path.getFileSystem(FileFactory.getConfiguration());
      fileStatus = fs.getFileStatus(path);
    } catch (IOException e) {
      LOGGER.error("Exception occured:" + e.getMessage());
    }
  }

  public AbstractDFSCarbonFile(Path path) {
    try {
      fs = path.getFileSystem(FileFactory.getConfiguration());
      fileStatus = fs.getFileStatus(path);
    } catch (IOException e) {
      LOGGER.error("Exception occured:" + e.getMessage());
    }
  }

  public AbstractDFSCarbonFile(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
  }

  @Override public boolean createNewFile() {
    Path path = fileStatus.getPath();
    try {
      return fs.createNewFile(path);
    } catch (IOException e) {
      return false;
    }
  }

  @Override public String getAbsolutePath() {
    return fileStatus.getPath().toString();
  }

  @Override public String getName() {
    return fileStatus.getPath().getName();
  }

  @Override public boolean isDirectory() {
    return fileStatus.isDirectory();
  }

  @Override public boolean exists() {
    try {
      if (null != fileStatus) {
        fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
        return fs.exists(fileStatus.getPath());
      }
    } catch (IOException e) {
      LOGGER.error("Exception occured:" + e.getMessage());
    }
    return false;
  }

  @Override public String getCanonicalPath() {
    return getAbsolutePath();
  }

  @Override public String getPath() {
    return getAbsolutePath();
  }

  @Override public long getSize() {
    return fileStatus.getLen();
  }

  public boolean renameTo(String changetoName) {
    FileSystem fs;
    try {
      fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
      return fs.rename(fileStatus.getPath(), new Path(changetoName));
    } catch (IOException e) {
      LOGGER.error("Exception occured:" + e.getMessage());
      return false;
    }
  }

  public boolean delete() {
    FileSystem fs;
    try {
      fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
      return fs.delete(fileStatus.getPath(), true);
    } catch (IOException e) {
      LOGGER.error("Exception occured:" + e.getMessage());
      return false;
    }
  }

  @Override public long getLastModifiedTime() {
    return fileStatus.getModificationTime();
  }

  @Override public boolean setLastModifiedTime(long timestamp) {
    try {
      fs.setTimes(fileStatus.getPath(), timestamp, timestamp);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * This method will delete the data in file data from a given offset
   */
  @Override public boolean truncate(String fileName, long validDataEndOffset) {
    DataOutputStream dataOutputStream = null;
    DataInputStream dataInputStream = null;
    boolean fileTruncatedSuccessfully = false;
    // if bytes to read less than 1024 then buffer size should be equal to the given offset
    int bufferSize = validDataEndOffset > CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR ?
        CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR :
        (int) validDataEndOffset;
    // temporary file name
    String tempWriteFilePath = fileName + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;
    FileFactory.FileType fileType = FileFactory.getFileType(fileName);
    try {
      CarbonFile tempFile = null;
      // delete temporary file if it already exists at a given path
      if (FileFactory.isFileExist(tempWriteFilePath, fileType)) {
        tempFile = FileFactory.getCarbonFile(tempWriteFilePath, fileType);
        tempFile.delete();
      }
      // create new temporary file
      FileFactory.createNewFile(tempWriteFilePath, fileType);
      tempFile = FileFactory.getCarbonFile(tempWriteFilePath, fileType);
      byte[] buff = new byte[bufferSize];
      dataInputStream = FileFactory.getDataInputStream(fileName, fileType);
      // read the data
      int read = dataInputStream.read(buff, 0, buff.length);
      dataOutputStream = FileFactory.getDataOutputStream(tempWriteFilePath, fileType);
      dataOutputStream.write(buff, 0, read);
      long remaining = validDataEndOffset - read;
      // anytime we should not cross the offset to be read
      while (remaining > 0) {
        if (remaining > bufferSize) {
          buff = new byte[bufferSize];
        } else {
          buff = new byte[(int) remaining];
        }
        read = dataInputStream.read(buff, 0, buff.length);
        dataOutputStream.write(buff, 0, read);
        remaining = remaining - read;
      }
      CarbonUtil.closeStreams(dataInputStream, dataOutputStream);
      // rename the temp file to original file
      tempFile.renameForce(fileName);
      fileTruncatedSuccessfully = true;
    } catch (IOException e) {
      LOGGER.error("Exception occured while truncating the file " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(dataOutputStream, dataInputStream);
    }
    return fileTruncatedSuccessfully;
  }

  /**
   * This method will be used to check whether a file has been modified or not
   *
   * @param fileTimeStamp time to be compared with latest timestamp of file
   * @param endOffset     file length to be compared with current length of file
   * @return
   */
  @Override public boolean isFileModified(long fileTimeStamp, long endOffset) {
    boolean isFileModified = false;
    if (getLastModifiedTime() > fileTimeStamp || getSize() > endOffset) {
      isFileModified = true;
    }
    return isFileModified;
  }
}
