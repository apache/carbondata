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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.CarbonS3FileSystem;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class S3CarbonFile implements CarbonFile
{

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(S3CarbonFile.class.getName());

  protected FileStatus fileStatus;

  private FileSystem fs;


  public S3CarbonFile(String filePath) {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    fs = new CarbonS3FileSystem();
    try {
      fs.initialize(path.toUri(), FileFactory.getConfiguration());
      fileStatus = fs.getFileStatus(path);
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
    }

  }

  public S3CarbonFile(Path path) {
    fs = new CarbonS3FileSystem();
    try {
      fs.initialize(path.toUri(), FileFactory.getConfiguration());
      fileStatus = fs.getFileStatus(path);
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
    }
  }

  public S3CarbonFile(FileStatus fileStatus) {
    fs = new CarbonS3FileSystem();
    try {
      fs.initialize(fileStatus.getPath().toUri(), FileFactory.getConfiguration());
      this.fileStatus = fs.getFileStatus(fileStatus.getPath());
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
    }
  }

  /**
   * @param listStatus
   * @return
   */
  private CarbonFile[] getFiles(FileStatus[] listStatus) {
    if (listStatus == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] files = new CarbonFile[listStatus.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new S3CarbonFile(listStatus[i]);
    }
    return files;
  }

  @Override public String getAbsolutePath() {
    return fileStatus.getPath().toString();
  }

  @Override public CarbonFile[] listFiles(CarbonFileFilter fileFilter) {
    CarbonFile[] files = listFiles();
    if (files != null && files.length >= 1) {
      List<CarbonFile> fileList = new ArrayList<CarbonFile>(files.length);
      for (int i = 0; i < files.length; i++) {
        if (fileFilter.accept(files[i])) {
          fileList.add(files[i]);
        }
      }
      if (fileList.size() >= 1) {
        return fileList.toArray(new CarbonFile[fileList.size()]);
      } else {
        return new CarbonFile[0];
      }
    }
    return files;
  }

  @Override public CarbonFile[] listFiles() {
    FileStatus[] listStatus = null;
    try {
      if (null != fileStatus && fileStatus.isDirectory()) {
        Path path = fileStatus.getPath();
        listStatus = fs.listStatus(path);
      } else {
        return new CarbonFile[0];
      }
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return new CarbonFile[0];
    }
    return getFiles(listStatus);
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
        return fs.exists(fileStatus.getPath());
      }
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
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

  @Override public CarbonFile getParentFile() {
    Path parent = fileStatus.getPath().getParent();
    return null == parent ? null : new S3CarbonFile(parent);
  }

  @Override public boolean renameTo(String changetoName) {
    try {
      return fs.rename(fileStatus.getPath(), new Path(changetoName));
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
      return false;
    }
  }

  @Override public boolean renameForce(String changetoName) {
    try {
      fs.rename(fileStatus.getPath(), new Path(changetoName));
      return true;
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }

  @Override public boolean delete() {
    try {
      return fs.delete(fileStatus.getPath(), true);
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
      return false;
    }
  }

  @Override public boolean createNewFile() {
    Path path = fileStatus.getPath();
    try {
      return fs.createNewFile(path);
    } catch (IOException e) {
      return false;
    }
  }

  @Override public boolean setLastModifiedTime(long timestamp) {
    try {
      fs.setTimes(fileStatus.getPath(), timestamp, timestamp);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

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
      CarbonFile tempFile;
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
      LOGGER.error("Exception occurred while truncating the file " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(dataOutputStream, dataInputStream);
    }
    return fileTruncatedSuccessfully;
  }

  @Override public boolean isFileModified(long fileTimeStamp, long endOffset) {
    boolean isFileModified = false;
    if (getLastModifiedTime() > fileTimeStamp || getSize() > endOffset) {
      isFileModified = true;
    }
    return isFileModified;
  }

  @Override public long getLastModifiedTime() {
    return fileStatus.getModificationTime();
  }
}
