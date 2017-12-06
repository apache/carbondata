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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;

public abstract  class AbstractDFSCarbonFile implements CarbonFile {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractDFSCarbonFile.class.getName());
  protected FileStatus fileStatus;
  public FileSystem fs;
  protected Configuration hadoopConf;

  public AbstractDFSCarbonFile(String filePath) {
    this(filePath, FileFactory.getConfiguration());
  }

  public AbstractDFSCarbonFile(String filePath, Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    try {
      fs = path.getFileSystem(this.hadoopConf);
      fileStatus = fs.getFileStatus(path);
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
    }
  }

  public AbstractDFSCarbonFile(Path path) {
    this(path, FileFactory.getConfiguration());
  }

  public AbstractDFSCarbonFile(Path path, Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
    FileSystem fs;
    try {
      fs = path.getFileSystem(this.hadoopConf);
      fileStatus = fs.getFileStatus(path);
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
    }
  }

  public AbstractDFSCarbonFile(FileStatus fileStatus) {
    this.hadoopConf = FileFactory.getConfiguration();
    this.fileStatus = fileStatus;
  }

  @Override public boolean createNewFile() {
    Path path = fileStatus.getPath();
    FileSystem fs;
    try {
      fs = fileStatus.getPath().getFileSystem(hadoopConf);
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
    FileSystem fs;
    try {
      if (null != fileStatus) {
        fs = fileStatus.getPath().getFileSystem(hadoopConf);
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

  public boolean renameTo(String changetoName) {
    FileSystem fs;
    try {
      if (null != fileStatus) {
        fs = fileStatus.getPath().getFileSystem(hadoopConf);
        return fs.rename(fileStatus.getPath(), new Path(changetoName));
      }
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
      return false;
    }
    return false;
  }

  public boolean delete() {
    FileSystem fs;
    try {
      if (null != fileStatus) {
        fs = fileStatus.getPath().getFileSystem(hadoopConf);
        return fs.delete(fileStatus.getPath(), true);
      }
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage());
      return false;
    }
    return false;
  }

  @Override public long getLastModifiedTime() {
    return fileStatus.getModificationTime();
  }

  @Override public boolean setLastModifiedTime(long timestamp) {
    FileSystem fs;
    try {
      if (null != fileStatus) {
        fs = fileStatus.getPath().getFileSystem(hadoopConf);
        fs.setTimes(fileStatus.getPath(), timestamp, timestamp);
      }
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

  /**
   * This method will be used to check whether a file has been modified or not
   *
   * @param fileTimeStamp time to be compared with latest timestamp of file
   * @param endOffset     file length to be compared with current length of file
   * @return whether a file has been modified or not
   */
  @Override public boolean isFileModified(long fileTimeStamp, long endOffset) {
    boolean isFileModified = false;
    if (getLastModifiedTime() > fileTimeStamp || getSize() > endOffset) {
      isFileModified = true;
    }
    return isFileModified;
  }

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, boolean append) throws IOException {
    Path pt = new Path(path);
    FileSystem fs = pt.getFileSystem(FileFactory.getConfiguration());
    FSDataOutputStream stream = null;
    if (append) {
      // append to a file only if file already exists else file not found
      // exception will be thrown by hdfs
      if (CarbonUtil.isFileExists(path)) {
        stream = fs.append(pt, bufferSize);
      } else {
        stream = fs.create(pt, true, bufferSize);
      }
    } else {
      stream = fs.create(pt, true, bufferSize);
    }
    return stream;
  }

  @Override public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
      int bufferSize, Configuration hadoopConf) throws IOException {
    path = path.replace("\\", "/");
    boolean gzip = path.endsWith(".gz");
    boolean bzip2 = path.endsWith(".bz2");
    InputStream stream;
    Path pt = new Path(path);
    FileSystem fs = pt.getFileSystem(hadoopConf);
    if (bufferSize == -1) {
      stream = fs.open(pt);
    } else {
      stream = fs.open(pt, bufferSize);
    }
    String codecName = null;
    if (gzip) {
      codecName = GzipCodec.class.getName();
    } else if (bzip2) {
      codecName = BZip2Codec.class.getName();
    }
    if (null != codecName) {
      CompressionCodecFactory ccf = new CompressionCodecFactory(hadoopConf);
      CompressionCodec codec = ccf.getCodecByClassName(codecName);
      stream = codec.createInputStream(stream);
    }
    return new DataInputStream(new BufferedInputStream(stream));
  }

  /**
   * return the datainputStream which is seek to the offset of file
   *
   * @param path
   * @param fileType
   * @param bufferSize
   * @param offset
   * @return DataInputStream
   * @throws IOException
   */
  @Override public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
      int bufferSize, long offset) throws IOException {
    path = path.replace("\\", "/");
    Path pt = new Path(path);
    FileSystem fs = pt.getFileSystem(FileFactory.getConfiguration());
    FSDataInputStream stream = fs.open(pt, bufferSize);
    stream.seek(offset);
    return new DataInputStream(new BufferedInputStream(stream));
  }

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType)
      throws IOException {
    path = path.replace("\\", "/");
    Path pt = new Path(path);
    FileSystem fs = pt.getFileSystem(FileFactory.getConfiguration());
    return fs.create(pt, true);
  }

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, long blockSize) throws IOException {
    path = path.replace("\\", "/");
    Path pt = new Path(path);
    FileSystem fs = pt.getFileSystem(FileFactory.getConfiguration());
    return fs.create(pt, true, bufferSize, fs.getDefaultReplication(pt), blockSize);
  }

  @Override public boolean isFileExist(String filePath, FileFactory.FileType fileType,
      boolean performFileCheck) throws IOException {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    if (performFileCheck) {
      return fs.exists(path) && fs.isFile(path);
    } else {
      return fs.exists(path);
    }
  }

  @Override public boolean isFileExist(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    return fs.exists(path);
  }

  @Override public boolean createNewFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    return fs.createNewFile(path);
  }

  @Override
  public boolean createNewFile(String filePath, FileFactory.FileType fileType, boolean doAs,
      final FsPermission permission) throws IOException {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    boolean result = fs.createNewFile(path);
    if (null != permission) {
      fs.setPermission(path, permission);
    }
    return result;
  }

  @Override public boolean deleteFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    return fs.delete(path, true);
  }

  @Override public boolean mkdirs(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    return fs.mkdirs(path);
  }

  @Override
  public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType)
      throws IOException {
    path = path.replace("\\", "/");
    Path pt = new Path(path);
    FileSystem fs = pt.getFileSystem(FileFactory.getConfiguration());
    return fs.append(pt);
  }

  @Override public boolean createNewLockFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    if (fs.createNewFile(path)) {
      fs.deleteOnExit(path);
      return true;
    }
    return false;
  }

  @Override
  public void setPermission(String directoryPath, FsPermission permission, String username,
      String group) throws IOException {
  }
}
