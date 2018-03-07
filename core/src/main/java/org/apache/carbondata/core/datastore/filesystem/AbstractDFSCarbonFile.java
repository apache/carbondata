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
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

public abstract class AbstractDFSCarbonFile implements CarbonFile {
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
      LOGGER.debug("Exception occurred:" + e.getMessage());
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
      LOGGER.debug("Exception occurred:" + e.getMessage());
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
    FileSystem fileSystem = pt.getFileSystem(FileFactory.getConfiguration());
    FSDataOutputStream stream = null;
    if (append) {
      // append to a file only if file already exists else file not found
      // exception will be thrown by hdfs
      if (CarbonUtil.isFileExists(path)) {
        if (FileFactory.FileType.S3 == fileType) {
          DataInputStream dataInputStream = fileSystem.open(pt);
          int count = dataInputStream.available();
          // create buffer
          byte[] byteStreamBuffer = new byte[count];
          dataInputStream.read(byteStreamBuffer);
          stream = fileSystem.create(pt, true, bufferSize);
          stream.write(byteStreamBuffer);
        } else {
          stream = fileSystem.append(pt, bufferSize);
        }
      } else {
        stream = fileSystem.create(pt, true, bufferSize);
      }
    } else {
      stream = fileSystem.create(pt, true, bufferSize);
    }
    return stream;
  }

  @Override public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
      int bufferSize, Configuration hadoopConf) throws IOException {
    return getDataInputStream(path, fileType, bufferSize,
        CarbonUtil.inferCompressorFromFileName(path));
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

  @Override public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
      int bufferSize, String compressor) throws IOException {
    path = path.replace("\\", "/");
    Path pt = new Path(path);
    InputStream inputStream;
    FileSystem fs = pt.getFileSystem(FileFactory.getConfiguration());
    if (bufferSize <= 0) {
      inputStream = fs.open(pt);
    } else {
      inputStream = fs.open(pt, bufferSize);
    }

    String codecName = getCodecNameFromCompressor(compressor);
    if (!codecName.isEmpty()) {
      CompressionCodec codec = new CompressionCodecFactory(hadoopConf).getCodecByName(codecName);
      inputStream = codec.createInputStream(inputStream);
    }

    return new DataInputStream(new BufferedInputStream(inputStream));
  }

  /**
   * get codec name from user specified compressor name
   * @param compressorName user specified compressor name
   * @return name of codec
   * @throws IOException
   */
  private String getCodecNameFromCompressor(String compressorName) throws IOException {
    if (compressorName.isEmpty()) {
      return "";
    } else if ("GZIP".equalsIgnoreCase(compressorName)) {
      return GzipCodec.class.getName();
    } else if ("BZIP2".equalsIgnoreCase(compressorName)) {
      return BZip2Codec.class.getName();
    } else if ("SNAPPY".equalsIgnoreCase(compressorName)) {
      return SnappyCodec.class.getName();
    } else if ("LZ4".equalsIgnoreCase(compressorName)) {
      return Lz4Codec.class.getName();
    } else {
      throw new IOException("Unsuppotted compressor: " + compressorName);
    }
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

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, String compressor) throws IOException {
    path = path.replace("\\", "/");
    Path pt = new Path(path);
    OutputStream outputStream;
    if (bufferSize <= 0) {
      outputStream = fs.create(pt);
    } else {
      outputStream = fs.create(pt, true, bufferSize);
    }

    String codecName = getCodecNameFromCompressor(compressor);
    if (!codecName.isEmpty()) {
      CompressionCodec codec = new CompressionCodecFactory(hadoopConf).getCodecByName(codecName);
      outputStream = codec.createOutputStream(outputStream);
    }

    return new DataOutputStream(new BufferedOutputStream(outputStream));
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
      FsPermission permission) throws IOException {
    filePath = filePath.replace("\\", "/");
    Path path = new Path(filePath);
    FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
    if (fs.exists(path)) {
      return false;
    } else {
      if (permission == null) {
        permission = FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(fs.getConf()));
      }
      // Pass the permissions duringg file creation itself
      fs.create(path, permission, false, fs.getConf().getInt("io.file.buffer.size", 4096),
          fs.getDefaultReplication(path), fs.getDefaultBlockSize(path), null).close();
      return true;
    }
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
    if (fs.exists(path)) {
      return false;
    } else {
      // Pass the permissions duringg file creation itself
      fs.create(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL), false,
          fs.getConf().getInt("io.file.buffer.size", 4096), fs.getDefaultReplication(path),
          fs.getDefaultBlockSize(path), null).close();
      return true;
    }
  }

  @Override
  public CarbonFile[] listFiles() {
    FileStatus[] listStatus = null;
    try {
      if (null != fileStatus && fileStatus.isDirectory()) {
        Path path = fileStatus.getPath();
        listStatus = path.getFileSystem(FileFactory.getConfiguration()).listStatus(path);
      } else {
        return new CarbonFile[0];
      }
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return new CarbonFile[0];
    }
    return getFiles(listStatus);
  }

  @Override
  public CarbonFile[] locationAwareListFiles() throws IOException {
    if (null != fileStatus && fileStatus.isDirectory()) {
      List<FileStatus> listStatus = new ArrayList<>();
      Path path = fileStatus.getPath();
      RemoteIterator<LocatedFileStatus> iter =
          path.getFileSystem(FileFactory.getConfiguration()).listLocatedStatus(path);
      while (iter.hasNext()) {
        listStatus.add(iter.next());
      }
      return getFiles(listStatus.toArray(new FileStatus[listStatus.size()]));
    }
    return new CarbonFile[0];
  }

  /**
   * Get the CarbonFiles from filestatus array
   */
  protected abstract CarbonFile[] getFiles(FileStatus[] listStatus);

  @Override public String[] getLocations() throws IOException {
    BlockLocation[] blkLocations;
    if (fileStatus instanceof LocatedFileStatus) {
      blkLocations = ((LocatedFileStatus)fileStatus).getBlockLocations();
    } else {
      FileSystem fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
      blkLocations = fs.getFileBlockLocations(fileStatus.getPath(), 0L, fileStatus.getLen());
    }

    return blkLocations[0].getHosts();
  }
}
