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
import java.util.Objects;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.exception.CarbonFileException;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

public abstract class AbstractDFSCarbonFile implements CarbonFile {
  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(AbstractDFSCarbonFile.class.getName());
  protected FileSystem fileSystem;
  protected Configuration hadoopConf;
  protected Path path;

  AbstractDFSCarbonFile(String filePath) {
    this(filePath, FileFactory.getConfiguration());
  }

  AbstractDFSCarbonFile(String filePath, Configuration hadoopConf) {
    this(new Path(filePath), hadoopConf);
  }

  AbstractDFSCarbonFile(Path path) {
    this(path, FileFactory.getConfiguration());
  }

  AbstractDFSCarbonFile(Path path, Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
    try {
      Path newPath = new Path(path.toString().replace("\\", "/"));
      fileSystem = newPath.getFileSystem(this.hadoopConf);
      this.path = newPath;
    } catch (IOException e) {
      throw new CarbonFileException("Error while getting File System: ", e);
    }
  }

  AbstractDFSCarbonFile(FileStatus fileStatus) {
    this(fileStatus.getPath());
  }

  @Override
  public boolean createNewFile() {
    try {
      return fileSystem.createNewFile(path);
    } catch (IOException e) {
      throw new CarbonFileException("Unable to create file: " + path.toString(), e);
    }
  }

  @Override
  public String getAbsolutePath() {
    try {
      return fileSystem.getFileStatus(path).getPath().toString();
    } catch (IOException e) {
      throw new CarbonFileException("Unable to get file status: ", e);
    }
  }

  @Override
  public String getName() {
    return path.getName();
  }

  @Override
  public boolean isDirectory() {
    try {
      return fileSystem.getFileStatus(path).isDirectory();
    } catch (IOException e) {
      throw new CarbonFileException("Unable to get file status: ", e);
    }
  }

  @Override
  public CarbonFile getParentFile() {
    Path parentPath = path.getParent();
    return parentPath == null ? null : FileFactory.getCarbonFile(parentPath.toString());
  }

  @Override
  public boolean exists() {
    try {
      return fileSystem.exists(path);
    } catch (IOException e) {
      throw new CarbonFileException(e);
    }
  }

  @Override
  public String getCanonicalPath() {
    return getAbsolutePath();
  }

  @Override
  public String getPath() {
    return getAbsolutePath();
  }

  @Override
  public long getSize() {
    try {
      return fileSystem.getFileStatus(path).getLen();
    } catch (IOException e) {
      throw new CarbonFileException("Unable to get file status: ", e);
    }
  }

  @Override
  public boolean renameTo(String changetoName) {
    try {
      return fileSystem.rename(path, new Path(changetoName));
    } catch (IOException e) {
      throw new CarbonFileException("Failed to rename file: ", e);
    }
  }

  public boolean delete() {
    try {
      return fileSystem.delete(path, true);
    } catch (IOException e) {
      throw new CarbonFileException("Failed to delete file:", e);
    }
  }

  @Override
  public long getLastModifiedTime() {
    try {
      return fileSystem.getFileStatus(path).getModificationTime();
    } catch (IOException e) {
      throw new CarbonFileException("Unable to get file status: ", e);
    }
  }

  @Override
  public boolean setLastModifiedTime(long timestamp) {
    try {
      fileSystem.setTimes(path, timestamp, timestamp);
      return true;
    } catch (IOException e) {
      throw new CarbonFileException("Error while setting modified time: ", e);
    }
  }

  /**
   * This method will delete the data in file data from a given offset
   */
  @Override
  public boolean truncate(String fileName, long validDataEndOffset) {
    DataOutputStream dataOutputStream = null;
    DataInputStream dataInputStream = null;
    boolean fileTruncatedSuccessfully = false;
    // if bytes to read less than 1024 then buffer size should be equal to the given offset
    int bufferSize = validDataEndOffset > CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR ?
        CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR :
        (int) validDataEndOffset;
    // temporary file name
    String tempWriteFilePath = fileName + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;
    try {
      CarbonFile tempFile;
      // delete temporary file if it already exists at a given path
      if (FileFactory.isFileExist(tempWriteFilePath)) {
        tempFile = FileFactory.getCarbonFile(tempWriteFilePath);
        tempFile.delete();
      }
      // create new temporary file
      FileFactory.createNewFile(tempWriteFilePath);
      tempFile = FileFactory.getCarbonFile(tempWriteFilePath);
      byte[] buff = new byte[bufferSize];
      dataInputStream = FileFactory.getDataInputStream(fileName);
      // read the data
      int read = dataInputStream.read(buff, 0, buff.length);
      dataOutputStream = FileFactory.getDataOutputStream(tempWriteFilePath);
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
      LOGGER.error("Exception occurred while truncating the file " + e.getMessage(), e);
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
  @Override
  public boolean isFileModified(long fileTimeStamp, long endOffset) {
    boolean isFileModified = false;
    if (getLastModifiedTime() > fileTimeStamp || getSize() > endOffset) {
      isFileModified = true;
    }
    return isFileModified;
  }

  @Override
  public DataOutputStream getDataOutputStream(int bufferSize, boolean append)
      throws IOException {
    FSDataOutputStream stream = null;
    if (append) {
      // append to a file only if file already exists else file not found
      // exception will be thrown by hdfs
      if (CarbonUtil.isFileExists(path.toString())) {
        stream = fileSystem.append(path, bufferSize);
      } else {
        stream = fileSystem.create(path, true, bufferSize);
      }
    } else {
      stream = fileSystem.create(path, true, bufferSize);
    }
    return stream;
  }

  @Override
  public DataInputStream getDataInputStream(int bufferSize) throws IOException {
    return getDataInputStream(bufferSize,
        CarbonUtil.inferCompressorFromFileName(path.toString()));
  }

  /**
   * return the datainputStream which is seek to the offset of file
   *
   * @return DataInputStream
   * @throws IOException
   */
  @Override
  public DataInputStream getDataInputStream(int bufferSize, long offset)
      throws IOException {
    FSDataInputStream stream = fileSystem.open(path, bufferSize);
    stream.seek(offset);
    return new DataInputStream(new BufferedInputStream(stream));
  }

  @Override
  public DataInputStream getDataInputStream(int bufferSize, String compressor)
      throws IOException {
    InputStream inputStream;
    if (bufferSize <= 0) {
      inputStream = fileSystem.open(path);
    } else {
      inputStream = fileSystem.open(path, bufferSize);
    }
    String codecName = getCodecNameFromCompressor(compressor);
    if (!codecName.isEmpty()) {
      CompressionCodec codec = new CompressionCodecFactory(hadoopConf).getCodecByName(codecName);
      inputStream = codec.createInputStream(inputStream);
    }
    if (bufferSize <= 0 && inputStream instanceof FSDataInputStream) {
      return (DataInputStream) inputStream;
    } else {
      return new DataInputStream(new BufferedInputStream(inputStream));
    }
  }

  /**
   * get codec name from user specified compressor name
   *
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

  @Override
  public DataOutputStream getDataOutputStream() throws IOException {
    return fileSystem.create(path, true);
  }

  @Override
  public DataOutputStream getDataOutputStream(int bufferSize, long blockSize)
      throws IOException {
    short replication = fileSystem.getDefaultReplication(path);
    return getDataOutputStream(bufferSize, blockSize, replication);
  }

  @Override
  public DataOutputStream getDataOutputStream(int bufferSize, long blockSize, short replication)
      throws IOException {
    return fileSystem.create(path, true, bufferSize, replication, blockSize);
  }

  @Override
  public DataOutputStream getDataOutputStream(int bufferSize, String compressor)
      throws IOException {
    OutputStream outputStream;
    if (bufferSize <= 0) {
      outputStream = fileSystem.create(path);
    } else {
      outputStream = fileSystem.create(path, true, bufferSize);
    }
    String codecName = getCodecNameFromCompressor(compressor);
    if (!codecName.isEmpty()) {
      CompressionCodec codec = new CompressionCodecFactory(hadoopConf).getCodecByName(codecName);
      outputStream = codec.createOutputStream(outputStream);
    }
    return new DataOutputStream(new BufferedOutputStream(outputStream));
  }

  @Override
  public boolean isFileExist(boolean performFileCheck) throws IOException {
    if (performFileCheck) {
      return fileSystem.exists(path) && fileSystem.isFile(path);
    } else {
      return fileSystem.exists(path);
    }
  }

  @Override
  public boolean isFileExist() throws IOException {
    return isFileExist(false);
  }

  @Override
  public boolean createNewFile(FsPermission permission) throws IOException {
    if (fileSystem.exists(path)) {
      return false;
    } else {
      if (permission == null) {
        permission =
            FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(fileSystem.getConf()));
      }
      // Pass the permissions duringg file creation itself
      fileSystem
          .create(path, permission, false, fileSystem.getConf().getInt("io.file.buffer.size", 4096),
              fileSystem.getDefaultReplication(path), fileSystem.getDefaultBlockSize(path), null)
          .close();
      return true;
    }
  }

  @Override
  public boolean deleteFile() throws IOException {
    return fileSystem.delete(path, true);
  }

  @Override
  public boolean mkdirs() throws IOException {
    return fileSystem.mkdirs(path);
  }

  @Override
  public DataOutputStream getDataOutputStreamUsingAppend() throws IOException {
    return fileSystem.append(path);
  }

  @Override
  public boolean createNewLockFile() throws IOException {
    if (fileSystem.exists(path)) {
      return false;
    } else {
      // Pass the permissions duringg file creation itself
      fileSystem.create(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL), false,
          fileSystem.getConf().getInt("io.file.buffer.size", 4096),
          fileSystem.getDefaultReplication(path), fileSystem.getDefaultBlockSize(path), null)
          .close();
      // haddop masks the permission accoding to configured permission, so need to set permission
      // forcefully
      fileSystem.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      return true;
    }
  }

  @Override
  public CarbonFile[] listFiles() {
    FileStatus[] listStatus;
    try {
      listStatus = fileSystem.listStatus(path);
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage(), e);
      return new CarbonFile[0];
    }
    return getFiles(listStatus);
  }

  /**
   * Get the CarbonFiles from filestatus array
   */
  protected abstract CarbonFile[] getFiles(FileStatus[] listStatus);

  @Override
  public List<CarbonFile> listFiles(Boolean recursive) throws IOException {
    RemoteIterator<LocatedFileStatus> listStatus = fileSystem.listFiles(path, recursive);
    return getFiles(listStatus);
  }

  @Override
  public CarbonFile[] locationAwareListFiles(PathFilter pathFilter) throws IOException {
    List<FileStatus> listStatus = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> iter = fileSystem.listLocatedStatus(path);
    while (iter.hasNext()) {
      LocatedFileStatus fileStatus = iter.next();
      if (pathFilter.accept(fileStatus.getPath()) && fileStatus.getLen() > 0) {
        listStatus.add(fileStatus);
      }
    }
    return getFiles(listStatus.toArray(new FileStatus[listStatus.size()]));
  }

  protected List<CarbonFile> getFiles(RemoteIterator<LocatedFileStatus> listStatus)
      throws IOException {
    List<CarbonFile> carbonFiles = new ArrayList<>();
    while (listStatus.hasNext()) {
      Path filePath = listStatus.next().getPath();
      carbonFiles.add(FileFactory.getCarbonFile(filePath.toString()));
    }
    return carbonFiles;
  }

  /**
   * Method used to list files recursively and apply file filter on the result.
   *
   */
  @Override
  public List<CarbonFile> listFiles(boolean recursive, CarbonFileFilter fileFilter)
      throws IOException {
    List<CarbonFile> carbonFiles = new ArrayList<>();
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    if (null != fileStatus && fileStatus.isDirectory()) {
      RemoteIterator<LocatedFileStatus> listStatus = fileSystem.listFiles(fileStatus.getPath(),
          recursive);
      while (listStatus.hasNext()) {
        LocatedFileStatus locatedFileStatus = listStatus.next();
        CarbonFile carbonFile = FileFactory.getCarbonFile(locatedFileStatus.getPath().toString());
        if (fileFilter.accept(carbonFile)) {
          carbonFiles.add(carbonFile);
        }
      }
    }
    return carbonFiles;
  }

  @Override
  public CarbonFile[] listFiles(boolean recursive, int maxCount)
      throws IOException {
    List<CarbonFile> carbonFiles = new ArrayList<>();
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    if (null != fileStatus && fileStatus.isDirectory()) {
      RemoteIterator<LocatedFileStatus> listStatus = fileSystem.listFiles(path, recursive);
      int counter = 0;
      while (counter < maxCount && listStatus.hasNext()) {
        LocatedFileStatus locatedFileStatus = listStatus.next();
        CarbonFile carbonFile = FileFactory.getCarbonFile(locatedFileStatus.getPath().toString());
        carbonFiles.add(carbonFile);
        counter++;
      }
    }
    return carbonFiles.toArray(new CarbonFile[0]);
  }

  @Override
  public String[] getLocations() throws IOException {
    BlockLocation[] blkLocations;
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    if (fileStatus instanceof LocatedFileStatus) {
      blkLocations = ((LocatedFileStatus) fileStatus).getBlockLocations();
    } else {
      blkLocations =
          fileSystem.getFileBlockLocations(fileStatus.getPath(), 0L, fileStatus.getLen());
    }
    return blkLocations[0].getHosts();
  }

  @Override
  public boolean setReplication(short replication) throws IOException {
    return fileSystem.setReplication(path, replication);
  }

  @Override
  public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
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

  @Override
  public short getDefaultReplication() {
    return fileSystem.getDefaultReplication(path);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractDFSCarbonFile that = (AbstractDFSCarbonFile) o;
    if (path == null || that.path == null) {
      return false;
    }
    return path.equals(that.path);
  }

  @Override
  public int hashCode() {
    if (path == null) {
      return 0;
    }
    return Objects.hash(path);
  }

  @Override
  public long getLength() throws IOException {
    return fileSystem.getFileStatus(path).getLen();
  }
}
