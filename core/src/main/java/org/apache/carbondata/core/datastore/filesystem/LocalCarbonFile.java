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
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class LocalCarbonFile implements CarbonFile {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LocalCarbonFile.class.getName());
  private File file;

  public LocalCarbonFile(String filePath) {
    Path pathWithoutSchemeAndAuthority = Path.getPathWithoutSchemeAndAuthority(new Path(filePath));
    file = new File(pathWithoutSchemeAndAuthority.toString());
  }

  public LocalCarbonFile(File file) {
    this.file = file;
  }

  @Override public String getAbsolutePath() {
    return file.getAbsolutePath();
  }

  @Override public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
    if (!file.isDirectory()) {
      return new CarbonFile[0];
    }

    File[] files = file.listFiles(new FileFilter() {

      @Override public boolean accept(File pathname) {
        return fileFilter.accept(new LocalCarbonFile(pathname));
      }
    });

    if (files == null) {
      return new CarbonFile[0];
    }

    CarbonFile[] carbonFiles = new CarbonFile[files.length];

    for (int i = 0; i < carbonFiles.length; i++) {
      carbonFiles[i] = new LocalCarbonFile(files[i]);
    }

    return carbonFiles;
  }

  @Override public String getName() {
    return file.getName();
  }

  @Override public boolean isDirectory() {
    return file.isDirectory();
  }

  @Override public boolean exists() {
    if (file != null) {
      return file.exists();
    }
    return false;
  }

  @Override public String getCanonicalPath() {
    try {
      return file.getCanonicalPath();
    } catch (IOException e) {
      LOGGER.error("Exception occured" + e.getMessage(), e);
    }
    return null;
  }

  @Override public CarbonFile getParentFile() {
    return new LocalCarbonFile(file.getParentFile());
  }

  @Override public String getPath() {
    return file.getPath();
  }

  @Override public long getSize() {
    return file.length();
  }

  public boolean renameTo(String changeToName) {
    changeToName = FileFactory.getUpdatedFilePath(changeToName, FileFactory.FileType.LOCAL);
    return file.renameTo(new File(changeToName));
  }

  public boolean delete() {
    try {
      return deleteFile(file.getAbsolutePath(), FileFactory.getFileType(file.getAbsolutePath()));
    } catch (IOException e) {
      LOGGER.error("Exception occurred:" + e.getMessage(), e);
      return false;
    }
  }

  @Override public CarbonFile[] listFiles() {

    if (!file.isDirectory()) {
      return new CarbonFile[0];
    }
    File[] files = file.listFiles();
    if (files == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] carbonFiles = new CarbonFile[files.length];
    for (int i = 0; i < carbonFiles.length; i++) {
      carbonFiles[i] = new LocalCarbonFile(files[i]);
    }

    return carbonFiles;

  }

  @Override
  public List<CarbonFile> listFiles(Boolean recursive) {
    if (!file.isDirectory()) {
      return new ArrayList<CarbonFile>();
    }
    Collection<File> fileCollection = FileUtils.listFiles(file, null, true);
    if (fileCollection == null) {
      return new ArrayList<CarbonFile>();
    }
    List<CarbonFile> carbonFiles = new ArrayList<CarbonFile>();
    for (File file : fileCollection) {
      carbonFiles.add(new LocalCarbonFile(file));
    }
    return carbonFiles;
  }

  @Override public List<CarbonFile> listFiles(boolean recursive, CarbonFileFilter fileFilter)
      throws IOException {
    if (!file.isDirectory()) {
      return new ArrayList<CarbonFile>();
    }
    Collection<File> fileCollection = FileUtils.listFiles(file, null, recursive);
    if (fileCollection == null) {
      return new ArrayList<CarbonFile>();
    }
    List<CarbonFile> carbonFiles = new ArrayList<CarbonFile>();
    for (File file : fileCollection) {
      CarbonFile carbonFile = new LocalCarbonFile(file);
      if (fileFilter.accept(carbonFile)) {
        carbonFiles.add(carbonFile);
      }
    }
    return carbonFiles;
  }

  @Override public boolean createNewFile() {
    try {
      return file.createNewFile();
    } catch (IOException e) {
      return false;
    }
  }

  @Override public long getLastModifiedTime() {
    return file.lastModified();
  }

  @Override public boolean setLastModifiedTime(long timestamp) {
    return file.setLastModified(timestamp);
  }

  /**
   * This method will delete the data in file data from a given offset
   */
  @Override public boolean truncate(String fileName, long validDataEndOffset) {
    FileChannel source = null;
    FileChannel destination = null;
    boolean fileTruncatedSuccessfully = false;
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
      source = new FileInputStream(fileName).getChannel();
      destination = new FileOutputStream(tempWriteFilePath).getChannel();
      long read = destination.transferFrom(source, 0, validDataEndOffset);
      long totalBytesRead = read;
      long remaining = validDataEndOffset - totalBytesRead;
      // read till required data offset is not reached
      while (remaining > 0) {
        read = destination.transferFrom(source, totalBytesRead, remaining);
        totalBytesRead = totalBytesRead + read;
        remaining = remaining - totalBytesRead;
      }
      CarbonUtil.closeStreams(source, destination);
      // rename the temp file to original file
      tempFile.renameForce(fileName);
      fileTruncatedSuccessfully = true;
    } catch (IOException e) {
      LOGGER.error("Exception occured while truncating the file " + e.getMessage(), e);
    } finally {
      CarbonUtil.closeStreams(source, destination);
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


  @Override public boolean renameForce(String changeToName) {
    File destFile = new File(changeToName);
    if (destFile.exists() && !file.getAbsolutePath().equals(destFile.getAbsolutePath())) {
      if (destFile.delete()) {
        return file.renameTo(new File(changeToName));
      }
    }
    return file.renameTo(new File(changeToName));
  }

  @Override public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, boolean append) throws FileNotFoundException {
    path = FileFactory.getUpdatedFilePath(path, FileFactory.FileType.LOCAL);
    return new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(path, append), bufferSize));
  }

  @Override public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
      int bufferSize, Configuration configuration) throws IOException {
    return getDataInputStream(path, fileType, bufferSize,
        CarbonUtil.inferCompressorFromFileName(path));
  }

  @Override public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType,
      int bufferSize, String compressor) throws IOException {
    path = path.replace("\\", "/");
    path = FileFactory.getUpdatedFilePath(path, fileType);
    InputStream inputStream;
    if (compressor.isEmpty()) {
      inputStream = new FileInputStream(path);
    } else if ("GZIP".equalsIgnoreCase(compressor)) {
      inputStream = new GZIPInputStream(new FileInputStream(path));
    } else if ("BZIP2".equalsIgnoreCase(compressor)) {
      inputStream = new BZip2CompressorInputStream(new FileInputStream(path));
    } else if ("SNAPPY".equalsIgnoreCase(compressor)) {
      inputStream = new SnappyInputStream(new FileInputStream(path));
    } else if ("LZ4".equalsIgnoreCase(compressor)) {
      inputStream = new LZ4BlockInputStream(new FileInputStream(path));
    } else if ("ZSTD".equalsIgnoreCase(compressor)) {
      inputStream = new ZstdInputStream(new FileInputStream(path));
    } else {
      throw new IOException("Unsupported compressor: " + compressor);
    }

    if (bufferSize <= 0) {
      return new DataInputStream(new BufferedInputStream(inputStream));
    } else {
      return new DataInputStream(new BufferedInputStream(inputStream, bufferSize));
    }
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
    path = FileFactory.getUpdatedFilePath(path, fileType);
    FileInputStream fis = new FileInputStream(path);
    long actualSkipSize = 0;
    long skipSize = offset;
    try {
      while (actualSkipSize != offset) {
        actualSkipSize += fis.skip(skipSize);
        skipSize = skipSize - actualSkipSize;
      }
    } catch (IOException ioe) {
      CarbonUtil.closeStream(fis);
      throw ioe;
    }
    return new DataInputStream(new BufferedInputStream(fis));
  }

  @Override
  public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType)
      throws IOException {
    path = path.replace("\\", "/");
    path = FileFactory.getUpdatedFilePath(path, FileFactory.FileType.LOCAL);
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
  }

  @Override
  public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, long blockSize) throws IOException {
    return getDataOutputStream(path, fileType, bufferSize, blockSize, (short) 1);
  }

  @Override
  public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, long blockSize, short replication) throws IOException {
    path = path.replace("\\", "/");
    path = FileFactory.getUpdatedFilePath(path, fileType);
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path), bufferSize));
  }

  @Override
  public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
      int bufferSize, String compressor) throws IOException {
    path = path.replace("\\", "/");
    path = FileFactory.getUpdatedFilePath(path, fileType);
    OutputStream outputStream;
    if (compressor.isEmpty()) {
      outputStream = new FileOutputStream(path);
    } else if ("GZIP".equalsIgnoreCase(compressor)) {
      outputStream = new GZIPOutputStream(new FileOutputStream(path));
    } else if ("BZIP2".equalsIgnoreCase(compressor)) {
      outputStream = new BZip2CompressorOutputStream(new FileOutputStream(path));
    } else if ("SNAPPY".equalsIgnoreCase(compressor)) {
      outputStream = new SnappyOutputStream(new FileOutputStream(path));
    } else if ("LZ4".equalsIgnoreCase(compressor)) {
      outputStream = new LZ4BlockOutputStream(new FileOutputStream(path));
    } else if ("ZSTD".equalsIgnoreCase(compressor)) {
      // compression level 1 is cost-effective for sort temp file
      // which is not used for storage
      outputStream = new ZstdOutputStream(new FileOutputStream(path), 1);
    } else {
      throw new IOException("Unsupported compressor: " + compressor);
    }

    if (bufferSize <= 0) {
      return new DataOutputStream(new BufferedOutputStream(outputStream));
    } else {
      return new DataOutputStream(new BufferedOutputStream(outputStream, bufferSize));
    }
  }

  @Override public boolean isFileExist(String filePath, boolean performFileCheck)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath);
    File defaultFile = new File(filePath);

    if (performFileCheck) {
      return defaultFile.exists() && defaultFile.isFile();
    } else {
      return defaultFile.exists();
    }
  }

  @Override public boolean isFileExist(String filePath)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath);
    File defaultFile = new File(filePath);
    return defaultFile.exists();
  }

  @Override public boolean createNewFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return file.createNewFile();
  }

  @Override
  public boolean createNewFile(String filePath, FileFactory.FileType fileType, boolean doAs,
      final FsPermission permission) throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return file.createNewFile();
  }

  @Override public boolean deleteFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return FileFactory.deleteAllFilesOfDir(file);
  }

  @Override public boolean mkdirs(String filePath)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath);
    File file = new File(filePath);
    return file.mkdirs();
  }

  @Override
  public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType)
      throws IOException {
    path = path.replace("\\", "/");
    path = FileFactory.getUpdatedFilePath(path, fileType);
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path, true)));
  }

  @Override public boolean createNewLockFile(String filePath, FileFactory.FileType fileType)
      throws IOException {
    filePath = filePath.replace("\\", "/");
    filePath = FileFactory.getUpdatedFilePath(filePath, fileType);
    File file = new File(filePath);
    return file.createNewFile();
  }

  @Override public CarbonFile[] locationAwareListFiles(PathFilter pathFilter) throws IOException {
    return listFiles();
  }

  @Override public String[] getLocations() throws IOException {
    return new String[]{"localhost"};
  }

  @Override
  public boolean setReplication(String filePath, short replication) throws IOException {
    // local carbon file does not need replication
    return true;
  }

  @Override
  public short getDefaultReplication(String filePath) throws IOException {
    return 1;
  }

  @Override public long getLength() {
    return file.length();
  }
}
