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
import java.util.Objects;
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

  private String absoluteFilePath;

  public LocalCarbonFile(String filePath) {
    Path pathWithoutSchemeAndAuthority = Path.getPathWithoutSchemeAndAuthority(new Path(filePath));
    filePath = pathWithoutSchemeAndAuthority.toString().replace("\\", "/");
    absoluteFilePath = FileFactory.getUpdatedFilePath(filePath);
    file = new File(absoluteFilePath);
  }

  LocalCarbonFile(File file) {
    this.file = file;
    String filePath = file.getAbsolutePath().replace("\\", "/");
    absoluteFilePath = FileFactory.getUpdatedFilePath(filePath);
  }

  @Override
  public String getAbsolutePath() {
    return file.getAbsolutePath();
  }

  @Override
  public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
    if (!isDirectory()) {
      return new CarbonFile[0];
    }
    File[] files = file.listFiles(pathname -> fileFilter.accept(new LocalCarbonFile(pathname)));
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
  public String getName() {
    return file.getName();
  }

  @Override
  public boolean isDirectory() {
    return file.isDirectory();
  }

  @Override
  public boolean exists() {
    return file.exists();
  }

  @Override
  public String getCanonicalPath() {
    try {
      return file.getCanonicalPath();
    } catch (IOException e) {
      LOGGER.error("Exception occurred" + e.getMessage(), e);
    }
    return null;
  }

  @Override
  public CarbonFile getParentFile() {
    return new LocalCarbonFile(file.getParentFile());
  }

  @Override
  public String getPath() {
    return file.getPath();
  }

  @Override
  public long getSize() {
    return file.length();
  }

  public boolean renameTo(String changeToName) {
    changeToName = FileFactory.getUpdatedFilePath(changeToName);
    return file.renameTo(new File(changeToName));
  }

  public boolean delete() {
    return deleteFile();
  }

  @Override
  public CarbonFile[] listFiles() {
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
  public CarbonFile[] listFiles(boolean recursive, int maxCount) {
    // ignore the maxCount for local filesystem
    return listFiles();
  }

  @Override
  public List<CarbonFile> listFiles(Boolean recursive) {
    if (!isDirectory()) {
      return new ArrayList<>();
    }
    Collection<File> fileCollection = FileUtils.listFiles(file, null, true);
    List<CarbonFile> carbonFiles = new ArrayList<>();
    for (File file : fileCollection) {
      carbonFiles.add(new LocalCarbonFile(file));
    }
    return carbonFiles;
  }

  @Override
  public List<CarbonFile> listFiles(boolean recursive, CarbonFileFilter fileFilter) {
    if (!file.isDirectory()) {
      return new ArrayList<>();
    }
    Collection<File> fileCollection = FileUtils.listFiles(file, null, recursive);
    if (fileCollection.isEmpty()) {
      return new ArrayList<>();
    }
    List<CarbonFile> carbonFiles = new ArrayList<>();
    for (File file : fileCollection) {
      CarbonFile carbonFile = new LocalCarbonFile(file);
      if (fileFilter.accept(carbonFile)) {
        carbonFiles.add(carbonFile);
      }
    }
    return carbonFiles;
  }

  @Override
  public boolean createNewFile() {
    try {
      return file.createNewFile();
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public long getLastModifiedTime() {
    return file.lastModified();
  }

  @Override
  public boolean setLastModifiedTime(long timestamp) {
    return file.setLastModified(timestamp);
  }

  /**
   * This method will delete the data in file data from a given offset
   */
  @Override
  public boolean truncate(String fileName, long validDataEndOffset) {
    FileChannel source = null;
    FileChannel destination = null;
    boolean fileTruncatedSuccessfully = false;
    // temporary file name
    String tempWriteFilePath = fileName + CarbonCommonConstants.TEMPWRITEFILEEXTENSION;
    try {
      CarbonFile tempFile;
      // delete temporary file if it already exists at a given path
      if (isFileExist()) {
        tempFile = FileFactory.getCarbonFile(tempWriteFilePath);
        tempFile.delete();
      }
      // create new temporary file
      FileFactory.createNewFile(tempWriteFilePath);
      tempFile = FileFactory.getCarbonFile(tempWriteFilePath);
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
      LOGGER.error("Exception occurred while truncating the file " + e.getMessage(), e);
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
   * @return true if the file is modified otherwise return false.
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
  public boolean renameForce(String changeToName) {
    File destFile = new File(changeToName);
    if (destFile.exists() && !file.getAbsolutePath().equals(destFile.getAbsolutePath())) {
      if (destFile.delete()) {
        return file.renameTo(new File(changeToName));
      }
    }
    return file.renameTo(new File(changeToName));
  }

  @Override
  public DataOutputStream getDataOutputStream(int bufferSize, boolean append) throws
      FileNotFoundException {
    return new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(absoluteFilePath, append), bufferSize));
  }

  @Override
  public DataInputStream getDataInputStream(int bufferSize) throws IOException {
    return getDataInputStream(bufferSize,
        CarbonUtil.inferCompressorFromFileName(file.getAbsolutePath()));
  }

  @Override
  public DataInputStream getDataInputStream(int bufferSize, String compressor)
      throws IOException {
    InputStream inputStream;
    if (compressor.isEmpty()) {
      inputStream = new FileInputStream(absoluteFilePath);
    } else if ("GZIP".equalsIgnoreCase(compressor)) {
      inputStream = new GZIPInputStream(new FileInputStream(absoluteFilePath));
    } else if ("BZIP2".equalsIgnoreCase(compressor)) {
      inputStream = new BZip2CompressorInputStream(new FileInputStream(absoluteFilePath));
    } else if ("SNAPPY".equalsIgnoreCase(compressor)) {
      inputStream = new SnappyInputStream(new FileInputStream(absoluteFilePath));
    } else if ("LZ4".equalsIgnoreCase(compressor)) {
      inputStream = new LZ4BlockInputStream(new FileInputStream(absoluteFilePath));
    } else if ("ZSTD".equalsIgnoreCase(compressor)) {
      inputStream = new ZstdInputStream(new FileInputStream(absoluteFilePath));
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
   * return the DataInputStream which is seek to the offset of file
   *
   * @param bufferSize
   * @param offset
   * @return DataInputStream
   * @throws IOException
   */
  @Override
  public DataInputStream getDataInputStream(int bufferSize, long offset) throws
      IOException {
    FileInputStream fis = new FileInputStream(absoluteFilePath);
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
  public DataOutputStream getDataOutputStream()
      throws IOException {
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(absoluteFilePath)));
  }

  @Override
  public DataOutputStream getDataOutputStream(int bufferSize, long blockSize) throws IOException {
    return getDataOutputStream(bufferSize, blockSize, (short) 1);
  }

  @Override
  public DataOutputStream getDataOutputStream(int bufferSize, long blockSize, short replication)
      throws IOException {
    return new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(absoluteFilePath), bufferSize));
  }

  @Override
  public DataOutputStream getDataOutputStream(int bufferSize, String compressor)
      throws IOException {
    OutputStream outputStream;
    if (compressor.isEmpty()) {
      outputStream = new FileOutputStream(absoluteFilePath);
    } else if ("GZIP".equalsIgnoreCase(compressor)) {
      outputStream = new GZIPOutputStream(new FileOutputStream(absoluteFilePath));
    } else if ("BZIP2".equalsIgnoreCase(compressor)) {
      outputStream = new BZip2CompressorOutputStream(new FileOutputStream(absoluteFilePath));
    } else if ("SNAPPY".equalsIgnoreCase(compressor)) {
      outputStream = new SnappyOutputStream(new FileOutputStream(absoluteFilePath));
    } else if ("LZ4".equalsIgnoreCase(compressor)) {
      outputStream = new LZ4BlockOutputStream(new FileOutputStream(absoluteFilePath));
    } else if ("ZSTD".equalsIgnoreCase(compressor)) {
      // compression level 1 is cost-effective for sort temp file
      // which is not used for storage
      outputStream = new ZstdOutputStream(new FileOutputStream(absoluteFilePath), 1);
    } else {
      throw new IOException("Unsupported compressor: " + compressor);
    }

    if (bufferSize <= 0) {
      return new DataOutputStream(new BufferedOutputStream(outputStream));
    } else {
      return new DataOutputStream(new BufferedOutputStream(outputStream, bufferSize));
    }
  }

  @Override
  public boolean isFileExist(boolean performFileCheck) {
    if (performFileCheck) {
      return file.exists() && file.isFile();
    } else {
      return file.exists();
    }
  }

  @Override
  public boolean isFileExist() {
    return file.exists();
  }

  @Override
  public boolean createNewFile(final FsPermission permission) throws IOException {
    return file.createNewFile();
  }

  @Override
  public boolean deleteFile() {
    return FileFactory.deleteAllFilesOfDir(file);
  }

  @Override
  public boolean mkdirs() {
    return file.mkdirs();
  }

  @Override
  public DataOutputStream getDataOutputStreamUsingAppend() throws IOException {
    return new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(absoluteFilePath, true)));
  }

  @Override
  public boolean createNewLockFile() throws IOException {
    return file.createNewFile();
  }

  @Override
  public CarbonFile[] locationAwareListFiles(PathFilter pathFilter) {
    return listFiles();
  }

  @Override
  public String[] getLocations() {
    return new String[]{"localhost"};
  }

  @Override
  public boolean setReplication(short replication) {
    // local carbon file does not need replication
    return true;
  }

  @Override
  public short getDefaultReplication() {
    return 1;
  }

  @Override
  public long getLength() {
    return file.length();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LocalCarbonFile that = (LocalCarbonFile) o;
    return Objects.equals(file.getAbsolutePath(), that.file.getAbsolutePath());
  }

  @Override
  public int hashCode() {
    return Objects.hash(file.getAbsolutePath());
  }

  @Override
  public List<CarbonFile> listDirs() throws IOException {
    if (!file.isDirectory()) {
      return new ArrayList<CarbonFile>();
    }
    File[] files = file.listFiles();
    if (null == files) {
      return new ArrayList<CarbonFile>();
    }
    List<CarbonFile> carbonFiles = new ArrayList<CarbonFile>();
    for (File value : files) {
      carbonFiles.add(new LocalCarbonFile(value));
    }
    return carbonFiles;
  }
}
