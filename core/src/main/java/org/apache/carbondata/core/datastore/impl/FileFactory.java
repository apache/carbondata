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

package org.apache.carbondata.core.datastore.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public final class FileFactory {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(FileFactory.class.getName());
  private static Configuration configuration = null;

  static {
    configuration = new Configuration();
    configuration.addResource(new Path("../core-default.xml"));
  }

  private static FileTypeInterface fileFileTypeInterface = new DefaultFileTypeProvider();
  public static void setFileTypeInterface(FileTypeInterface fileTypeInterface) {
    fileFileTypeInterface = fileTypeInterface;
  }
  private FileFactory() {

  }

  public static Configuration getConfiguration() {
    return configuration;
  }

  public static FileReader getFileHolder(FileType fileType) {
    return fileFileTypeInterface.getFileHolder(fileType);
  }

  public static FileType getFileType(String path) {
    String lowerPath = path.toLowerCase();
    if (lowerPath.startsWith(CarbonCommonConstants.HDFSURL_PREFIX)) {
      return FileType.HDFS;
    } else if (lowerPath.startsWith(CarbonCommonConstants.ALLUXIOURL_PREFIX)) {
      return FileType.ALLUXIO;
    } else if (lowerPath.startsWith(CarbonCommonConstants.VIEWFSURL_PREFIX)) {
      return FileType.VIEWFS;
    } else if (lowerPath.startsWith(CarbonCommonConstants.S3N_PREFIX) ||
        lowerPath.startsWith(CarbonCommonConstants.S3A_PREFIX) ||
        lowerPath.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      return FileType.S3;
    }
    return FileType.LOCAL;
  }

  public static CarbonFile getCarbonFile(String path) {
    return fileFileTypeInterface.getCarbonFile(path, getFileType(path));
  }
  public static CarbonFile getCarbonFile(String path, FileType fileType) {
    return fileFileTypeInterface.getCarbonFile(path, fileType);
  }
  public static CarbonFile getCarbonFile(String path,
      Configuration hadoopConf) {
    return fileFileTypeInterface.getCarbonFile(path, getFileType(path), hadoopConf);
  }

  public static DataInputStream getDataInputStream(String path, FileType fileType)
      throws IOException {
    return getDataInputStream(path, fileType, -1);
  }

  public static DataInputStream getDataInputStream(String path, FileType fileType, int bufferSize)
      throws IOException {
    return getDataInputStream(path, fileType, bufferSize, configuration);
  }
  public static DataInputStream getDataInputStream(String path, FileType fileType, int bufferSize,
      Configuration configuration) throws IOException {
    return getCarbonFile(path).getDataInputStream(path, fileType, bufferSize, configuration);
  }

  /**
   * get data input stream
   * @param path
   * @param fileType
   * @param bufferSize
   * @param compressorName name of compressor to read this file
   * @return data input stream
   * @throws IOException
   */
  public static DataInputStream getDataInputStream(String path, FileType fileType, int bufferSize,
      String compressorName) throws IOException {
    return getCarbonFile(path).getDataInputStream(path, fileType, bufferSize, compressorName);
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
  public static DataInputStream getDataInputStream(String path, FileType fileType, int bufferSize,
      long offset) throws IOException {
    return getCarbonFile(path).getDataInputStream(path, fileType, bufferSize, offset);
  }

  public static DataOutputStream getDataOutputStream(String path, FileType fileType)
      throws IOException {
    return getCarbonFile(path).getDataOutputStream(path, fileType);
  }

  public static DataOutputStream getDataOutputStream(String path, FileType fileType, int bufferSize,
      boolean append) throws IOException {
    return getCarbonFile(path).getDataOutputStream(path, fileType, bufferSize, append);
  }

  public static DataOutputStream getDataOutputStream(String path, FileType fileType, int bufferSize,
      long blockSize) throws IOException {
    return getCarbonFile(path).getDataOutputStream(path, fileType, bufferSize, blockSize);
  }

  /**
   * get data output stream
   * @param path file path
   * @param fileType file type
   * @param bufferSize write buffer size
   * @param blockSize block size
   * @param replication replication
   * @return data output stream
   * @throws IOException if error occurs
   */
  public static DataOutputStream getDataOutputStream(String path, FileType fileType, int bufferSize,
      long blockSize, short replication) throws IOException {
    return getCarbonFile(path).getDataOutputStream(path, fileType, bufferSize, blockSize,
        replication);
  }
  /**
   * get data out put stream
   * @param path
   * @param fileType
   * @param bufferSize
   * @param compressorName name of compressor to write this file
   * @return data out put stram
   * @throws IOException
   */
  public static DataOutputStream getDataOutputStream(String path, FileType fileType, int bufferSize,
      String compressorName) throws IOException {
    return getCarbonFile(path).getDataOutputStream(path, fileType, bufferSize, compressorName);
  }
  /**
   * This method checks the given path exists or not and also is it file or
   * not if the performFileCheck is true
   *
   * @param filePath         - Path
   * @param fileType         - FileType Local/HDFS
   * @param performFileCheck - Provide false for folders, true for files and
   */
  public static boolean isFileExist(String filePath, FileType fileType, boolean performFileCheck)
      throws IOException {
    return getCarbonFile(filePath).isFileExist(filePath, fileType, performFileCheck);
  }

  /**
   * This method checks the given path exists or not.
   *
   * @param filePath - Path
   * @param fileType - FileType Local/HDFS
   */
  public static boolean isFileExist(String filePath, FileType fileType) throws IOException {
    return getCarbonFile(filePath).isFileExist(filePath, fileType);
  }

  /**
   * This method checks the given path exists or not.
   *
   * @param filePath - Path
   */
  public static boolean isFileExist(String filePath) throws IOException {
    return isFileExist(filePath, getFileType(filePath));
  }

  public static boolean createNewFile(String filePath, FileType fileType) throws IOException {
    return createNewFile(filePath, fileType, true, null);
  }
  public static boolean createNewFile(
      String filePath,
      FileType fileType,
      boolean doAs,
      final FsPermission permission) throws IOException {
    return getCarbonFile(filePath).createNewFile(filePath, fileType, doAs, permission);
  }
  public static boolean deleteFile(String filePath, FileType fileType) throws IOException {
    return getCarbonFile(filePath).deleteFile(filePath, fileType);
  }

  public static boolean deleteAllFilesOfDir(File path) {
    if (!path.exists()) {
      return true;
    }
    if (path.isFile()) {
      return path.delete();
    }
    File[] files = path.listFiles();
    if (null == files) {
      return true;
    }
    for (int i = 0; i < files.length; i++) {
      deleteAllFilesOfDir(files[i]);
    }
    return path.delete();
  }

  public static boolean deleteAllCarbonFilesOfDir(CarbonFile path) {
    if (!path.exists()) {
      return true;
    }
    if (!path.isDirectory()) {
      return path.delete();
    }
    CarbonFile[] files = path.listFiles();
    for (int i = 0; i < files.length; i++) {
      deleteAllCarbonFilesOfDir(files[i]);
    }
    return path.delete();
  }

  public static boolean deleteAllCarbonFiles(List<CarbonFile> carbonFiles) {
    boolean isFilesDeleted = true;
    // Delete all old stale segment folders
    for (CarbonFile carbonFile : carbonFiles) {
      // try block is inside for loop because even if there is failure in deletion of 1 stale
      // folder still remaining stale folders should be deleted
      isFilesDeleted = isFilesDeleted && deleteAllCarbonFilesOfDir(carbonFile);
    }
    return isFilesDeleted;
  }

  public static boolean mkdirs(String filePath, FileType fileType) throws IOException {
    return getCarbonFile(filePath).mkdirs(filePath);
  }

  public static boolean mkdirs(String filePath, Configuration configuration) throws IOException {
    return getCarbonFile(filePath, configuration).mkdirs(filePath);
  }

  /**
   * for getting the dataoutput stream using the hdfs filesystem append API.
   *
   * @param path
   * @param fileType
   * @return
   * @throws IOException
   */
  public static DataOutputStream getDataOutputStreamUsingAppend(String path, FileType fileType)
      throws IOException {
    return getCarbonFile(path).getDataOutputStreamUsingAppend(path, fileType);
  }

  /**
   * this method will truncate the file to the new size.
   * @param path
   * @param fileType
   * @param newSize
   * @throws IOException
   */
  public static void truncateFile(String path, FileType fileType, long newSize) throws IOException {
    path = path.replace("\\", "/");
    FileChannel fileChannel = null;
    switch (fileType) {
      case LOCAL:
        path = getUpdatedFilePath(path, fileType);
        fileChannel = new FileOutputStream(path, true).getChannel();
        try {
          fileChannel.truncate(newSize);
        } finally {
          if (fileChannel != null) {
            fileChannel.close();
          }
        }
        return;
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case S3:
        // if hadoop version >= 2.7, it can call method 'FileSystem.truncate' to truncate file,
        // this method was new in hadoop 2.7, otherwise use CarbonFile.truncate to do this.
        try {
          Path pt = new Path(path);
          FileSystem fs = pt.getFileSystem(configuration);
          Method truncateMethod = fs.getClass().getDeclaredMethod("truncate",
              new Class[]{Path.class, long.class});
          truncateMethod.invoke(fs, new Object[]{pt, newSize});
        } catch (NoSuchMethodException e) {
          LOGGER.error("the version of hadoop is below 2.7, there is no 'truncate'"
              + " method in FileSystem, It needs to use 'CarbonFile.truncate'.");
          CarbonFile carbonFile = FileFactory.getCarbonFile(path, fileType);
          carbonFile.truncate(path, newSize);
        } catch (Exception e) {
          LOGGER.error("Other exception occurred while truncating the file " + e.getMessage());
        }
        return;
      default:
        fileChannel = new FileOutputStream(path, true).getChannel();
        try {
          fileChannel.truncate(newSize);
        } finally {
          if (fileChannel != null) {
            fileChannel.close();
          }
        }
        return;
    }
  }

  /**
   * for creating a new Lock file and if it is successfully created
   * then in case of abrupt shutdown then the stream to that file will be closed.
   *
   * @param filePath
   * @param fileType
   * @return
   * @throws IOException
   */
  public static boolean createNewLockFile(String filePath, FileType fileType) throws IOException {
    return getCarbonFile(filePath).createNewLockFile(filePath, fileType);
  }

  public enum FileType {
    LOCAL, HDFS, ALLUXIO, VIEWFS, S3
  }

  /**
   * below method will be used to update the file path
   * for local type
   * it removes the file:/ from the path
   *
   * @param filePath
   * @param fileType
   * @return updated file path without url for local
   */
  public static String getUpdatedFilePath(String filePath, FileType fileType) {
    switch (fileType) {
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case S3:
        return filePath;
      case LOCAL:
      default:
        if (filePath != null && !filePath.isEmpty()) {
          // If the store path is relative then convert to absolute path.
          if (filePath.startsWith("./")) {
            try {
              return new File(filePath).getCanonicalPath();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          } else {
            Path pathWithoutSchemeAndAuthority =
                Path.getPathWithoutSchemeAndAuthority(new Path(filePath));
            return pathWithoutSchemeAndAuthority.toString();
          }
        } else {
          return filePath;
        }
    }
  }

  /**
   * below method will be used to update the file path
   * for local type
   * it removes the file:/ from the path
   *
   * @param filePath
   * @return updated file path without url for local
   */
  public static String getUpdatedFilePath(String filePath) {
    FileType fileType = getFileType(filePath);
    return getUpdatedFilePath(filePath, fileType);
  }

  /**
   * It computes size of directory
   *
   * @param filePath
   * @return size in bytes
   * @throws IOException
   */
  public static long getDirectorySize(String filePath) throws IOException {
    FileType fileType = getFileType(filePath);
    switch (fileType) {
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case S3:
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        return fs.getContentSummary(path).getLength();
      case LOCAL:
      default:
        filePath = getUpdatedFilePath(filePath, fileType);
        File file = new File(filePath);
        return FileUtils.sizeOfDirectory(file);
    }
  }

  /**
   * This method will create the path object for a given file
   *
   * @param filePath
   * @return
   */
  public static Path getPath(String filePath) {
    return new Path(filePath);
  }

  /**
   * This method will return the filesystem instance
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static FileSystem getFileSystem(Path path) throws IOException {
    return path.getFileSystem(configuration);
  }


  public static void createDirectoryAndSetPermission(String directoryPath, FsPermission permission)
      throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(directoryPath);
    switch (fileType) {
      case S3:
      case HDFS:
      case VIEWFS:
        try {
          Path path = new Path(directoryPath);
          FileSystem fs = path.getFileSystem(FileFactory.configuration);
          if (!fs.exists(path)) {
            fs.mkdirs(path);
            fs.setPermission(path, permission);
          }
        } catch (IOException e) {
          LOGGER.error("Exception occurred : " + e.getMessage());
          throw e;
        }
        return;
      case LOCAL:
      default:
        directoryPath = FileFactory.getUpdatedFilePath(directoryPath, fileType);
        File file = new File(directoryPath);
        if (!file.mkdirs()) {
          LOGGER.error(" Failed to create directory path " + directoryPath);
        }

    }
  }

  /**
   * set the file replication
   *
   * @param path file path
   * @param fileType file type
   * @param replication replication
   * @return true, if success; false, if failed
   * @throws IOException if error occurs
   */
  public static boolean setReplication(String path, FileFactory.FileType fileType,
      short replication) throws IOException {
    return getCarbonFile(path, fileType).setReplication(path, replication);
  }

  /**
   * get the default replication
   *
   * @param path file path
   * @param fileType file type
   * @return replication
   * @throws IOException if error occurs
   */
  public static short getDefaultReplication(String path, FileFactory.FileType fileType)
      throws IOException {
    return getCarbonFile(path, fileType).getDefaultReplication(path);
  }
}
