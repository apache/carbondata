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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.AlluxioCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

public final class FileFactory {
  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(FileFactory.class.getName());

  private static Configuration configuration;

  static {
    configuration = new Configuration();
    configuration.addResource(new Path("../core-default.xml"));
  }

  private static DefaultFileTypeProvider fileFileTypeInterface = new DefaultFileTypeProvider();

  public static void setFileTypeInterface(DefaultFileTypeProvider fileTypeInterface) {
    fileFileTypeInterface = fileTypeInterface;
  }

  private FileFactory() {

  }

  public static Configuration getConfiguration() {
    Configuration conf;
    Object confObject = ThreadLocalSessionInfo.getOrCreateCarbonSessionInfo()
        .getNonSerializableExtraInfo().get("carbonConf");
    if (confObject == null) {
      conf = configuration;
    } else {
      conf = (Configuration) confObject;
    }
    return conf;
  }

  public static FileReader getFileHolder(FileType fileType) {
    return getFileHolder(fileType, getConfiguration());
  }

  public static FileReader getFileHolder(FileFactory.FileType fileType,
      Configuration configuration) {
    switch (fileType) {
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case S3:
      case HDFS_LOCAL:
        return new DFSFileReaderImpl(configuration);
      case LOCAL:
      default:
        return new FileReaderImpl();
    }
  }

  public static FileType getFileType(String path) {
    FileType fileType = getFileTypeWithActualPath(path);
    if (fileType != null) {
      return fileType;
    }
    fileType = getFileTypeWithLowerCase(path);
    if (fileType != null) {
      return fileType;
    }

    // If custom file type is configured,
    if (fileFileTypeInterface.isPathSupported(path)) {
      return FileType.CUSTOM;
    }

    // If its unsupported file system, throw error instead of heading to wrong behavior,
    if (path.contains("://") && !path.startsWith("file://")) {
      throw new IllegalArgumentException("Path belongs to unsupported file system " + path);
    }

    return FileType.LOCAL;
  }

  private static FileType getFileTypeWithLowerCase(String path) {
    return getFileTypeWithActualPath(path.toLowerCase());
  }

  private static FileType getFileTypeWithActualPath(String path) {
    if (path.startsWith(CarbonCommonConstants.HDFSURL_PREFIX)) {
      return FileType.HDFS;
    } else if (path.startsWith(CarbonCommonConstants.ALLUXIOURL_PREFIX)) {
      return FileType.ALLUXIO;
    } else if (path.startsWith(CarbonCommonConstants.VIEWFSURL_PREFIX)) {
      return FileType.VIEWFS;
    } else if (path.startsWith(CarbonCommonConstants.S3N_PREFIX) || path
        .startsWith(CarbonCommonConstants.S3A_PREFIX) || path
        .startsWith(CarbonCommonConstants.S3_PREFIX)) {
      return FileType.S3;
    } else if (path.startsWith(CarbonCommonConstants.LOCAL_FILE_PREFIX) && !configuration
        .get(CarbonCommonConstants.FS_DEFAULT_FS)
        .equalsIgnoreCase(CarbonCommonConstants.LOCAL_FS_URI)) {
      return FileType.HDFS_LOCAL;
    }
    return null;
  }

  public static CarbonFile getCarbonFile(String path) {
    return fileFileTypeInterface.getCarbonFile(path, getConfiguration());
  }

  /**
   * Need carbon file object path because depends on file format implementation
   * path will be formatted.
   */
  public static String getFormattedPath(String path) {
    if (getFileType(path) == FileType.ALLUXIO) {
      return AlluxioCarbonFile.getFormattedPath(path);
    }
    return path;
  }

  public static CarbonFile getCarbonFile(String path,
      Configuration hadoopConf) {
    return fileFileTypeInterface.getCarbonFile(path, hadoopConf);
  }

  public static DataInputStream getDataInputStream(String path)
      throws IOException {
    return getDataInputStream(path, -1);
  }

  public static DataInputStream getDataInputStream(String path,
      Configuration configuration) throws IOException {
    return getDataInputStream(path, -1, configuration);
  }

  public static DataInputStream getDataInputStream(String path, int bufferSize)
      throws IOException {
    return getDataInputStream(path, bufferSize, getConfiguration());
  }

  public static DataInputStream getDataInputStream(String path, int bufferSize,
      Configuration configuration) throws IOException {
    return getCarbonFile(path, configuration).getDataInputStream(bufferSize);
  }

  /**
   * get data input stream
   * @param path
   * @param bufferSize
   * @param compressorName name of compressor to read this file
   * @return data input stream
   * @throws IOException
   */
  public static DataInputStream getDataInputStream(String path, int bufferSize,
      String compressorName) throws IOException {
    return getCarbonFile(path).getDataInputStream(bufferSize, compressorName);
  }

  /**
   * return the DataInputStream which is seek to the offset of file
   *
   * @param path
   * @param bufferSize
   * @param offset
   * @return DataInputStream
   * @throws IOException
   */
  public static DataInputStream getDataInputStream(String path, int bufferSize,
      long offset) throws IOException {
    return getCarbonFile(path).getDataInputStream(bufferSize, offset);
  }

  public static DataOutputStream getDataOutputStream(String path)
      throws IOException {
    return getCarbonFile(path).getDataOutputStream();
  }

  public static DataOutputStream getDataOutputStream(String path, int bufferSize,
      boolean append) throws IOException {
    return getCarbonFile(path).getDataOutputStream(bufferSize, append);
  }

  public static DataOutputStream getDataOutputStream(String path, int bufferSize,
      long blockSize) throws IOException {
    return getCarbonFile(path).getDataOutputStream(bufferSize, blockSize);
  }

  /**
   * get data output stream
   * @param path file path
   * @param bufferSize write buffer size
   * @param blockSize block size
   * @param replication replication
   * @return data output stream
   * @throws IOException if error occurs
   */
  public static DataOutputStream getDataOutputStream(String path, int bufferSize,
      long blockSize, short replication) throws IOException {
    return getCarbonFile(path).getDataOutputStream(bufferSize, blockSize, replication);
  }

  /**
   * get data out put stream
   * @param path
   * @param bufferSize
   * @param compressorName name of compressor to write this file
   * @return data out put stream
   * @throws IOException
   */
  public static DataOutputStream getDataOutputStream(String path, int bufferSize,
      String compressorName) throws IOException {
    return getCarbonFile(path).getDataOutputStream(bufferSize, compressorName);
  }

  /**
   * This method checks the given path exists or not and also is it file or
   * not if the performFileCheck is true
   *
   * @param filePath         - Path
   * @param performFileCheck - Provide false for folders, true for files and
   */
  public static boolean isFileExist(String filePath, boolean performFileCheck)
      throws IOException {
    return getCarbonFile(filePath).isFileExist(performFileCheck);
  }

  /**
   * This method checks the given path exists or not.
   *
   * @param filePath - Path
   */
  public static boolean isFileExist(String filePath) throws IOException {
    return getCarbonFile(filePath).isFileExist();
  }

  public static boolean createNewFile(String filePath) throws IOException {
    return createNewFile(filePath, null);
  }

  public static boolean createNewFile(
      String filePath,
      final FsPermission permission) throws IOException {
    return getCarbonFile(filePath).createNewFile(permission);
  }

  public static boolean deleteFile(String filePath) throws IOException {
    return getCarbonFile(filePath).deleteFile();
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

  public static boolean mkdirs(String filePath) throws IOException {
    return getCarbonFile(filePath).mkdirs();
  }

  public static boolean mkdirs(String filePath, Configuration configuration) throws IOException {
    return getCarbonFile(filePath, configuration).mkdirs();
  }

  /**
   * for getting the DataOutputStream using the hdfs filesystem append API.
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static DataOutputStream getDataOutputStreamUsingAppend(String path)
      throws IOException {
    return getCarbonFile(path).getDataOutputStreamUsingAppend();
  }

  /**
   * this method will truncate the file to the new size.
   * @param path
   * @param newSize
   * @throws IOException
   */
  public static void truncateFile(String path, long newSize) throws IOException {
    path = path.replace("\\", "/");
    FileChannel fileChannel = null;
    switch (getFileType(path)) {
      case LOCAL:
        path = getUpdatedFilePath(path);
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
      case CUSTOM:
      case S3:
        // if hadoop version >= 2.7, it can call method 'FileSystem.truncate' to truncate file,
        // this method was new in hadoop 2.7, otherwise use CarbonFile.truncate to do this.
        try {
          Path pt = new Path(path);
          FileSystem fs = pt.getFileSystem(getConfiguration());
          Method truncateMethod = fs.getClass().getDeclaredMethod("truncate",
              new Class[]{Path.class, long.class});
          truncateMethod.invoke(fs, new Object[]{pt, newSize});
        } catch (NoSuchMethodException e) {
          LOGGER.error("the version of hadoop is below 2.7, there is no 'truncate'"
              + " method in FileSystem, It needs to use 'CarbonFile.truncate'.");
          CarbonFile carbonFile = FileFactory.getCarbonFile(path);
          carbonFile.truncate(path, newSize);
        } catch (Exception e) {
          LOGGER.error("Other exception occurred while truncating the file " + e.getMessage(), e);
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
    }
  }

  /**
   * for creating a new Lock file and if it is successfully created
   * then in case of abrupt shutdown then the stream to that file will be closed.
   *
   * @param filePath
   * @return
   * @throws IOException
   */
  public static boolean createNewLockFile(String filePath) throws IOException {
    return getCarbonFile(filePath).createNewLockFile();
  }

  public enum FileType {
    LOCAL, HDFS, ALLUXIO, VIEWFS, S3, CUSTOM, HDFS_LOCAL
  }

  /**
   * Adds the schema to file path if not exists to the file path.
   * @param filePath path of file
   * @return Updated filepath
   */
  public static String addSchemeIfNotExists(String filePath) {
    FileType fileType = getFileType(filePath);
    switch (fileType) {
      case LOCAL:
        if (filePath.startsWith("file:")) {
          return filePath;
        } else {
          return new Path("file://" + filePath).toString();
        }
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case S3:
      case CUSTOM:
      case HDFS_LOCAL:
      default:
        return filePath;
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
    switch (getFileType(filePath)) {
      case HDFS:
      case VIEWFS:
      case S3:
      case CUSTOM:
      case HDFS_LOCAL:
        return filePath;
      case ALLUXIO:
        return StringUtils.startsWith(filePath, "alluxio") ? filePath : "alluxio:///" + filePath;
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
      case CUSTOM:
      case HDFS_LOCAL:
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(getConfiguration());
        return fs.getContentSummary(path).getLength();
      case LOCAL:
      default:
        filePath = getUpdatedFilePath(filePath);
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
    return path.getFileSystem(getConfiguration());
  }

  public static void createDirectoryAndSetPermission(String directoryPath, FsPermission permission)
      throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(directoryPath);
    switch (fileType) {
      case S3:
      case HDFS:
      case ALLUXIO:
      case VIEWFS:
      case CUSTOM:
      case HDFS_LOCAL:
        try {
          Path path = new Path(directoryPath);
          FileSystem fs = path.getFileSystem(getConfiguration());
          if (!fs.exists(path)) {
            fs.mkdirs(path);
            fs.setPermission(path, permission);
          }
        } catch (IOException e) {
          LOGGER.error("Exception occurred : " + e.getMessage(), e);
          throw e;
        }
        return;
      case LOCAL:
      default:
        directoryPath = FileFactory.getUpdatedFilePath(directoryPath);
        File file = new File(directoryPath);
        if (!file.mkdirs()) {
          LOGGER.error(" Failed to create directory path " + directoryPath);
        }

    }
  }

  /**
   * Check and append the hadoop's defaultFS to the path
   */
  public static String checkAndAppendDefaultFs(String path, Configuration conf) {
    if (FileFactory.getFileType(path) == FileType.CUSTOM) {
      // If its custom file type, already schema is present, no need to append schema.
      return path;
    }
    String defaultFs = conf.get(CarbonCommonConstants.FS_DEFAULT_FS);
    String lowerPath = path.toLowerCase();
    if (lowerPath.startsWith(CarbonCommonConstants.HDFSURL_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.ALLUXIOURL_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.VIEWFSURL_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.S3N_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.S3A_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.S3_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.LOCAL_FILE_PREFIX)) {
      return path;
    } else if (defaultFs != null) {
      return defaultFs + CarbonCommonConstants.FILE_SEPARATOR + path;
    } else {
      return path;
    }
  }

  /**
   * Return true if schema is present or not in the file path
   *
   * @param path
   * @return
   */
  public static boolean checkIfPrefixExists(String path) {
    if (FileFactory.getFileType(path) == FileType.CUSTOM) {
      // If its custom file type, already schema is present, no need to append schema.
      return true;
    }

    final String lowerPath = path.toLowerCase(Locale.getDefault());
    return lowerPath.contains("file:/") || lowerPath.contains("://") || lowerPath
        .startsWith(CarbonCommonConstants.HDFSURL_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.VIEWFSURL_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.LOCAL_FILE_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.ALLUXIOURL_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.S3N_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.S3_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.S3A_PREFIX);
  }

  /**
   * set the file replication
   *
   * @param path file path
   * @param replication replication
   * @return true, if success; false, if failed
   * @throws IOException if error occurs
   */
  public static boolean setReplication(String path, short replication) throws IOException {
    return getCarbonFile(path).setReplication(replication);
  }

  /**
   * get the default replication
   *
   * @param path file path
   * @return replication
   * @throws IOException if error occurs
   */
  public static short getDefaultReplication(String path) {
    return getCarbonFile(path).getDefaultReplication();
  }

  /**
   * Write content into specified file path
   * @param content content to write
   * @param filePath file path to write
   * @throws IOException if IO errors
   */
  public static void writeFile(String content, String filePath) throws IOException {
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(filePath);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      brWriter.write(content);
    } catch (IOException ie) {
      LOGGER.error("Error message: " + ie.getLocalizedMessage());
      fileWrite.setFailed();
      throw ie;
    } finally {
      try {
        CarbonUtil.closeStreams(brWriter);
      } finally {
        fileWrite.close();
      }
    }
  }

  /**
   * Read all lines in a specified file
   *
   * @param filePath file to read
   * @param conf hadoop configuration
   * @return file content
   * @throws IOException if IO errors
   */
  public static List<String> readLinesInFile(
      String filePath, Configuration conf) throws IOException {
    DataInputStream fileReader = null;
    BufferedReader bufferedReader = null;
    try {
      fileReader = FileFactory.getDataInputStream(filePath, -1, conf);
      bufferedReader =
          new BufferedReader(
              new InputStreamReader(
                  fileReader, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      return bufferedReader.lines().collect(Collectors.toList());
    } finally {
      CarbonUtil.closeStreams(fileReader, bufferedReader);
    }
  }

  public static void touchFile(CarbonFile file) throws IOException {
    if (file.exists()) {
      return;
    }
    touchDirectory(file.getParentFile());
    file.createNewFile();
  }

  public static void touchFile(CarbonFile file, FsPermission permission) throws IOException {
    if (file.exists()) {
      return;
    }
    touchDirectory(file.getParentFile(), permission);
    file.createNewFile(permission);
  }

  public static void touchDirectory(CarbonFile directory)
      throws IOException {
    if (directory.exists()) {
      return;
    }
    touchDirectory(directory.getParentFile());
    directory.mkdirs();
  }

  public static void touchDirectory(CarbonFile directory, FsPermission permission)
      throws IOException {
    if (directory.exists()) {
      return;
    }
    touchDirectory(directory.getParentFile(), permission);
    FileFactory.createDirectoryAndSetPermission(directory.getCanonicalPath(), permission);
  }

  /**
   * get the carbon folder list
   *
   * @param path folder path
   * @throws IOException if error occurs
   */
  public static List<CarbonFile> getFolderList(String path) throws IOException {
    return getCarbonFile(path, getConfiguration()).listDirs();
  }

}
