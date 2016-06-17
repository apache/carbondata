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

package org.carbondata.core.datastorage.store.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.HDFSCarbonFile;
import org.carbondata.core.datastorage.store.filesystem.LocalCarbonFile;
import org.carbondata.core.datastorage.store.filesystem.ViewFSCarbonFile;
import org.carbondata.core.util.CarbonUtil;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class FileFactory {
  private static Configuration configuration = null;

  private static FileType storeDefaultFileType = FileType.LOCAL;

  static {
    String property = CarbonUtil.getCarbonStorePath(null, null);
    if (property != null) {
      if (property.startsWith(CarbonUtil.HDFS_PREFIX)) {
        storeDefaultFileType = FileType.HDFS;
      } else if (property.startsWith(CarbonUtil.VIEWFS_PREFIX)) {
        storeDefaultFileType = FileType.VIEWFS;
      }
    }

    configuration = new Configuration();
    configuration.addResource(new Path("../core-default.xml"));
  }

  private FileFactory() {

  }

  public static Configuration getConfiguration() {
    return configuration;
  }

  public static FileHolder getFileHolder(FileType fileType) {
    switch (fileType) {
      case LOCAL:
        return new FileHolderImpl();
      case HDFS:
      case VIEWFS:
        return new DFSFileHolderImpl();
      default:
        return new FileHolderImpl();
    }
  }

  public static FileType getFileType() {
    String property = CarbonUtil.getCarbonStorePath(null, null);
    if (property != null) {
      if (property.startsWith(CarbonUtil.HDFS_PREFIX)) {
        storeDefaultFileType = FileType.HDFS;
      } else if (property.startsWith(CarbonUtil.VIEWFS_PREFIX)) {
        storeDefaultFileType = FileType.VIEWFS;
      }
    }
    return storeDefaultFileType;
  }

  public static FileType getFileType(String path) {
    if (path.startsWith(CarbonUtil.HDFS_PREFIX)) {
      return FileType.HDFS;
    } else if (path.startsWith(CarbonUtil.VIEWFS_PREFIX)) {
      return FileType.VIEWFS;
    }
    return FileType.LOCAL;
  }

  public static CarbonFile getCarbonFile(String path, FileType fileType) {
    switch (fileType) {
      case LOCAL:
        return new LocalCarbonFile(path);
      case HDFS:
        return new HDFSCarbonFile(path);
      case VIEWFS:
        return new ViewFSCarbonFile(path);
      default:
        return new LocalCarbonFile(path);
    }
  }

  public static DataInputStream getDataInputStream(String path, FileType fileType)
      throws IOException {
    path = path.replace("\\", "/");
    switch (fileType) {
      case LOCAL:
        return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
        FSDataInputStream stream = fs.open(pt);
        return new DataInputStream(new BufferedInputStream(stream));
      default:
        return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
    }
  }

  public static DataInputStream getDataInputStream(String path, FileType fileType, int bufferSize)
      throws IOException {
    path = path.replace("\\", "/");
    switch (fileType) {
      case LOCAL:
        return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
        FSDataInputStream stream = fs.open(pt, bufferSize);
        return new DataInputStream(new BufferedInputStream(stream));
      default:
        return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
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
  public static DataInputStream getDataInputStream(String path, FileType fileType, int bufferSize,
      long offset) throws IOException {
    path = path.replace("\\", "/");
    switch (fileType) {
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
        FSDataInputStream stream = fs.open(pt, bufferSize);
        stream.seek(offset);
        return new DataInputStream(new BufferedInputStream(stream));
      default:
        FileInputStream fis = new FileInputStream(path);
        long actualSkipSize = 0;
        long skipSize = offset;
        while (actualSkipSize != offset) {
          actualSkipSize += fis.skip(skipSize);
          skipSize = skipSize - actualSkipSize;
        }
        return new DataInputStream(new BufferedInputStream(fis));
    }
  }

  public static DataOutputStream getDataOutputStream(String path, FileType fileType)
      throws IOException {
    path = path.replace("\\", "/");
    switch (fileType) {
      case LOCAL:
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
        FSDataOutputStream stream = fs.create(pt, true);
        return stream;
      default:
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
    }
  }

  public static DataOutputStream getDataOutputStream(String path, FileType fileType,
      short replicationFactor) throws IOException {
    path = path.replace("\\", "/");
    switch (fileType) {
      case LOCAL:
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
        FSDataOutputStream stream = fs.create(pt, replicationFactor);
        return stream;
      default:
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
    }
  }

  public static DataOutputStream getDataOutputStream(String path, FileType fileType, int bufferSize)
      throws IOException {
    path = path.replace("\\", "/");
    switch (fileType) {
      case LOCAL:
        return new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(path), bufferSize));
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
        FSDataOutputStream stream = fs.create(pt, true, bufferSize);
        return stream;
      default:
        return new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(path), bufferSize));
    }
  }

  public static DataOutputStream getDataOutputStream(String path, FileType fileType, int bufferSize,
      boolean append) throws IOException {
    path = path.replace("\\", "/");
    switch (fileType) {
      case LOCAL:
        return new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(path, append), bufferSize));
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
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
      default:
        return new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(path), bufferSize));
    }
  }

  public static DataOutputStream getDataOutputStream(String path, FileType fileType, int bufferSize,
      long blockSize) throws IOException {
    path = path.replace("\\", "/");
    switch (fileType) {
      case LOCAL:
        return new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(path), bufferSize));
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
        FSDataOutputStream stream =
            fs.create(pt, true, bufferSize, fs.getDefaultReplication(pt), blockSize);
        return stream;
      default:
        return new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(path), bufferSize));
    }
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
    filePath = filePath.replace("\\", "/");
    switch (fileType) {
      case HDFS:
      case VIEWFS:
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        if (performFileCheck) {
          return fs.exists(path) && fs.isFile(path);
        } else {
          return fs.exists(path);
        }

      case LOCAL:
      default:
        File defaultFile = new File(filePath);

        if (performFileCheck) {
          return defaultFile.exists() && defaultFile.isFile();
        } else {
          return defaultFile.exists();
        }
    }
  }

  /**
   * This method checks the given path exists or not and also is it file or
   * not if the performFileCheck is true
   *
   * @param filePath - Path
   * @param fileType - FileType Local/HDFS
   */
  public static boolean isFileExist(String filePath, FileType fileType) throws IOException {
    filePath = filePath.replace("\\", "/");
    switch (fileType) {
      case HDFS:
      case VIEWFS:
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        return fs.exists(path);

      case LOCAL:
      default:
        File defaultFile = new File(filePath);
        return defaultFile.exists();
    }
  }

  public static boolean createNewFile(String filePath, FileType fileType) throws IOException {
    filePath = filePath.replace("\\", "/");
    switch (fileType) {
      case HDFS:
      case VIEWFS:
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        return fs.createNewFile(path);

      case LOCAL:
      default:
        File file = new File(filePath);
        return file.createNewFile();
    }
  }

  public static boolean mkdirs(String filePath, FileType fileType) throws IOException {
    filePath = filePath.replace("\\", "/");
    switch (fileType) {
      case HDFS:
      case VIEWFS:
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        return fs.mkdirs(path);
      case LOCAL:
      default:
        File file = new File(filePath);
        return file.mkdirs();
    }
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
    path = path.replace("\\", "/");
    switch (fileType) {
      case LOCAL:
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path, true)));
      case HDFS:
      case VIEWFS:
        Path pt = new Path(path);
        FileSystem fs = pt.getFileSystem(configuration);
        FSDataOutputStream stream = fs.append(pt);
        return stream;
      default:
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
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
    filePath = filePath.replace("\\", "/");
    switch (fileType) {
      case HDFS:
      case VIEWFS:
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        if (fs.createNewFile(path)) {
          fs.deleteOnExit(path);
          return true;
        }
        return false;
      case LOCAL:
      default:
        File file = new File(filePath);
        return file.createNewFile();
    }
  }

  public enum FileType {
    LOCAL, HDFS, VIEWFS
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
    switch (fileType) {
      case HDFS:
      case VIEWFS:
        return filePath;
      case LOCAL:
      default:
        Path pathWithoutSchemeAndAuthority =
            Path.getPathWithoutSchemeAndAuthority(new Path(filePath));
        return pathWithoutSchemeAndAuthority.toString();
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
      case VIEWFS:
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        return fs.getContentSummary(path).getLength();
      case LOCAL:
      default:
        File file = new File(filePath);
        return FileUtils.sizeOfDirectory(file);
    }
  }

}
