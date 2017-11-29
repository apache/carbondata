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

import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;

public interface CarbonFile {

  String getAbsolutePath();

  CarbonFile[] listFiles(CarbonFileFilter fileFilter);

  CarbonFile[] listFiles();

  String getName();

  boolean isDirectory();

  boolean exists();

  String getCanonicalPath();

  CarbonFile getParentFile();

  String getPath();

  long getSize();

  boolean renameTo(String changetoName);

  boolean renameForce(String changetoName);

  boolean delete();

  boolean createNewFile();

  long getLastModifiedTime();

  boolean setLastModifiedTime(long timestamp);

  boolean truncate(String fileName, long validDataEndOffset);

  /**
   * This method will be used to check whether a file has been modified or not
   *
   * @param fileTimeStamp time to be compared with latest timestamp of file
   * @param endOffset     file length to be compared with current length of file
   * @return
   */
  boolean isFileModified(long fileTimeStamp, long endOffset);

  DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType, int bufferSize,
      boolean append) throws IOException;

  DataInputStream getDataInputStream(String path, FileFactory.FileType fileType, int bufferSize,
      Configuration configuration) throws IOException;

  DataInputStream getDataInputStream(String path, FileFactory.FileType fileType, int bufferSize,
      long offset) throws IOException;

  DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType)
      throws IOException;

  DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType, int bufferSize,
      long blockSize) throws IOException;

  boolean isFileExist(String filePath, FileFactory.FileType fileType, boolean performFileCheck)
      throws IOException;

  boolean isFileExist(String filePath, FileFactory.FileType fileType) throws IOException;

  boolean createNewFile(String filePath, FileFactory.FileType fileType) throws IOException;

  boolean createNewFile(String filePath, FileFactory.FileType fileType, boolean doAs,
      final FsPermission permission) throws IOException;

  boolean deleteFile(String filePath, FileFactory.FileType fileType) throws IOException;

  boolean mkdirs(String filePath, FileFactory.FileType fileType) throws IOException;

  DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType)
      throws IOException;

  boolean createNewLockFile(String filePath, FileFactory.FileType fileType) throws IOException;

  void setPermission(String directoryPath, FsPermission permission, String username, String group)
      throws IOException;

}
