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
import java.util.List;

import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;

public interface CarbonFile {

  String getAbsolutePath();

  CarbonFile[] listFiles(CarbonFileFilter fileFilter);

  CarbonFile[] listFiles();

  List<CarbonFile> listFiles(Boolean recursive) throws IOException;

  List<CarbonFile> listFiles(boolean recursive, CarbonFileFilter fileFilter) throws IOException;

  /**
   * It returns list of files with location details.
   * @return
   */
  CarbonFile[] locationAwareListFiles(PathFilter pathFilter) throws IOException;

  String getName();

  boolean isDirectory();

  boolean exists();

  String getCanonicalPath();

  CarbonFile getParentFile();

  String getPath();

  long getSize();

  boolean renameTo(String changeToName);

  boolean renameForce(String changeToName);

  /**
   * This method will delete the files recursively from file system
   *
   * @return true if success
   */
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

  /**
   * get data input stream
   * @param path
   * @param fileType
   * @param bufferSize
   * @param compressor name of compressor to write this file
   * @return dataInputStream
   * @throws IOException
   */
  DataInputStream getDataInputStream(String path, FileFactory.FileType fileType, int bufferSize,
      String compressor) throws IOException;

  DataInputStream getDataInputStream(String path, FileFactory.FileType fileType, int bufferSize,
      long offset) throws IOException;

  DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType)
      throws IOException;

  DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType, int bufferSize,
      long blockSize) throws IOException;

  /**
   * get data output stream
   * @param path file path
   * @param fileType file type
   * @param bufferSize write buffer size
   * @param blockSize block size
   * @param replication replication for this file
   * @return data output stream
   * @throws IOException if error occurs
   */
  DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType, int bufferSize,
      long blockSize, short replication) throws IOException;
  /**
   * get data output stream
   * @param path
   * @param fileType
   * @param bufferSize
   * @param compressor name of compressor to write this file
   * @return DataOutputStream
   * @throws IOException
   */
  DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType, int bufferSize,
      String compressor) throws IOException;

  boolean isFileExist(String filePath, boolean performFileCheck)
      throws IOException;

  boolean isFileExist(String filePath) throws IOException;

  boolean createNewFile(String filePath, FileFactory.FileType fileType) throws IOException;

  boolean createNewFile(String filePath, FileFactory.FileType fileType, boolean doAs,
      final FsPermission permission) throws IOException;

  boolean deleteFile(String filePath, FileFactory.FileType fileType) throws IOException;

  boolean mkdirs(String filePath) throws IOException;

  DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType)
      throws IOException;

  boolean createNewLockFile(String filePath, FileFactory.FileType fileType) throws IOException;

  /**
   * Returns locations of the file
   * @return
   */
  String[] getLocations() throws IOException;

  /**
   * set the replication factor for this file
   *
   * @param filePath file path
   * @param replication replication
   * @return true, if success; false, if failed
   * @throws IOException if error occurs
   */
  boolean setReplication(String filePath, short replication) throws IOException;

  /**
   * get the default replication for this file
   * @param filePath file path
   * @return replication factor
   * @throws IOException if error occurs
   */
  short getDefaultReplication(String filePath) throws IOException;

  /**
   * Get the length of this file, in bytes.
   * @return the length of this file, in bytes.
   */
  long getLength();
}
