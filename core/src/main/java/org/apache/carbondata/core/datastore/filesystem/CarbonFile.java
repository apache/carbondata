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

import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;

public interface CarbonFile {

  String getAbsolutePath();

  CarbonFile[] listFiles(CarbonFileFilter fileFilter);

  CarbonFile[] listFiles();

  CarbonFile[] listFiles(boolean recursive, int maxCount) throws IOException;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3678

  List<CarbonFile> listFiles(Boolean recursive) throws IOException;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3126

  List<CarbonFile> listFiles(boolean recursive, CarbonFileFilter fileFilter) throws IOException;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3057

  /**
   * It returns list of files with location details.
   */
  CarbonFile[] locationAwareListFiles(PathFilter pathFilter) throws IOException;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362

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
   */
  boolean isFileModified(long fileTimeStamp, long endOffset);

  DataOutputStream getDataOutputStream(int bufferSize, boolean append) throws IOException;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863

  DataInputStream getDataInputStream(int bufferSize) throws
      IOException;

  /**
   * get data input stream
   * @param compressor name of compressor to write this file
   * @return dataInputStream
   * @throws IOException
   */
  DataInputStream getDataInputStream(int bufferSize, String compressor) throws IOException;

  DataInputStream getDataInputStream(int bufferSize, long offset) throws IOException;

  DataOutputStream getDataOutputStream() throws IOException;

  DataOutputStream getDataOutputStream(int bufferSize, long blockSize) throws IOException;

  /**
   * get data output stream
   * @param bufferSize write buffer size
   * @param blockSize block size
   * @param replication replication for this file
   * @return data output stream
   * @throws IOException if error occurs
   */
  DataOutputStream getDataOutputStream(int bufferSize,
      long blockSize, short replication) throws IOException;

  /**
   * get data output stream
   * @param compressor name of compressor to write this file
   * @return DataOutputStream
   * @throws IOException
   */
  DataOutputStream getDataOutputStream(int bufferSize, String compressor) throws IOException;

  boolean isFileExist(boolean performFileCheck) throws IOException;

  boolean isFileExist() throws IOException;

  boolean createNewFile(final FsPermission permission) throws IOException;

  boolean deleteFile() throws IOException;

  boolean mkdirs() throws IOException;

  DataOutputStream getDataOutputStreamUsingAppend() throws IOException;

  boolean createNewLockFile() throws IOException;

  /**
   * Returns locations of the file
   * @return
   */
  String[] getLocations() throws IOException;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2037

  /**
   * set the replication factor for this file
   *
   * @param replication replication
   * @return true, if success; false, if failed
   * @throws IOException if error occurs
   */
  boolean setReplication(short replication) throws IOException;

  /**
   * get the default replication for this file
   * @return replication factor
   */
  short getDefaultReplication();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863

  /**
   * Get the length of this file, in bytes.
   * @return the length of this file, in bytes.
   */
  long getLength() throws IOException;
}
