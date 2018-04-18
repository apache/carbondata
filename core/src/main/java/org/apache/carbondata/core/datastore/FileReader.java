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

package org.apache.carbondata.core.datastore;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface FileReader {

  /**
   * This method will be used to reads the data to byteBuffer from file based on offset
   * and length(number of bytes) need to read
   *
   * @param filePath fully qualified file path
   * @param offset reading start position,
   * @param length number of bytes to be read
   * @return ByteBuffer
   * @throws IOException
   */
  ByteBuffer readByteBuffer(String filePath, long offset, int length)
      throws IOException;
  /**
   * This method will be used to read the byte array from file based on offset
   * and length(number of bytes) need to read
   *
   * @param filePath fully qualified file path
   * @param offset   reading start position,
   * @param length   number of bytes to be read
   * @return read byte array
   */
  byte[] readByteArray(String filePath, long offset, int length) throws IOException;

  /**
   * This method will be used to read the byte array from file based on length(number of bytes)
   *
   * @param filePath fully qualified file path
   * @param length   number of bytes to be read
   * @return read byte array
   */
  byte[] readByteArray(String filePath, int length) throws IOException;

  /**
   * This method will be used to read int from file from postion(offset), here
   * length will be always 4 bacause int byte size if 4
   *
   * @param filePath fully qualified file path
   * @param offset   reading start position,
   * @return read int
   */
  int readInt(String filePath, long offset) throws IOException;

  /**
   * This method will be used to read long from file from postion(offset), here
   * length will be always 8 bacause int byte size is 8
   *
   * @param filePath fully qualified file path
   * @param offset   reading start position,
   * @return read long
   */
  long readLong(String filePath, long offset) throws IOException;

  /**
   * This method will be used to read int from file from postion(offset), here
   * length will be always 4 bacause int byte size if 4
   *
   * @param filePath fully qualified file path
   * @return read int
   */
  int readInt(String filePath) throws IOException;

  /**
   * This method will be used to read long value from file from postion(offset), here
   * length will be always 8 because long byte size if 4
   *
   * @param filePath fully qualified file path
   * @param offset   reading start position,
   * @return read long
   */
  long readDouble(String filePath, long offset) throws IOException;

  /**
   * This method will be used to close all the streams currently present in the cache
   */
  void finish() throws IOException;

  /**
   * Set the flag to read data page by page instead of whole blocklet.
   *
   * @param isReadPageByPage
   */
  void setReadPageByPage(boolean isReadPageByPage);

  /**
   * Whether read the data page by page from carbondata file instead of reading whole
   * blocklet to memory. It is need in case of memory constraint operations.
   */
  boolean isReadPageByPage();

}