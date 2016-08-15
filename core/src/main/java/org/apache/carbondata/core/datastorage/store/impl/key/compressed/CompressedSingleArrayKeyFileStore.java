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

package org.apache.carbondata.core.datastorage.store.impl.key.compressed;

import org.apache.carbondata.core.datastorage.store.FileHolder;

public class CompressedSingleArrayKeyFileStore extends AbstractCompressedSingleArrayStore {

  /**
   * offset, this will be used for seek position
   */
  private long offset;

  /**
   * fully qualified file path
   */
  private String filePath;

  /**
   * length to be read
   */
  private int length;

  public CompressedSingleArrayKeyFileStore(int size, int elementSize, long offset, String filePath,
      int length) {
    super(size, elementSize, false);
    this.offset = offset;
    this.filePath = filePath;
    this.length = length;
  }

  /**
   * This method will be used to get the actual keys array present in the
   * store . Here back array will be uncompress array. This method will first read
   * the data from file based on offset and length then uncompress the array
   * to get the actual array
   *
   * @param fileHolder file holder will be used to read the file
   * @return uncompressed
   * keys will return uncompressed key
   */
  @Override public byte[] getBackArray(FileHolder fileHolder) {
    if (null != fileHolder) {
      // read from file based on offset and index, fileholder will read that
      // much byte from that offset, then uncompress and return
      return COMPRESSOR.unCompress(fileHolder.readByteArray(filePath, offset, length));
    } else {
      return new byte[0];
    }
  }

  /**
   * This method will be used to get the key array based on index
   * This method will first read
   * the data from file based on offset and length then uncompress the array
   * to get the actual array, then get the array for index and return
   *
   * @param index      index in store
   * @param fileHolder file holder will be used to read the file
   * @return key
   */
  @Override public byte[] get(int index, FileHolder fileHolder) {
    // read from file based on offset and index, fileholder will read that
    // much byte from that offset, then uncompress to get the actual array
    byte[] unCompress = COMPRESSOR.unCompress(fileHolder.readByteArray(filePath, offset, length));
    // create new array of size of each element
    byte[] copy = new byte[sizeOfEachElement];
    // copy array for given index
    // copy will done based on below calculation
    // eg: index is 4 and size of each key is 6 then copy from 6*4= 24th
    // index till 29th index
    System.arraycopy(unCompress, ((index) * sizeOfEachElement), copy, 0, sizeOfEachElement);
    return copy;
  }
}
