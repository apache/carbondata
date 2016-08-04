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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DFSFileHolderImpl implements FileHolder {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DFSFileHolderImpl.class.getName());
  /**
   * cache to hold filename and its stream
   */
  private Map<String, FSDataInputStream> fileNameAndStreamCache;

  public DFSFileHolderImpl() {
    this.fileNameAndStreamCache =
        new HashMap<String, FSDataInputStream>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  @Override public byte[] readByteArray(String filePath, long offset, int length) {
    FSDataInputStream fileChannel = updateCache(filePath);
    byte[] byteBffer = read(fileChannel, length, offset);
    return byteBffer;
  }

  /**
   * This method will be used to check whether stream is already present in
   * cache or not for filepath if not present then create it and then add to
   * cache, other wise get from cache
   *
   * @param filePath fully qualified file path
   * @return channel
   */
  private FSDataInputStream updateCache(String filePath) {
    FSDataInputStream fileChannel = fileNameAndStreamCache.get(filePath);
    try {
      if (null == fileChannel) {
        Path pt = new Path(filePath);
        FileSystem fs = FileSystem.get(FileFactory.getConfiguration());
        fileChannel = fs.open(pt);
        fileNameAndStreamCache.put(filePath, fileChannel);
      }
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return fileChannel;
  }

  /**
   * This method will be used to read from file based on number of bytes to be read and positon
   *
   * @param channel file channel
   * @param size    number of bytes
   * @param offset  position
   * @return byte buffer
   */
  private byte[] read(FSDataInputStream channel, int size, long offset) {
    byte[] byteBffer = new byte[size];
    try {
      channel.seek(offset);
      channel.readFully(byteBffer);
    } catch (Exception e) {
      LOGGER.error(e, e.getMessage());
    }
    return byteBffer;
  }

  /**
   * This method will be used to read from file based on number of bytes to be read and positon
   *
   * @param channel file channel
   * @param size    number of bytes
   * @return byte buffer
   */
  private byte[] read(FSDataInputStream channel, int size) {
    byte[] byteBffer = new byte[size];
    try {
      channel.readFully(byteBffer);
    } catch (Exception e) {
      LOGGER.error(e, e.getMessage());
    }
    return byteBffer;
  }

  @Override public int readInt(String filePath, long offset) {
    FSDataInputStream fileChannel = updateCache(filePath);
    int i = -1;
    try {
      fileChannel.seek(offset);
      i = fileChannel.readInt();
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }

    return i;
  }

  @Override public long readDouble(String filePath, long offset) {
    FSDataInputStream fileChannel = updateCache(filePath);
    long i = -1;
    try {
      fileChannel.seek(offset);
      i = fileChannel.readLong();
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }

    return i;
  }

  @Override public void finish() {
    for (Entry<String, FSDataInputStream> entry : fileNameAndStreamCache.entrySet()) {
      try {
        FSDataInputStream channel = entry.getValue();
        if (null != channel) {
          channel.close();
        }
      } catch (IOException exception) {
        LOGGER.error(exception, exception.getMessage());
      }
    }

  }

  @Override public byte[] readByteArray(String filePath, int length) {
    FSDataInputStream fileChannel = updateCache(filePath);
    byte[] byteBffer = read(fileChannel, length);
    return byteBffer;
  }

  @Override public long readLong(String filePath, long offset) {
    FSDataInputStream fileChannel = updateCache(filePath);
    long i = -1;
    try {
      fileChannel.seek(offset);
      i = fileChannel.readLong();
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return i;
  }

  @Override public int readInt(String filePath) {
    FSDataInputStream fileChannel = updateCache(filePath);
    int i = -1;
    try {
      i = fileChannel.readInt();
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return i;
  }
}
