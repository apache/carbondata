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
package org.apache.carbondata.core.datastorage.store.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.FileHolder;

public class MemoryMappedFileHolderImpl implements FileHolder {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MemoryMappedFileHolderImpl.class.getName());

  private Map<String, FileChannel> fileNameAndStreamCache;
  private Map<String, MappedByteBuffer> fileNameAndMemoryMappedFileCache;

  public MemoryMappedFileHolderImpl() {
    this(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  public MemoryMappedFileHolderImpl(int capacity) {
    this.fileNameAndStreamCache = new HashMap<String, FileChannel>(capacity);
    this.fileNameAndMemoryMappedFileCache = new HashMap<String, MappedByteBuffer>(capacity);
  }

  private MappedByteBuffer updateCache(String filePath) {
    MappedByteBuffer byteBuffer = fileNameAndMemoryMappedFileCache.get(filePath);
    try {
      if (null == byteBuffer) {
        FileChannel fileChannel = new RandomAccessFile(filePath, "r").getChannel();
        byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        fileNameAndStreamCache.put(filePath, fileChannel);
        fileNameAndMemoryMappedFileCache.put(filePath, byteBuffer);
      }
    } catch (IOException e) {
      LOGGER.error(e, e.getMessage());
    }
    return byteBuffer;
  }

  @Override
  public byte[] readByteArray(String filePath, long offset, int length) {
    byte[] dst = new byte[length];
    updateCache(filePath).get(dst, (int)offset, length);
    return dst;
  }

  @Override
  public byte[] readByteArray(String filePath, int length) {
    byte[] dst = new byte[length];
    updateCache(filePath).get(dst);
    return dst;
  }

  @Override
  public int readInt(String filePath, long offset) {
    byte[] dst = readByteArray(filePath, offset, CarbonCommonConstants.INT_SIZE_IN_BYTE);
    return ByteBuffer.wrap(dst).getInt();
  }

  @Override
  public long readLong(String filePath, long offset) {
    byte[] dst = readByteArray(filePath, offset, CarbonCommonConstants.LONG_SIZE_IN_BYTE);
    return ByteBuffer.wrap(dst).getLong();
  }

  @Override
  public int readInt(String filePath) {
    return updateCache(filePath).getInt();
  }

  @Override
  public long readDouble(String filePath, long offset) {
    byte[] dst = readByteArray(filePath, offset, CarbonCommonConstants.LONG_SIZE_IN_BYTE);
    return ByteBuffer.wrap(dst).getLong();
  }

  @Override
  public void finish() {
    fileNameAndMemoryMappedFileCache.clear();
    for (Entry<String, FileChannel> entry : fileNameAndStreamCache.entrySet()) {
      try {
        FileChannel channel = entry.getValue();
        if (null != channel) {
          channel.close();
        }
      } catch (IOException exception) {
        LOGGER.error(exception, exception.getMessage());
      }
    }
  }
}
