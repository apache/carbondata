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
package org.apache.carbondata.core.datastore.chunk.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.AbstractRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.memory.MemoryException;

/**
 * Contains raw dimension data,
 * 1. The read uncompressed raw data of column chunk with all pages is stored in this instance.
 * 2. The raw data can be converted to processed chunk using convertToDimColDataChunk method
 *  by specifying page number.
 */
public class DimensionRawColumnChunk extends AbstractRawColumnChunk {

  private DimensionColumnDataChunk[] dataChunks;

  private DimensionColumnChunkReader chunkReader;

  private FileHolder fileHolder;

  public DimensionRawColumnChunk(int blockletId, ByteBuffer rawData, int offSet, int length,
      DimensionColumnChunkReader columnChunkReader) {
    super(blockletId, rawData, offSet, length);
    this.chunkReader = columnChunkReader;
  }

  /**
   * Convert all raw data with all pages to processed DimensionColumnDataChunk's
   * @return
   */
  public DimensionColumnDataChunk[] convertToDimColDataChunks() {
    if (dataChunks == null) {
      dataChunks = new DimensionColumnDataChunk[pagesCount];
    }
    for (int i = 0; i < pagesCount; i++) {
      try {
        if (dataChunks[i] == null) {
          dataChunks[i] = chunkReader.convertToDimensionChunk(this, i);
        }
      } catch (IOException | MemoryException e) {
        throw new RuntimeException(e);
      }
    }
    return dataChunks;
  }

  /**
   * Convert raw data with specified page number processed to DimensionColumnDataChunk
   * @param index
   * @return
   */
  public DimensionColumnDataChunk convertToDimColDataChunk(int index) {
    assert index < pagesCount;
    if (dataChunks == null) {
      dataChunks = new DimensionColumnDataChunk[pagesCount];
    }
    if (dataChunks[index] == null) {
      try {
        dataChunks[index] = chunkReader.convertToDimensionChunk(this, index);
      } catch (IOException | MemoryException e) {
        throw new RuntimeException(e);
      }
    }

    return dataChunks[index];
  }

  @Override public void freeMemory() {
    if (null != dataChunks) {
      for (int i = 0; i < dataChunks.length; i++) {
        if (dataChunks[i] != null) {
          dataChunks[i].freeMemory();
        }
      }
    }
  }

  public void setFileHolder(FileHolder fileHolder) {
    this.fileHolder = fileHolder;
  }

  public FileHolder getFileReader() {
    return fileHolder;
  }
}
