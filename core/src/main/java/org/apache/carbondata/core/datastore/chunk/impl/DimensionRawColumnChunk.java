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

import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.AbstractRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.memory.MemoryException;

/**
 * Contains raw dimension data,
 * 1. The read uncompressed raw data of column chunk with all pages is stored in this instance.
 * 2. The raw data can be converted to processed chunk using decodeColumnPage method
 *  by specifying page number.
 */
public class DimensionRawColumnChunk extends AbstractRawColumnChunk {

  private DimensionColumnPage[] dataChunks;

  private DimensionColumnChunkReader chunkReader;

  private FileReader fileReader;

  public DimensionRawColumnChunk(int columnIndex, ByteBuffer rawData, long offSet, int length,
      DimensionColumnChunkReader columnChunkReader) {
    super(columnIndex, rawData, offSet, length);
    this.chunkReader = columnChunkReader;
  }

  /**
   * Convert all raw data with all pages to processed DimensionColumnPage's
   * @return
   */
  public DimensionColumnPage[] decodeAllColumnPages() {
    if (dataChunks == null) {
      dataChunks = new DimensionColumnPage[pagesCount];
    }
    for (int i = 0; i < pagesCount; i++) {
      try {
        if (dataChunks[i] == null) {
          dataChunks[i] = chunkReader.decodeColumnPage(this, i);
        }
      } catch (IOException | MemoryException e) {
        throw new RuntimeException(e);
      }
    }
    return dataChunks;
  }

  /**
   * Convert raw data with specified page number processed to DimensionColumnPage
   * @param pageNumber
   * @return
   */
  public DimensionColumnPage decodeColumnPage(int pageNumber) {
    assert pageNumber < pagesCount;
    if (dataChunks == null) {
      dataChunks = new DimensionColumnPage[pagesCount];
    }
    if (dataChunks[pageNumber] == null) {
      try {
        dataChunks[pageNumber] = chunkReader.decodeColumnPage(this, pageNumber);
      } catch (IOException | MemoryException e) {
        throw new RuntimeException(e);
      }
    }

    return dataChunks[pageNumber];
  }

  /**
   * Convert raw data with specified page number processed to DimensionColumnDataChunk
   *
   * @param index
   * @return
   */
  public DimensionColumnPage convertToDimColDataChunkWithOutCache(int index) {
    assert index < pagesCount;
    // in case of filter query filter column if filter column is decoded and stored.
    // then return the same
    if (dataChunks != null && null != dataChunks[index]) {
      return dataChunks[index];
    }
    try {
      return chunkReader.decodeColumnPage(this, index);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void freeMemory() {
    if (null != dataChunks) {
      for (int i = 0; i < dataChunks.length; i++) {
        if (dataChunks[i] != null) {
          dataChunks[i].freeMemory();
        }
      }
    }
    rawData = null;
  }

  public void setFileReader(FileReader fileReader) {
    this.fileReader = fileReader;
  }

  public FileReader getFileReader() {
    return fileReader;
  }
}
