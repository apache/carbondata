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
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.MemoryException;

/**
 * Contains raw measure data
 * 1. The read uncompressed raw data of column chunk with all pages is stored in this instance.
 * 2. The raw data can be converted to processed chunk using convertToMeasureColDataChunk method
 *  by specifying page number.
 */
public class MeasureRawColumnChunk extends AbstractRawColumnChunk {

  private ColumnPage[] dataChunks;

  private MeasureColumnChunkReader chunkReader;

  private FileHolder fileReader;

  public MeasureRawColumnChunk(int blockId, ByteBuffer rawData, int offSet, int length,
      MeasureColumnChunkReader chunkReader) {
    super(blockId, rawData, offSet, length);
    this.chunkReader = chunkReader;
  }

  /**
   * Convert all raw data with all pages to processed ColumnPage
   * @return
   */
  public ColumnPage[] convertToMeasureColDataChunks() {
    if (dataChunks == null) {
      dataChunks = new ColumnPage[pagesCount];
    }
    for (int i = 0; i < pagesCount; i++) {
      try {
        if (dataChunks[i] == null) {
          dataChunks[i] = chunkReader.convertToMeasureChunk(this, i);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return dataChunks;
  }

  /**
   * Convert raw data with specified page number processed to ColumnPage
   * @param index
   * @return
   */
  public ColumnPage convertToMeasureColDataChunk(int index) {
    assert index < pagesCount;
    if (dataChunks == null) {
      dataChunks = new ColumnPage[pagesCount];
    }

    try {
      if (dataChunks[index] == null) {
        dataChunks[index] = chunkReader.convertToMeasureChunk(this, index);
      }
    } catch (IOException | MemoryException e) {
      throw new RuntimeException(e);
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

  public void setFileReader(FileHolder fileReader) {
    this.fileReader = fileReader;
  }

  public FileHolder getFileReader() {
    return fileReader;
  }
}
