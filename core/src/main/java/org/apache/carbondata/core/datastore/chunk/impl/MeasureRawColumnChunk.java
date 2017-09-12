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
 * 2. The raw data can be converted to processed chunk using convertToColumnPage method
 *  by specifying page number.
 */
public class MeasureRawColumnChunk extends AbstractRawColumnChunk {

  private ColumnPage[] columnPages;

  private MeasureColumnChunkReader chunkReader;

  private FileHolder fileReader;

  public MeasureRawColumnChunk(int columnIndex, ByteBuffer rawData, int offSet, int length,
      MeasureColumnChunkReader chunkReader) {
    super(columnIndex, rawData, offSet, length);
    this.chunkReader = chunkReader;
  }

  /**
   * Convert all raw data with all pages to processed ColumnPage
   */
  public ColumnPage[] convertToColumnPage() {
    if (columnPages == null) {
      columnPages = new ColumnPage[pagesCount];
    }
    for (int i = 0; i < pagesCount; i++) {
      try {
        if (columnPages[i] == null) {
          columnPages[i] = chunkReader.convertToColumnPage(this, i);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return columnPages;
  }

  /**
   * Convert raw data with specified `columnIndex` processed to ColumnPage
   */
  public ColumnPage convertToColumnPage(int columnIndex) {
    assert columnIndex < pagesCount;
    if (columnPages == null) {
      columnPages = new ColumnPage[pagesCount];
    }

    try {
      if (columnPages[columnIndex] == null) {
        columnPages[columnIndex] = chunkReader.convertToColumnPage(this, columnIndex);
      }
    } catch (IOException | MemoryException e) {
      throw new RuntimeException(e);
    }

    return columnPages[columnIndex];
  }

  @Override public void freeMemory() {
    if (null != columnPages) {
      for (int i = 0; i < columnPages.length; i++) {
        if (columnPages[i] != null) {
          columnPages[i].freeMemory();
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
