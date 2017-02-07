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
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;

/**
 * Contains raw measure data
 */
public class MeasureRawColumnChunk extends AbstractRawColumnChunk {

  private MeasureColumnDataChunk[] dataChunks;

  private MeasureColumnChunkReader chunkReader;

  private FileHolder fileReader;

  public MeasureRawColumnChunk(int blockId, ByteBuffer rawData, int offSet, int length,
      MeasureColumnChunkReader chunkReader) {
    super(blockId, rawData, offSet, length);
    this.chunkReader = chunkReader;
  }

  public MeasureColumnDataChunk[] convertToMeasureColDataChunks() {
    if (dataChunks == null) {
      dataChunks = new MeasureColumnDataChunk[pagesCount];
    }
    for (int i = 0; i < pagesCount; i++) {
      try {
        if (dataChunks[i] == null) {
          dataChunks[i] =
              chunkReader.convertToMeasureChunk(this, i);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return dataChunks;
  }

  public MeasureColumnDataChunk convertToMeasureColDataChunk(int index) {
    assert index < pagesCount;
    if (dataChunks == null) {
      dataChunks = new MeasureColumnDataChunk[pagesCount];
    }

    try {
      if (dataChunks[index] == null) {
        dataChunks[index] =
            chunkReader.convertToMeasureChunk(this, index);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return dataChunks[index];
  }

  @Override public void freeMemory() {
    for (int i = 0; i < dataChunks.length; i++) {
      if (dataChunks[i] != null) {
        dataChunks[i].freeMemory();
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
