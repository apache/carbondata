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
package org.carbondata.query.carbon.scanner;

import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.datastorage.store.FileHolder;

/**
 * Block chunk holder which will hold the dimension and
 * measure chunk
 */
public class BlocksChunkHolder {

  /**
   * dimension column data chunk
   */
  private DimensionColumnDataChunk[] dimensionDataChunk;

  /**
   * measure column data chunk
   */
  private MeasureColumnDataChunk[] measureDataChunk;

  /**
   * file reader which will use to read the block from file
   */
  private FileHolder fileReader;

  /**
   * data block
   */
  private DataRefNode dataBlock;

  public BlocksChunkHolder(int numberOfDimensionBlock, int numberOfMeasureBlock) {
    dimensionDataChunk = new DimensionColumnDataChunk[numberOfDimensionBlock];
    measureDataChunk = new MeasureColumnDataChunk[numberOfMeasureBlock];
  }

  /**
   * @return the dimensionDataChunk
   */
  public DimensionColumnDataChunk[] getDimensionDataChunk() {
    return dimensionDataChunk;
  }

  /**
   * @param dimensionDataChunk the dimensionDataChunk to set
   */
  public void setDimensionDataChunk(DimensionColumnDataChunk[] dimensionDataChunk) {
    this.dimensionDataChunk = dimensionDataChunk;
  }

  /**
   * @return the measureDataChunk
   */
  public MeasureColumnDataChunk[] getMeasureDataChunk() {
    return measureDataChunk;
  }

  /**
   * @param measureDataChunk the measureDataChunk to set
   */
  public void setMeasureDataChunk(MeasureColumnDataChunk[] measureDataChunk) {
    this.measureDataChunk = measureDataChunk;
  }

  /**
   * @return the fileReader
   */
  public FileHolder getFileReader() {
    return fileReader;
  }

  /**
   * @param fileReader the fileReader to set
   */
  public void setFileReader(FileHolder fileReader) {
    this.fileReader = fileReader;
  }

  /**
   * @return the dataBlock
   */
  public DataRefNode getDataBlock() {
    return dataBlock;
  }

  /**
   * @param dataBlock the dataBlock to set
   */
  public void setDataBlock(DataRefNode dataBlock) {
    this.dataBlock = dataBlock;
  }

  /***
   * To reset the measure chunk and dimension chunk
   * array
   */
  public void reset() {
    for (int i = 0; i < measureDataChunk.length; i++) {
      this.measureDataChunk[i] = null;
    }
    for (int i = 0; i < dimensionDataChunk.length; i++) {
      this.dimensionDataChunk[i] = null;
    }
  }
}
