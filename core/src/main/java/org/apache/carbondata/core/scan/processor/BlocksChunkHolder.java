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
package org.apache.carbondata.core.scan.processor;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * Block chunk holder which will hold the dimension and
 * measure chunk
 */
public class BlocksChunkHolder {

  /**
   * dimension column data chunk
   */
  private DimensionRawColumnChunk[] dimensionRawDataChunk;

  /**
   * measure column data chunk
   */
  private MeasureRawColumnChunk[] measureRawDataChunk;

  /**
   * file reader which will use to read the block from file
   */
  private FileHolder fileReader;

  /**
   * data block
   */
  private DataRefNode dataBlock;

  private BitSetGroup bitSetGroup;

  public BlocksChunkHolder(int numberOfDimensionBlock, int numberOfMeasureBlock,
      FileHolder fileReader) {
    dimensionRawDataChunk = new DimensionRawColumnChunk[numberOfDimensionBlock];
    measureRawDataChunk = new MeasureRawColumnChunk[numberOfMeasureBlock];
    this.fileReader = fileReader;
  }

  /**
   * @return the dimensionRawDataChunk
   */
  public DimensionRawColumnChunk[] getDimensionRawDataChunk() {
    return dimensionRawDataChunk;
  }

  /**
   * @param dimensionRawDataChunk the dimensionRawDataChunk to set
   */
  public void setDimensionRawDataChunk(DimensionRawColumnChunk[] dimensionRawDataChunk) {
    this.dimensionRawDataChunk = dimensionRawDataChunk;
  }

  /**
   * @return the measureRawDataChunk
   */
  public MeasureRawColumnChunk[] getMeasureRawDataChunk() {
    return measureRawDataChunk;
  }

  /**
   * @param measureRawDataChunk the measureRawDataChunk to set
   */
  public void setMeasureRawDataChunk(MeasureRawColumnChunk[] measureRawDataChunk) {
    this.measureRawDataChunk = measureRawDataChunk;
  }

  /**
   * @return the fileReader
   */
  public FileHolder getFileReader() {
    return fileReader;
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
    for (int i = 0; i < measureRawDataChunk.length; i++) {
      this.measureRawDataChunk[i] = null;
    }
    for (int i = 0; i < dimensionRawDataChunk.length; i++) {
      this.dimensionRawDataChunk[i] = null;
    }
  }

  public BitSetGroup getBitSetGroup() {
    return bitSetGroup;
  }

  public void setBitSetGroup(BitSetGroup bitSetGroup) {
    this.bitSetGroup = bitSetGroup;
  }
}
