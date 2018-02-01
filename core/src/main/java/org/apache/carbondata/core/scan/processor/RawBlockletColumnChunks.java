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
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * Contains dimension and measure raw column chunks of one blocklet
 */
public class RawBlockletColumnChunks {

  /**
   * dimension column data chunk
   */
  private DimensionRawColumnChunk[] dimensionRawColumnChunks;

  /**
   * measure column data chunk
   */
  private MeasureRawColumnChunk[] measureRawColumnChunks;

  /**
   * file reader which will use to read the block from file
   */
  private FileReader fileReader;

  /**
   * data block
   */
  private DataRefNode dataBlock;

  private BitSetGroup bitSetGroup;

  private RawBlockletColumnChunks() { }

  public static RawBlockletColumnChunks newInstance(int numberOfDimensionChunk,
      int numberOfMeasureChunk, FileReader fileReader, DataRefNode dataBlock) {
    RawBlockletColumnChunks instance = new RawBlockletColumnChunks();
    instance.dimensionRawColumnChunks = new DimensionRawColumnChunk[numberOfDimensionChunk];
    instance.measureRawColumnChunks = new MeasureRawColumnChunk[numberOfMeasureChunk];
    instance.fileReader = fileReader;
    instance.dataBlock = dataBlock;
    return instance;
  }

  /**
   * @return the dimensionRawColumnChunks
   */
  public DimensionRawColumnChunk[] getDimensionRawColumnChunks() {
    return dimensionRawColumnChunks;
  }

  /**
   * @param dimensionRawColumnChunks the dimensionRawColumnChunks to set
   */
  public void setDimensionRawColumnChunks(DimensionRawColumnChunk[] dimensionRawColumnChunks) {
    this.dimensionRawColumnChunks = dimensionRawColumnChunks;
  }

  /**
   * @return the measureRawColumnChunks
   */
  public MeasureRawColumnChunk[] getMeasureRawColumnChunks() {
    return measureRawColumnChunks;
  }

  /**
   * @param measureRawColumnChunks the measureRawColumnChunks to set
   */
  public void setMeasureRawColumnChunks(MeasureRawColumnChunk[] measureRawColumnChunks) {
    this.measureRawColumnChunks = measureRawColumnChunks;
  }

  /**
   * @return the fileReader
   */
  public FileReader getFileReader() {
    return fileReader;
  }

  /**
   * @return the dataBlock
   */
  public DataRefNode getDataBlock() {
    return dataBlock;
  }

  public BitSetGroup getBitSetGroup() {
    return bitSetGroup;
  }

  public void setBitSetGroup(BitSetGroup bitSetGroup) {
    this.bitSetGroup = bitSetGroup;
  }
}
