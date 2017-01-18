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

package org.apache.carbondata.core.metadata.blocklet;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;

/**
 * class to store the information about the blocklet
 */
public class BlockletInfo implements Serializable {

  /**
   * serialization id
   */
  private static final long serialVersionUID = 1873135459695635381L;

  /**
   * Number of rows in this blocklet
   */
  private int numberOfRows;

  /**
   * Information about dimension chunk of all dimensions in this blocklet
   */
  private List<DataChunk> dimensionColumnChunk;

  /**
   * Information about measure chunk of all measures in this blocklet
   */
  private List<DataChunk> measureColumnChunk;

  private List<Long> dimensionChunkOffsets;

  private List<Short> dimensionChunksLength;

  private List<Long> measureChunkOffsets;

  private List<Short> measureChunksLength;

  /**
   * to store the index like min max and start and end key of each column of the blocklet
   */
  private BlockletIndex blockletIndex;

  /**
   * @return the numberOfRows
   */
  public int getNumberOfRows() {
    return numberOfRows;
  }

  /**
   * @param numberOfRows the numberOfRows to set
   */
  public void setNumberOfRows(int numberOfRows) {
    this.numberOfRows = numberOfRows;
  }

  /**
   * @return the dimensionColumnChunk
   */
  public List<DataChunk> getDimensionColumnChunk() {
    return dimensionColumnChunk;
  }

  /**
   * @param dimensionColumnChunk the dimensionColumnChunk to set
   */
  public void setDimensionColumnChunk(List<DataChunk> dimensionColumnChunk) {
    this.dimensionColumnChunk = dimensionColumnChunk;
  }

  /**
   * @return the measureColumnChunk
   */
  public List<DataChunk> getMeasureColumnChunk() {
    return measureColumnChunk;
  }

  /**
   * @param measureColumnChunk the measureColumnChunk to set
   */
  public void setMeasureColumnChunk(List<DataChunk> measureColumnChunk) {
    this.measureColumnChunk = measureColumnChunk;
  }

  /**
   * @return the blockletIndex
   */
  public BlockletIndex getBlockletIndex() {
    return blockletIndex;
  }

  /**
   * @param blockletIndex the blockletIndex to set
   */
  public void setBlockletIndex(BlockletIndex blockletIndex) {
    this.blockletIndex = blockletIndex;
  }

  public List<Long> getDimensionChunkOffsets() {
    return dimensionChunkOffsets;
  }

  public void setDimensionChunkOffsets(List<Long> dimensionChunkOffsets) {
    this.dimensionChunkOffsets = dimensionChunkOffsets;
  }

  public List<Short> getDimensionChunksLength() {
    return dimensionChunksLength;
  }

  public void setDimensionChunksLength(List<Short> dimensionChunksLength) {
    this.dimensionChunksLength = dimensionChunksLength;
  }

  public List<Long> getMeasureChunkOffsets() {
    return measureChunkOffsets;
  }

  public void setMeasureChunkOffsets(List<Long> measureChunkOffsets) {
    this.measureChunkOffsets = measureChunkOffsets;
  }

  public List<Short> getMeasureChunksLength() {
    return measureChunksLength;
  }

  public void setMeasureChunksLength(List<Short> measureChunksLength) {
    this.measureChunksLength = measureChunksLength;
  }

}
