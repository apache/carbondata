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
package org.apache.carbondata.core.datastore.chunk;

import java.nio.ByteBuffer;

import org.apache.carbondata.format.DataChunk3;

/**
 * It contains group of uncompressed blocklets on one column.
 */
public abstract class AbstractRawColumnChunk {

  private byte[][] minValues;

  private byte[][] maxValues;

  protected ByteBuffer rawData;

  private int[] offsets;

  private int[] rowCount;

  protected int pagesCount;

  protected int blockletId;

  private int offSet;

  protected int length;

  private DataChunk3 dataChunkV3;

  public AbstractRawColumnChunk(int blockletId, ByteBuffer rawData, int offSet, int length) {
    this.blockletId = blockletId;
    this.rawData = rawData;
    this.offSet = offSet;
    this.length = length;
  }

  public byte[][] getMinValues() {
    return minValues;
  }

  public void setMinValues(byte[][] minValues) {
    this.minValues = minValues;
  }

  public byte[][] getMaxValues() {
    return maxValues;
  }

  public void setMaxValues(byte[][] maxValues) {
    this.maxValues = maxValues;
  }

  public ByteBuffer getRawData() {
    return rawData;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public void setOffsets(int[] offsets) {
    this.offsets = offsets;
  }

  public int getPagesCount() {
    return pagesCount;
  }

  public void setPagesCount(int pagesCount) {
    this.pagesCount = pagesCount;
  }

  public int[] getRowCount() {
    return rowCount;
  }

  public void setRowCount(int[] rowCount) {
    this.rowCount = rowCount;
  }

  public abstract void freeMemory();

  public int getBlockletId() {
    return blockletId;
  }

  public int getOffSet() {
    return offSet;
  }

  public int getLength() {
    return length;
  }

  public DataChunk3 getDataChunkV3() {
    return dataChunkV3;
  }

  public void setDataChunkV3(DataChunk3 dataChunkV3) {
    this.dataChunkV3 = dataChunkV3;
  }

}
