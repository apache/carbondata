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

package org.apache.carbondata.core.util;

import java.util.BitSet;

import org.apache.carbondata.core.datastore.compression.WriterCompressModel;

public class NodeHolder {
  /**
   * keyArray
   */
  private byte[][] keyArray;

  /**
   * dataArray
   */
  private byte[][] dataArray;

  /**
   * measureLenght
   */
  private int[] measureLenght;

  /**
   * startKey
   */
  private byte[] startKey;

  /**
   * endKey
   */
  private byte[] endKey;

  /**
   * entryCount
   */
  private int entryCount;
  /**
   * keyLenghts
   */
  private int[] keyLengths;

  /**
   * keyIndexBlockLenght
   */
  private int[] keyBlockIndexLength;

  /**
   * isSortedKeyBlock
   */
  private boolean[] isSortedKeyBlock;

  private byte[][] compressedIndex;

  private byte[][] compressedIndexMap;

  /**
   * dataIndexMap
   */
  private int[] dataIndexMapLength;

  /**
   * compressedDataIndex
   */
  private byte[][] compressedDataIndex;

  /**
   * column max data
   */
  private byte[][] columnMaxData;

  /**
   * column min data
   */
  private byte[][] columnMinData;

  private byte[][] measureColumnMaxData;

  private byte[][] measureColumnMinData;

  /**
   * compression model for numbers data block.
   */
  private WriterCompressModel compressionModel;

  /**
   * array of aggBlocks flag to identify the aggBlocks
   */
  private boolean[] aggBlocks;

  /**
   * true if given index is colgroup block
   */
  private boolean[] colGrpBlocks;

  /**
   * bit set which will holds the measure
   * indexes which are null
   */
  private BitSet[] measureNullValueIndex;

  /**
   * total length of dimension values
   */
  private int totalDimensionArrayLength;

  /**
   * total length of all measure values
   */
  private int totalMeasureArrayLength;

  /**
   * data size this node holder is holding
   */
  private int holderSize;

  /**
   * to check all the pages to be
   * written, this will be used for v3 format
   */
  private boolean writeAll;

  /**
   * @return the keyArray
   */
  public byte[][] getKeyArray() {
    return keyArray;
  }

  /**
   * @param keyArray the keyArray to set
   */
  public void setKeyArray(byte[][] keyArray) {
    this.keyArray = keyArray;
  }

  /**
   * @return the dataArray
   */
  public byte[][] getDataArray() {
    return dataArray;
  }

  /**
   * @param dataArray the dataArray to set
   */
  public void setDataArray(byte[][] dataArray) {
    this.dataArray = dataArray;
  }

  /**
   * @return the measureLenght
   */
  public int[] getMeasureLenght() {
    return measureLenght;
  }

  /**
   * @param measureLenght the measureLenght to set
   */
  public void setMeasureLenght(int[] measureLenght) {
    this.measureLenght = measureLenght;
  }

  /**
   * @return the startKey
   */
  public byte[] getStartKey() {
    return startKey;
  }

  /**
   * @param startKey the startKey to set
   */
  public void setStartKey(byte[] startKey) {
    this.startKey = startKey;
  }

  /**
   * @return the endKey
   */
  public byte[] getEndKey() {
    return endKey;
  }

  /**
   * @param endKey the endKey to set
   */
  public void setEndKey(byte[] endKey) {
    this.endKey = endKey;
  }

  /**
   * @return the entryCount
   */
  public int getEntryCount() {
    return entryCount;
  }

  /**
   * @param entryCount the entryCount to set
   */
  public void setEntryCount(int entryCount) {
    this.entryCount = entryCount;
  }

  /**
   * @return the keyLenghts
   */
  public int[] getKeyLengths() {
    return keyLengths;
  }

  public void setKeyLengths(int[] keyLengths) {
    this.keyLengths = keyLengths;
  }

  /**
   * @return the keyBlockIndexLength
   */
  public int[] getKeyBlockIndexLength() {
    return keyBlockIndexLength;
  }

  /**
   * @param keyBlockIndexLength the keyBlockIndexLength to set
   */
  public void setKeyBlockIndexLength(int[] keyBlockIndexLength) {
    this.keyBlockIndexLength = keyBlockIndexLength;
  }

  /**
   * @return the isSortedKeyBlock
   */
  public boolean[] getIsSortedKeyBlock() {
    return isSortedKeyBlock;
  }

  /**
   * @param isSortedKeyBlock the isSortedKeyBlock to set
   */
  public void setIsSortedKeyBlock(boolean[] isSortedKeyBlock) {
    this.isSortedKeyBlock = isSortedKeyBlock;
  }

  /**
   * @return the compressedIndexex
   */
  public byte[][] getCompressedIndex() {
    return compressedIndex;
  }

  public void setCompressedIndex(byte[][] compressedIndex) {
    this.compressedIndex = compressedIndex;
  }

  /**
   * @return the compressedIndexMap
   */
  public byte[][] getCompressedIndexMap() {
    return compressedIndexMap;
  }

  /**
   * @param compressedIndexMap the compressedIndexMap to set
   */
  public void setCompressedIndexMap(byte[][] compressedIndexMap) {
    this.compressedIndexMap = compressedIndexMap;
  }

  /**
   * @return the compressedDataIndex
   */
  public byte[][] getCompressedDataIndex() {
    return compressedDataIndex;
  }

  /**
   * @param compressedDataIndex the compressedDataIndex to set
   */
  public void setCompressedDataIndex(byte[][] compressedDataIndex) {
    this.compressedDataIndex = compressedDataIndex;
  }

  /**
   * @return the dataIndexMapLength
   */
  public int[] getDataIndexMapLength() {
    return dataIndexMapLength;
  }

  /**
   * @param dataIndexMapLength the dataIndexMapLength to set
   */
  public void setDataIndexMapLength(int[] dataIndexMapLength) {
    this.dataIndexMapLength = dataIndexMapLength;
  }

  public byte[][] getColumnMaxData() {
    return this.columnMaxData;
  }

  public void setColumnMaxData(byte[][] columnMaxData) {
    this.columnMaxData = columnMaxData;
  }

  public byte[][] getColumnMinData() {
    return this.columnMinData;
  }

  public void setColumnMinData(byte[][] columnMinData) {
    this.columnMinData = columnMinData;
  }

  public WriterCompressModel getCompressionModel() {
    return compressionModel;
  }

  public void setCompressionModel(WriterCompressModel compressionModel) {
    this.compressionModel = compressionModel;
  }

  /**
   * returns array of aggBlocks flag to identify the aag blocks
   *
   * @return
   */
  public boolean[] getAggBlocks() {
    return aggBlocks;
  }

  /**
   * set array of aggBlocks flag to identify the aggBlocks
   *
   * @param aggBlocks
   */
  public void setAggBlocks(boolean[] aggBlocks) {
    this.aggBlocks = aggBlocks;
  }

  /**
   * @return
   */
  public boolean[] getColGrpBlocks() {
    return this.colGrpBlocks;
  }

  /**
   * @param colGrpBlock true if block is column group
   */
  public void setColGrpBlocks(boolean[] colGrpBlock) {
    this.colGrpBlocks = colGrpBlock;
  }

  /**
   * @return the measureNullValueIndex
   */
  public BitSet[] getMeasureNullValueIndex() {
    return measureNullValueIndex;
  }

  /**
   * @param measureNullValueIndex the measureNullValueIndex to set
   */
  public void setMeasureNullValueIndex(BitSet[] measureNullValueIndex) {
    this.measureNullValueIndex = measureNullValueIndex;
  }

  public int getTotalDimensionArrayLength() {
    return totalDimensionArrayLength;
  }

  public void setTotalDimensionArrayLength(int totalDimensionArrayLength) {
    this.totalDimensionArrayLength = totalDimensionArrayLength;
  }

  public int getTotalMeasureArrayLength() {
    return totalMeasureArrayLength;
  }

  public void setTotalMeasureArrayLength(int totalMeasureArrayLength) {
    this.totalMeasureArrayLength = totalMeasureArrayLength;
  }

  public byte[][] getMeasureColumnMaxData() {
    return measureColumnMaxData;
  }

  public void setMeasureColumnMaxData(byte[][] measureColumnMaxData) {
    this.measureColumnMaxData = measureColumnMaxData;
  }

  public byte[][] getMeasureColumnMinData() {
    return measureColumnMinData;
  }

  public void setMeasureColumnMinData(byte[][] measureColumnMinData) {
    this.measureColumnMinData = measureColumnMinData;
  }

  public int getHolderSize() {
    return holderSize;
  }

  public void setHolderSize(int holderSize) {
    this.holderSize = holderSize;
  }

  public void setWriteAll(boolean writeAll) {
    this.writeAll = writeAll;
  }
  public boolean isWriteAll() {
    return this.writeAll;
  }
}
