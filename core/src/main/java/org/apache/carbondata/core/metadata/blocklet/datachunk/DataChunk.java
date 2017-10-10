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
package org.apache.carbondata.core.metadata.blocklet.datachunk;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.encoder.Encoding;

/**
 * Class holds the information about the data chunk metadata
 */
public class DataChunk implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 1L;

  /**
   * whether this chunk is a row chunk or column chunk
   */
  private boolean isRowMajor;

  /**
   * Offset of data page
   */
  private long dataPageOffset;

  /**
   * length of data page
   */
  private int dataPageLength;

  /**
   * information about presence of values in each row of this column chunk
   */
  private BitSet nullValueIndexForColumn;

  /**
   * offset of row id page, only if encoded using inverted index
   */
  private long rowIdPageOffset;

  /**
   * length of row id page, only if encoded using inverted index
   */
  private int rowIdPageLength;

  /**
   * offset of rle page, only if RLE coded.
   */
  private long rlePageOffset;

  /**
   * length of rle page, only if RLE coded.
   */
  private int rlePageLength;

  /**
   * The List of encoders overriden at node level
   */
  private List<Encoding> encodingList;

  /**
   * value encoder meta which will holds the information
   * about max, min, decimal length, type
   */
  private List<ValueEncoderMeta> valueEncoderMetaList;

  /**
   * @return the isRowMajor
   */
  public boolean isRowMajor() {
    return isRowMajor;
  }

  /**
   * @param isRowMajor the isRowMajor to set
   */
  public void setRowMajor(boolean isRowMajor) {
    this.isRowMajor = isRowMajor;
  }

  /**
   * @return the dataPageOffset
   */
  public long getDataPageOffset() {
    return dataPageOffset;
  }

  /**
   * @param dataPageOffset the dataPageOffset to set
   */
  public void setDataPageOffset(long dataPageOffset) {
    this.dataPageOffset = dataPageOffset;
  }

  /**
   * @return the dataPageLength
   */
  public int getDataPageLength() {
    return dataPageLength;
  }

  /**
   * @param dataPageLength the dataPageLength to set
   */
  public void setDataPageLength(int dataPageLength) {
    this.dataPageLength = dataPageLength;
  }

  /**
   * @return the nullValueIndexForColumn
   */
  public BitSet getNullValueIndexForColumn() {
    return nullValueIndexForColumn;
  }

  /**
   * @param nullValueIndexForColumn the nullValueIndexForColumn to set
   */
  public void setNullValueIndexForColumn(BitSet nullValueIndexForColumn) {
    this.nullValueIndexForColumn = nullValueIndexForColumn;
  }

  /**
   * @return the rowIdPageOffset
   */
  public long getRowIdPageOffset() {
    return rowIdPageOffset;
  }

  /**
   * @param rowIdPageOffset the rowIdPageOffset to set
   */
  public void setRowIdPageOffset(long rowIdPageOffset) {
    this.rowIdPageOffset = rowIdPageOffset;
  }

  /**
   * @return the rowIdPageLength
   */
  public int getRowIdPageLength() {
    return rowIdPageLength;
  }

  /**
   * @param rowIdPageLength the rowIdPageLength to set
   */
  public void setRowIdPageLength(int rowIdPageLength) {
    this.rowIdPageLength = rowIdPageLength;
  }

  /**
   * @return the rlePageOffset
   */
  public long getRlePageOffset() {
    return rlePageOffset;
  }

  /**
   * @param rlePageOffset the rlePageOffset to set
   */
  public void setRlePageOffset(long rlePageOffset) {
    this.rlePageOffset = rlePageOffset;
  }

  /**
   * @return the rlePageLength
   */
  public int getRlePageLength() {
    return rlePageLength;
  }

  /**
   * @param rlePageLength the rlePageLength to set
   */
  public void setRlePageLength(int rlePageLength) {
    this.rlePageLength = rlePageLength;
  }

  /**
   * @return the encoderList
   */
  public List<Encoding> getEncodingList() {
    return encodingList;
  }

  /**
   * @param encodingList the encoderList to set
   */
  public void setEncodingList(List<Encoding> encodingList) {
    this.encodingList = encodingList;
  }

  /**
   * @return the valueEncoderMeta
   */
  public List<ValueEncoderMeta> getValueEncoderMeta() {
    return valueEncoderMetaList;
  }

  /**
   * @param valueEncoderMetaList the valueEncoderMeta to set
   */
  public void setValueEncoderMeta(List<ValueEncoderMeta> valueEncoderMetaList) {
    this.valueEncoderMetaList = valueEncoderMetaList;
  }

}
