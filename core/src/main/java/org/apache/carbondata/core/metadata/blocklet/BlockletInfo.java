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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;

import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.hadoop.io.Writable;

/**
 * class to store the information about the blocklet
 */
public class BlockletInfo implements Serializable, Writable {

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

  private List<Integer> dimensionChunksLength;

  private List<Long> measureChunkOffsets;

  private List<Integer> measureChunksLength;

  /**
   * to store the index like min max and start and end key of each column of the blocklet
   */
  private BlockletIndex blockletIndex;

  /**
   * last dimension end offset
   */
  private long dimensionOffset;

  /**
   * last measure end offsets
   */
  private long measureOffsets;

  /**
   * number of pages in blocklet
   * default value is one for V1 and V2 version
   */
  private int numberOfPages = 1;

  private int[] numberOfRowsPerPage;

  private Boolean isSorted = true;

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

  public List<Integer> getDimensionChunksLength() {
    return dimensionChunksLength;
  }

  public void setDimensionChunksLength(List<Integer> dimensionChunksLength) {
    this.dimensionChunksLength = dimensionChunksLength;
  }

  public List<Long> getMeasureChunkOffsets() {
    return measureChunkOffsets;
  }

  public void setMeasureChunkOffsets(List<Long> measureChunkOffsets) {
    this.measureChunkOffsets = measureChunkOffsets;
  }

  public List<Integer> getMeasureChunksLength() {
    return measureChunksLength;
  }

  public void setMeasureChunksLength(List<Integer> measureChunksLength) {
    this.measureChunksLength = measureChunksLength;
  }

  public long getDimensionOffset() {
    return dimensionOffset;
  }

  public void setDimensionOffset(long dimensionOffset) {
    this.dimensionOffset = dimensionOffset;
  }

  public long getMeasureOffsets() {
    return measureOffsets;
  }

  public void setMeasureOffsets(long measureOffsets) {
    this.measureOffsets = measureOffsets;
  }

  public int getNumberOfPages() {
    return numberOfPages;
  }

  public void setNumberOfPages(int numberOfPages) {
    this.numberOfPages = numberOfPages;
  }

  @Override public void write(DataOutput output) throws IOException {
    output.writeLong(dimensionOffset);
    output.writeLong(measureOffsets);
    int dsize = dimensionChunkOffsets != null ? dimensionChunkOffsets.size() : 0;
    output.writeShort(dsize);
    for (int i = 0; i < dsize; i++) {
      output.writeLong(dimensionChunkOffsets.get(i));
    }
    for (int i = 0; i < dsize; i++) {
      output.writeInt(dimensionChunksLength.get(i));
    }
    int mSize = measureChunkOffsets != null ? measureChunkOffsets.size() : 0;
    output.writeShort(mSize);
    for (int i = 0; i < mSize; i++) {
      output.writeLong(measureChunkOffsets.get(i));
    }
    for (int i = 0; i < mSize; i++) {
      output.writeInt(measureChunksLength.get(i));
    }
    writeChunkInfoForOlderVersions(output);

    boolean isSortedPresent = (isSorted != null);
    output.writeBoolean(isSortedPresent);
    if (isSortedPresent) {
      output.writeBoolean(isSorted);
    }
    if (null != getNumberOfRowsPerPage()) {
      output.writeShort(getNumberOfRowsPerPage().length);
      for (int i = 0; i < getNumberOfRowsPerPage().length; i++) {
        output.writeInt(getNumberOfRowsPerPage()[i]);
      }
    } else {
      //for old store
      output.writeShort(0);
    }
  }

  /**
   * Serialize datachunks as well for older versions like V1 and V2
   */
  private void writeChunkInfoForOlderVersions(DataOutput output) throws IOException {
    int dimChunksSize = dimensionColumnChunk != null ? dimensionColumnChunk.size() : 0;
    output.writeShort(dimChunksSize);
    for (int i = 0; i < dimChunksSize; i++) {
      byte[] bytes = serializeDataChunk(dimensionColumnChunk.get(i));
      output.writeInt(bytes.length);
      output.write(bytes);
    }
    int msrChunksSize = measureColumnChunk != null ? measureColumnChunk.size() : 0;
    output.writeShort(msrChunksSize);
    for (int i = 0; i < msrChunksSize; i++) {
      byte[] bytes = serializeDataChunk(measureColumnChunk.get(i));
      output.writeInt(bytes.length);
      output.write(bytes);
    }
  }

  private byte[] serializeDataChunk(DataChunk chunk) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    ObjectOutputStream outputStream = new ObjectOutputStream(stream);
    outputStream.writeObject(chunk);
    outputStream.close();
    return stream.toByteArray();
  }

  private DataChunk deserializeDataChunk(byte[] bytes) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
    ObjectInputStream inputStream =
        new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), stream);
    DataChunk dataChunk = null;
    try {
      dataChunk = (DataChunk) inputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    inputStream.close();
    return dataChunk;
  }

  @Override public void readFields(DataInput input) throws IOException {
    dimensionOffset = input.readLong();
    measureOffsets = input.readLong();
    int dimensionChunkOffsetsSize = input.readShort();
    dimensionChunkOffsets = new ArrayList<>(dimensionChunkOffsetsSize);
    for (int i = 0; i < dimensionChunkOffsetsSize; i++) {
      dimensionChunkOffsets.add(input.readLong());
    }
    dimensionChunksLength = new ArrayList<>(dimensionChunkOffsetsSize);
    for (int i = 0; i < dimensionChunkOffsetsSize; i++) {
      dimensionChunksLength.add(input.readInt());
    }

    short measureChunkOffsetsSize = input.readShort();
    measureChunkOffsets = new ArrayList<>(measureChunkOffsetsSize);
    for (int i = 0; i < measureChunkOffsetsSize; i++) {
      measureChunkOffsets.add(input.readLong());
    }
    measureChunksLength = new ArrayList<>(measureChunkOffsetsSize);
    for (int i = 0; i < measureChunkOffsetsSize; i++) {
      measureChunksLength.add(input.readInt());
    }
    readChunkInfoForOlderVersions(input);
    final boolean isSortedPresent = input.readBoolean();
    if (isSortedPresent) {
      this.isSorted = input.readBoolean();
    }
    short pageCount = input.readShort();
    if (pageCount != 0) {
      // should set only for new store, old store will be set via setNumberOfRowsPerPage
      numberOfRowsPerPage = new int[pageCount];
      for (int i = 0; i < numberOfRowsPerPage.length; i++) {
        numberOfRowsPerPage[i] = input.readInt();
      }
    }
  }

  /**
   * Deserialize datachunks as well for older versions like V1 and V2
   */
  private void readChunkInfoForOlderVersions(DataInput input) throws IOException {
    short dimChunksSize = input.readShort();
    dimensionColumnChunk = new ArrayList<>(dimChunksSize);
    for (int i = 0; i < dimChunksSize; i++) {
      byte[] bytes = new byte[input.readInt()];
      input.readFully(bytes);
      dimensionColumnChunk.add(deserializeDataChunk(bytes));
    }
    short msrChunksSize = input.readShort();
    measureColumnChunk = new ArrayList<>(msrChunksSize);
    for (int i = 0; i < msrChunksSize; i++) {
      byte[] bytes = new byte[input.readInt()];
      input.readFully(bytes);
      measureColumnChunk.add(deserializeDataChunk(bytes));
    }
  }

  public int[] getNumberOfRowsPerPage() {
    return numberOfRowsPerPage;
  }

  public void setNumberOfRowsPerPage(int[] numberOfRowsPerPage) {
    this.numberOfRowsPerPage = numberOfRowsPerPage;
  }

  public Boolean isSorted() {
    return isSorted;
  }

  public void setSorted(Boolean sorted) {
    isSorted = sorted;
  }
}
