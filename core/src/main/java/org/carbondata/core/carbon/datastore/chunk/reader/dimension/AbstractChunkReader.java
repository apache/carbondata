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
package org.carbondata.core.carbon.datastore.chunk.reader.dimension;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.datastore.chunk.reader.DimensionColumnChunkReader;
import org.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.Compressor;
import org.carbondata.core.datastorage.store.compression.SnappyCompression;
import org.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.carbondata.core.util.CarbonProperties;

/**
 * Class which will have all the common properties and behavior among all type
 * of reader
 */
public abstract class AbstractChunkReader implements DimensionColumnChunkReader {

  /**
   * compressor will be used to uncompress the data
   */
  protected static final Compressor<byte[]> COMPRESSOR =
      SnappyCompression.SnappyByteCompression.INSTANCE;

  /**
   * data chunk list which holds the information
   * about the data block metadata
   */
  protected List<DataChunk> dimensionColumnChunk;

  /**
   * size of the each column value
   * for no dictionary column it will be -1
   */
  protected int[] eachColumnValueSize;

  /**
   * full qualified path of the data file from
   * which data will be read
   */
  protected String filePath;

  /**
   * this will be used to uncompress the
   * row id and rle chunk
   */
  protected NumberCompressor numberComressor;

  /**
   * number of element in each chunk
   */
  private int numberOfElement;

  /**
   * Constructor to get minimum parameter to create
   * instance of this class
   *
   * @param dimensionColumnChunk dimension chunk metadata
   * @param eachColumnValueSize  size of the each column value
   * @param filePath             file from which data will be read
   */
  public AbstractChunkReader(List<DataChunk> dimensionColumnChunk, int[] eachColumnValueSize,
      String filePath) {
    this.dimensionColumnChunk = dimensionColumnChunk;
    this.eachColumnValueSize = eachColumnValueSize;
    this.filePath = filePath;
    int numberOfElement = 0;
    try {
      numberOfElement = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
              CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL));
    } catch (NumberFormatException exception) {
      numberOfElement = Integer.parseInt(CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL);
    }
    this.numberComressor = new NumberCompressor(numberOfElement);
  }

  /**
   * Below method will be used to create the inverted index reverse
   * this will be used to point to actual data in the chunk
   *
   * @param invertedIndex inverted index
   * @return reverse inverted index
   */
  protected int[] getInvertedReverseIndex(int[] invertedIndex) {
    int[] columnIndexTemp = new int[invertedIndex.length];

    for (int i = 0; i < invertedIndex.length; i++) {
      columnIndexTemp[invertedIndex[i]] = i;
    }
    return columnIndexTemp;
  }

  /**
   * In case of no dictionary column size of the each column value
   * will not be same, so in case of filter query we can not take
   * advantage of binary search as length with each value will be also
   * store with the data, so converting this data to two dimension
   * array format filter query processing will be faster
   *
   * @param dataChunkWithLength no dictionary column chunk
   *                            <Lenght><Data><Lenght><data>
   *                            Length will store in 2 bytes
   * @return list of data chuck, one value in list will represent one column value
   */
  protected List<byte[]> getNoDictionaryDataChunk(byte[] dataChunkWithLength) {
    List<byte[]> dataChunk = new ArrayList<byte[]>(numberOfElement);
    // wrapping the chunk to byte buffer
    ByteBuffer buffer = ByteBuffer.wrap(dataChunkWithLength);
    buffer.rewind();
    byte[] data = null;
    // iterating till all the elements are read
    while (buffer.hasRemaining()) {
      // as all the data is stored with length(2 bytes)
      // first reading the size and then based on size
      // we need to read the actual value
      data = new byte[buffer.getShort()];
      buffer.get(data);
      dataChunk.add(data);
    }
    return dataChunk;
  }
}
