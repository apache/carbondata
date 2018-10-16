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
package org.apache.carbondata.core.datastore.chunk.reader.dimension;

import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Class which will have all the common properties and behavior among all type
 * of reader
 */
public abstract class AbstractChunkReader implements DimensionColumnChunkReader {

  /**
   * compressor will be used to uncompress the data
   */
  protected Compressor compressor;

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
  protected int numberOfRows;

  /**
   * Constructor to get minimum parameter to create
   * instance of this class
   *
   * @param eachColumnValueSize  size of the each column value
   * @param filePath             file from which data will be read
   */
  public AbstractChunkReader(final int[] eachColumnValueSize, final String filePath,
      int numberOfRows) {
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
    this.numberOfRows = numberOfRows;
  }

  @Override
  public void decodeColumnPageAndFillVector(DimensionRawColumnChunk dimensionRawColumnChunk,
      int pageNumber, ColumnVectorInfo vectorInfo) throws IOException, MemoryException {
    throw new UnsupportedOperationException(
        "This operation is not supported in this reader " + this.getClass().getName());
  }
}
