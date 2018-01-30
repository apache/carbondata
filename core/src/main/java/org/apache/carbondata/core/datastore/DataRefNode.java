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
package org.apache.carbondata.core.datastore;

import java.io.IOException;

import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;

/**
 * Interface data block reference
 */
public interface DataRefNode {

  /**
   * Return the next data block in the tree, this can be used while scanning when
   * iterator of this class can be used iterate over blocks
   */
  DataRefNode getNextDataRefNode();

  /**
   * Return the number of rows in the data block
   */
  int numRows();

  /**
   * Return the block index. This can be used when multiple
   * thread can be used scan group of blocks in that can we can assign
   * some of the blocks to one thread and some to other
   */
  long nodeIndex();

  /**
   * Return the blocklet index in the node
   */
  short blockletIndex();

  /**
   * Return the number of pages
   */
  int numberOfPages();

  /**
   * Return the number of rows for a give page
   */
  int getPageRowCount(int pageNumber);

  /**
   * Return the max value of all the columns, this can
   * be used in case of filter query
   */
  byte[][] getColumnsMaxValue();

  /**
   * Return the min value of all the columns, this can
   * be used in case of filter query
   */
  byte[][] getColumnsMinValue();

  /**
   * Below method will be used to get the dimension chunks
   *
   * @param fileReader   file reader to read the chunks from file
   * @param columnIndexRange range indexes of the blocks need to be read
   *                     value can be {{0,10},{11,12},{13,13}}
   *                     here 0 to 10 and 11 to 12 column blocks will be read in one
   *                     IO operation 13th column block will be read separately
   *                     This will be helpful to reduce IO by reading bigger chunk of
   *                     data in one IO operation
   * @return dimension data chunks
   */
  DimensionRawColumnChunk[] readDimensionChunks(FileReader fileReader, int[][] columnIndexRange)
      throws IOException;

  /**
   * Below method will be used to get the dimension chunk
   *
   * @param fileReader file reader to read the chunk from file
   * @return dimension data chunk
   */
  DimensionRawColumnChunk readDimensionChunk(FileReader fileReader, int columnIndex)
      throws IOException;

  /**
   * Below method will be used to get the measure chunk
   *
   * @param fileReader   file reader to read the chunk from file
   * @param columnIndexRange range indexes of the blocks need to be read
   *                     value can be {{0,10},{11,12},{13,13}}
   *                     here 0 to 10 and 11 to 12 column blocks will be read in one
   *                     IO operation 13th column block will be read separately
   *                     This will be helpful to reduce IO by reading bigger chunk of
   *                     data in one IO operation
   * @return measure column data chunk
   */
  MeasureRawColumnChunk[] readMeasureChunks(FileReader fileReader, int[][] columnIndexRange)
      throws IOException;

  /**
   * Below method will be used to read the measure chunk
   *
   * @param fileReader file read to read the file chunk
   * @param columnIndex block index to be read from file
   * @return measure data chunk
   */
  MeasureRawColumnChunk readMeasureChunk(FileReader fileReader, int columnIndex) throws IOException;

}
