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
import java.util.BitSet;

import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;

/**
 * Interface data block reference
 */
public interface DataRefNode {

  /**
   * Method to get the next block this can be used while scanning when
   * iterator of this class can be used iterate over blocks
   *
   * @return next block
   */
  DataRefNode getNextDataRefNode();

  /**
   * to get the number of keys tuples present in the block
   *
   * @return number of keys in the block
   */
  int nodeSize();

  /**
   * Method can be used to get the block index .This can be used when multiple
   * thread can be used scan group of blocks in that can we can assign the
   * some of the blocks to one thread and some to other
   *
   * @return block number
   */
  long nodeNumber();

  /**
   * Method is used for retreiving the BlockletId.
   * @return the blockletid related to the data block.
   */
  String blockletId();

  /**
   * This method will be used to get the max value of all the columns this can
   * be used in case of filter query
   *
   */
  byte[][] getColumnsMaxValue();

  /**
   * This method will be used to get the min value of all the columns this can
   * be used in case of filter query
   *
   */
  byte[][] getColumnsMinValue();

  /**
   * This method will be used to get the Null values of all the columns present
   * in the blocklet. This is used for Measure Filter Queries where is Null or
   * is Not Null is being prunned. Driver side will be able to prune the blocklets
   * based in this value.
   * @return
   */
  BitSet getColumnsNullValue();

  /**
   * Below method will be used to get the dimension chunks
   *
   * @param fileReader   file reader to read the chunks from file
   * @param blockIndexes range indexes of the blocks need to be read
   *                     value can be {{0,10},{11,12},{13,13}}
   *                     here 0 to 10 and 11 to 12 column blocks will be read in one
   *                     IO operation 13th column block will be read separately
   *                     This will be helpful to reduce IO by reading bigger chunk of
   *                     data in On IO
   * @return dimension data chunks
   */
  DimensionRawColumnChunk[] getDimensionChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException;

  /**
   * Below method will be used to get the dimension chunk
   *
   * @param fileReader file reader to read the chunk from file
   * @return dimension data chunk
   */
  DimensionRawColumnChunk getDimensionChunk(FileHolder fileReader, int blockIndexes)
      throws IOException;

  /**
   * Below method will be used to get the measure chunk
   *
   * @param fileReader   file reader to read the chunk from file
   * @param blockIndexes range indexes of the blocks need to be read
   *                     value can be {{0,10},{11,12},{13,13}}
   *                     here 0 to 10 and 11 to 12 column blocks will be read in one
   *                     IO operation 13th column block will be read separately
   *                     This will be helpful to reduce IO by reading bigger chunk of
   *                     data in On IO
   * @return measure column data chunk
   */
  MeasureRawColumnChunk[] getMeasureChunks(FileHolder fileReader, int[][] blockIndexes)
      throws IOException;

  /**
   * Below method will be used to read the measure chunk
   *
   * @param fileReader file read to read the file chunk
   * @param blockIndex block index to be read from file
   * @return measure data chunk
   */
  MeasureRawColumnChunk getMeasureChunk(FileHolder fileReader, int blockIndex) throws IOException;

  /**
   * @param deleteDeltaDataCache
   */
  void setDeleteDeltaDataCache(BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache);

  /**
   * @return
   */
  BlockletLevelDeleteDeltaDataCache getDeleteDeltaDataCache();

  /**
   * number of pages in blocklet
   * @return
   */
  int numberOfPages();

  /**
   * Return the number of rows for a give page
   *
   * @param pageNumber
   * @return
   */
  int getPageRowCount(int pageNumber);
}
