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

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.sdk.file.cache.BlockletRows;

import org.apache.hadoop.mapreduce.InputSplit;

/**
 * CarbonData SDK reader with pagination support
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
public class PaginationCarbonReader<T> extends CarbonReader<T> {
  // Splits based the file present in the reader path when the reader is built.
  private List<InputSplit> allBlockletSplits;

  // Rows till the current splits stored as list.
  private List<Long> rowCountInSplits;

  // Reader builder used to create the pagination reader, used for building split level readers.
  private CarbonReaderBuilder readerBuilder;

  private boolean isClosed;

  // to store the rows of each blocklet in memory based LRU cache.
  // key: unique blocklet id
  // value: BlockletRows
  private CarbonLRUCache cache =
      new CarbonLRUCache(CarbonCommonConstants.CARBON_MAX_PAGINATION_LRU_CACHE_SIZE_IN_MB,
          CarbonCommonConstants.CARBON_MAX_PAGINATION_LRU_CACHE_SIZE_IN_MB_DEFAULT);

  /**
   * Call {@link #builder(String)} to construct an instance
   */

  PaginationCarbonReader(List<InputSplit> splits, CarbonReaderBuilder readerBuilder,
      List<Long> rowsInSplits) {
    // Initialize super class with no readers.
    // Based on the splits identified for pagination query, readers will be built for the query.
    super(null);
    this.allBlockletSplits = splits;
    this.readerBuilder = readerBuilder;
    // prepare the mapping.
    rowCountInSplits = rowsInSplits;
  }

  /**
   * Pagination query with from and to range.
   *
   * @param fromRowNumber must be greater than 0 (as row id starts from 1)
   *                      and less than or equals to toRowNumber
   * @param toRowNumber must be greater than 0 (as row id starts from 1)
   *                and greater than or equals to fromRowNumber
   *                and should not cross the total rows count
   * @return array of rows between fromRowNumber and toRowNumber (inclusive)
   * @throws Exception
   */
  public Object[] read(long fromRowNumber, long toRowNumber)
      throws IOException, InterruptedException {
    if (isClosed) {
      throw new RuntimeException("Pagination Reader is closed. please build again");
    }
    if (fromRowNumber < 1) {
      throw new IllegalArgumentException("from row id:" + fromRowNumber + " is less than 1");
    }
    if (fromRowNumber > toRowNumber) {
      throw new IllegalArgumentException(
          "from row id:" + fromRowNumber + " is greater than to row id:" + toRowNumber);
    }
    if (toRowNumber > getTotalRows()) {
      throw new IllegalArgumentException(
          "to row id:" + toRowNumber + " is greater than total rows:" + getTotalRows());
    }
    return getRows(fromRowNumber, toRowNumber);
  }

  /**
   * Get total rows in the folder or a list of CarbonData files.
   * It is based on the snapshot of files taken while building the reader.
   *
   * @return total rows from all the files in the reader.
   */
  public long getTotalRows() {
    if (isClosed) {
      throw new RuntimeException("Pagination Reader is closed. please build again");
    }
    if (rowCountInSplits.size() == 0) {
      return 0;
    }
    return rowCountInSplits.get(rowCountInSplits.size() - 1);
  }

  /**
   * This interface is for python to call java.
   * Because python cannot understand java Long object. so send string object.
   *
   * Get total rows in the folder or a list of CarbonData files.
   * It is based on the snapshot of files taken while building the reader.
   *
   *
   * @return total rows from all the files in the reader.
   */
  public String getTotalRowsAsString() {
    if (isClosed) {
      throw new RuntimeException("Pagination Reader is closed. please build again");
    }
    return (rowCountInSplits.get(rowCountInSplits.size() - 1)).toString();
  }

  private static int findBlockletIndex(List<Long> summationArray, Long key) {
    // summation array is in sorted order, so can use the binary search.
    int index = Collections.binarySearch(summationArray, key);
    if (index < 0) {
      // when key not found, binary search returns negative index [-1 to -N].
      // which is the possible place where key can be inserted.
      // with one shifted position. As 0 is also a valid index.
      // offset the one index shifted and get absolute value of it.
      index = Math.abs(index + 1);
    }
    return index;
  }

  private Range getBlockletIndexRange(long fromRowNumber, long toRowNumber) {
    // find the matching blocklet index range by checking with from and to.
    int upperBound = findBlockletIndex(rowCountInSplits, toRowNumber);
    // lower bound cannot be more than upper bound, so work on sub list.
    int lowerBound = findBlockletIndex(rowCountInSplits.subList(0, upperBound), fromRowNumber);
    return new Range(lowerBound, upperBound);
  }

  private Object[] getRows(long fromRowNumber, long toRowNumber)
      throws IOException, InterruptedException {
    int rowCount = 0;
    Object[] rows = new Object[(int)(toRowNumber - fromRowNumber + 1)];
    // get the matching split index (blocklets) range for the input range.
    Range blockletIndexRange = getBlockletIndexRange(fromRowNumber, toRowNumber);
    for (int i = blockletIndexRange.getFrom(); i <= blockletIndexRange.getTo(); i++) {
      String blockletUniqueId = String.valueOf(i);
      BlockletRows blockletRows;
      if (cache.get(blockletUniqueId) != null) {
        blockletRows = (BlockletRows)cache.get(blockletUniqueId);
      } else {
        BlockletDetailInfo detailInfo =
            ((CarbonInputSplit) allBlockletSplits.get(i)).getDetailInfo();
        List<Object> rowsInBlocklet = new ArrayList<>();
        // read the rows from the blocklet
        // TODO: read blocklets in multi-thread if there is a performance requirement.
        readerBuilder.setInputSplit(allBlockletSplits.get(i));
        CarbonReader<Object> carbonReader = readerBuilder.build();
        while (carbonReader.hasNext()) {
          rowsInBlocklet.add(carbonReader.readNextRow());
        }
        carbonReader.close();
        long fromRowId;
        if (i == 0) {
          fromRowId = 1;
        } else {
          // previous index will contain the sum of rows till previous blocklet.
          fromRowId = rowCountInSplits.get(i - 1) + 1;
        }
        blockletRows = new BlockletRows(fromRowId, detailInfo.getBlockSize(),
            rowsInBlocklet.toArray());
        // add entry to cache with no expiry time
        // key: unique blocklet id
        // value: BlockletRows
        cache.put(String.valueOf(i), blockletRows, blockletRows.getMemorySize(), Integer.MAX_VALUE);
      }
      long fromBlockletRow = blockletRows.getRowIdStartIndex();
      long toBlockletRow = fromBlockletRow + blockletRows.getRowsCount();
      Object[] rowsInBlocklet = blockletRows.getRows();
      if (toRowNumber >= toBlockletRow) {
        if (fromRowNumber >= fromBlockletRow) {
          // only fromRowNumber lies in this blocklet,
          // read from fromRowNumber to end of the blocklet.
          // -1 because row id starts form 0
          int start = (int) (fromRowNumber - blockletRows.getRowIdStartIndex());
          int end = blockletRows.getRowsCount();
          while (start < end) {
            rows[rowCount++] = rowsInBlocklet[start++];
          }
        } else {
          // both fromRowNumber and toRowNumber doesn't lie in this blocklet.
          // Read the whole blocklet.
          System.arraycopy(rowsInBlocklet, 0, rows, rowCount, rowsInBlocklet.length);
          rowCount += rowsInBlocklet.length;
        }
      } else {
        if (fromRowNumber >= fromBlockletRow) {
          // both fromRowNumber and toRowNumber exist in this blocklet itself.
          // prune it and fill the results.
          int start = (int) (fromRowNumber - blockletRows.getRowIdStartIndex());
          int end = (int) (start + (toRowNumber + 1 - fromRowNumber));
          while (start < end) {
            rows[rowCount++] = rowsInBlocklet[start++];
          }
        } else {
          // toRowNumber lies in this blocklet. Read from Starting of blocklet to toRowNumber.
          int start = 0;
          int end = (int) (toRowNumber + 1 - blockletRows.getRowIdStartIndex());
          while (start < end) {
            rows[rowCount++] = rowsInBlocklet[start++];
          }
        }
      }
    }
    return rows;
  }

  @Override
  public boolean hasNext() throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "cannot support this operation with pagination");
  }

  @Override
  public T readNextRow() throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "cannot support this operation with pagination");
  }

  @Override
  public Object[] readNextBatchRow() throws Exception {
    throw new UnsupportedOperationException(
        "cannot support this operation with pagination");
  }

  @Override
  public List<CarbonReader> split(int maxSplits) {
    throw new UnsupportedOperationException(
        "cannot support this operation with pagination");
  }

  /**
   * Closes the pagination reader, drops the cache and snapshot.
   * Need to build reader again if the files need to be read again.
   * call this when the all pagination queries are finished and can the drop cache.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (isClosed) {
      throw new RuntimeException("Pagination Reader is already closed");
    }
    cache.clear();
    rowCountInSplits = null;
    allBlockletSplits = null;
    isClosed = true;
  }

  private class Range {
    private int from;
    private int to;

    Range(int from, int to) {
      this.from = from;
      this.to = to;
    }

    public int getFrom() {
      return from;
    }

    public int getTo() {
      return to;
    }
  }

}

