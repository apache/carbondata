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

  PaginationCarbonReader(List<InputSplit> splits, CarbonReaderBuilder readerBuilder) {
    // Initialize super class with no readers.
    // Based on the splits identified for pagination query, readers will be built for the query.
    super(null);
    this.allBlockletSplits = splits;
    this.readerBuilder = readerBuilder;
    // prepare the mapping.
    rowCountInSplits = new ArrayList<>(splits.size());
    long sum = ((CarbonInputSplit) splits.get(0)).getDetailInfo().getRowCount();
    rowCountInSplits.add(sum);
    for (int i = 1; i < splits.size(); i++) {
      // prepare a summation array of row counts in each blocklet,
      // this is used for pruning with pagination vales.
      // At current index, it contains sum of rows of all the blocklet from previous + current.
      sum += ((CarbonInputSplit) splits.get(i)).getDetailInfo().getRowCount();
      rowCountInSplits.add(sum);
    }
  }

  /**
   * Pagination query with from and to range.
   *
   * @param from must be greater than 0 and <= to
   * @param to must be >= from and not outside the total rows
   * @return array of rows between from and to (inclusive)
   * @throws Exception
   */
  public Object[] read(long from, long to) throws IOException, InterruptedException {
    if (isClosed) {
      throw new RuntimeException("Pagination Reader is closed. please build again");
    }
    if (from < 1) {
      throw new IllegalArgumentException("from row id:" + from + " is less than 1");
    }
    if (from > to) {
      throw new IllegalArgumentException(
          "from row id:" + from + " is greater than to row id:" + to);
    }
    if (to > getTotalRows()) {
      throw new IllegalArgumentException(
          "to row id:" + to + " is greater than total rows:" + getTotalRows());
    }
    return getRows(from, to);
  }

  /**
   * Get total rows in the folder.
   * It is based on the snapshot of files taken while building the reader.
   *
   * @return total rows from all the files in the reader.
   */
  public long getTotalRows() {
    if (isClosed) {
      throw new RuntimeException("Pagination Reader is closed. please build again");
    }
    return rowCountInSplits.get(rowCountInSplits.size() - 1);
  }

  private static int findBlockletIndex(List<Long> summationArray2, Long key) {
    // summation array is in sorted order, so can use the binary search.
    int index = Collections.binarySearch(summationArray2, key);
    if (index < 0) {
      // when key not found, binary search returns negative index [-1 to -N].
      // which is the possible place where key can be inserted.
      // with one shifted position. As 0 is also a valid index.
      index = index + 1; // offset the one index shifted.
      index = Math.abs(index);
    }
    return index;
  }

  private int[] getBlockletIndexRange(long from, long to) {
    // find the matching blocklet index range by checking with from and to.
    int upperBound = findBlockletIndex(rowCountInSplits, to);
    // lower bound cannot be more than upper bound, so work on sub list.
    int lowerBound = findBlockletIndex(rowCountInSplits.subList(0, upperBound), from);
    return new int[] {lowerBound, upperBound};
  }

  private Object[] getRows(long from, long to)
      throws IOException, InterruptedException {
    int rowCount = 0;
    Object[] rows = new Object[(int)(to - from + 1)];
    // get the matching split index (blocklets) range for the input range.
    int[] blockletIndexRange = getBlockletIndexRange(from, to);
    for (int i = blockletIndexRange[0]; i <= blockletIndexRange[1]; i++) {
      String blockletUniqueId = String.valueOf(i);
      BlockletRows blockletRows;
      if (cache.get(blockletUniqueId) != null) {
        blockletRows = (BlockletRows)cache.get(blockletUniqueId);
      } else {
        BlockletDetailInfo detailInfo =
            ((CarbonInputSplit) allBlockletSplits.get(i)).getDetailInfo();
        int rowCountInBlocklet = detailInfo.getRowCount();
        Object[] rowsInBlocklet = new Object[rowCountInBlocklet];
        // read the rows from the blocklet
        // TODO: read blocklets in multi-thread if there is a performance requirement.
        readerBuilder.setInputSplit(allBlockletSplits.get(i));
        CarbonReader<Object> carbonReader = readerBuilder.build();
        int count = 0;
        while (carbonReader.hasNext()) {
          rowsInBlocklet[count++] = carbonReader.readNextRow();
        }
        carbonReader.close();
        long fromRowId;
        if (i == 0) {
          fromRowId = 1;
        } else {
          // previous index will contain the sum of rows till previous blocklet.
          fromRowId = rowCountInSplits.get(i - 1) + 1;
        }
        blockletRows = new BlockletRows(fromRowId, detailInfo.getBlockSize(), rowsInBlocklet);
        // add entry to cache with no expiry time
        // key: unique blocklet id
        // value: BlockletRows
        cache.put(String.valueOf(i), blockletRows, blockletRows.getMemorySize(), Integer.MAX_VALUE);
      }
      long fromRow = blockletRows.getRowIdStartIndex();
      long toRow = fromRow + blockletRows.getRowsCount();
      Object[] rowsInBlocklet = blockletRows.getRows();
      if (to > toRow) {
        if (from >= fromRow) {
          // only from index lies in this blocklet, read from from-index to end of the blocklet.
          // -1 because row id starts form 0
          int start = (int) (from - blockletRows.getRowIdStartIndex());
          int end = blockletRows.getRowsCount();
          while (start < end) {
            rows[rowCount++] = rowsInBlocklet[start++];
          }
        } else {
          // both from and to doesn't lie in this blocklet. Read the whole blocklet.
          for (Object row : rowsInBlocklet) {
            // TODO: better to do array copy instead of assign ?
            rows[rowCount++] = row;
          }
        }
      } else {
        if (from >= fromRow) {
          // both from and to index exist in this blocklet itself. prune it and fill the results.
          int start = (int) (from - blockletRows.getRowIdStartIndex());
          int end = (int) (start + (to + 1 - from));
          while (start < end) {
            rows[rowCount++] = rowsInBlocklet[start++];
          }
        } else {
          // to index lies in this blocklet. Read from Starting of blocklet to to-index.
          int start = 0;
          int end = (int) (to + 1 - blockletRows.getRowIdStartIndex());
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
   * Suggest to call this when new files are added in the folder.
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
}
