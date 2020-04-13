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
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * Reader for CarbonData file for pagination support
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
public class PaginationCarbonReader<T> extends CarbonReader<T> {

  private List<InputSplit> allBlockletSplits;

  private List<Long> rowCountInSplits;

  private CarbonReaderBuilder readerBuilder;

  /**
   * Call {@link #builder(String)} to construct an instance
   */

  PaginationCarbonReader(List<InputSplit> splits, CarbonReaderBuilder readerBuilder) {
    super(null);
    this.allBlockletSplits = splits;
    this.readerBuilder = readerBuilder;
    // prepare the mapping.
    rowCountInSplits = new ArrayList<>(splits.size());
    long sum = ((CarbonInputSplit) splits.get(0)).getDetailInfo().getRowCount();
    rowCountInSplits.add(sum);
    for (int i = 1; i < splits.size(); i++) {
      sum += ((CarbonInputSplit) splits.get(i)).getDetailInfo().getRowCount();
      rowCountInSplits.add(sum);
    }
  }

  private static int getIndex(List<Long> summationArray2, Long i2) {
    int index = Collections.binarySearch(summationArray2, i2);
    if (index < 0) {
      // binary search returns negative index [-1 to -N] if not found,
      // which is the possible place where data can be inserted
      // with one shifted position as 0 is also a valid index.
      index = index + 1; // offset the one index shifting
      index = Math.abs(index);
    }
    return index;
  }

  public Object[] read(int from, int to) throws Exception {
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
    return getRowsFromMappingBlocklets(from, to);
  }

  public long getTotalRows() {
    return rowCountInSplits.get(rowCountInSplits.size() - 1);
  }

  private Object[] getRowsFromMappingBlocklets(int from, int to)
      throws IOException, InterruptedException {
    int[] outputRowMapping = getOutputRowMapping(from, to);
    int fromSplitIndex = outputRowMapping[0];
    int toSplitIndex = outputRowMapping[1];
    long fromRowId;
    if (fromSplitIndex == 0) {
      fromRowId = 1;
    } else {
      fromRowId = rowCountInSplits.get(fromSplitIndex - 1) + 1;
    }
    // long toRowId = rowCountInSplits.get(toSplitIndex);
    Object[] rows = new Object[to - from + 1];
    int rowCount = 0;
    for (int i = fromSplitIndex; i <= toSplitIndex; i++) {
      readerBuilder.setInputSplit(allBlockletSplits.get(i));
      CarbonReader<Object> carbonReader = readerBuilder.build();
      // may be can read concurrently by multithread
      while (carbonReader.hasNext()) {
        Object row = carbonReader.readNextRow();
        // TODO: don't waste the other rows cache it
        if (fromRowId >= from && fromRowId <= to) {
          rows[rowCount++] = row;
        } else if (fromRowId > to) {
          break;
        }
        fromRowId++;
      }
      carbonReader.close();
    }
    return rows;
  }

  private int[] getOutputRowMapping(int from, int to) {
    int upperBound = getIndex(rowCountInSplits, (long)to);
    // lower bound cannot be more than upper bound, so work on sub list.
    int lowerBound = getIndex(rowCountInSplits.subList(0, upperBound), (long)from);
    return new int[] {lowerBound, upperBound};
  }

  @Override public boolean hasNext() throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "cannot support this operation with pagination support");
  }

  @Override public T readNextRow() throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "cannot support this operation with pagination support");
  }

  @Override public Object[] readNextBatchRow() throws Exception {
    throw new UnsupportedOperationException(
        "cannot support this operation with pagination support");
  }

  @Override public List<CarbonReader> split(int maxSplits) {
    throw new UnsupportedOperationException(
        "cannot support this operation with pagination support");
  }

  @Override public void close() throws IOException {
    // free the entry maps and anything specific
  }
}
