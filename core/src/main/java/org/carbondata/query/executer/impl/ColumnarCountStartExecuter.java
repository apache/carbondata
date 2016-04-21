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

package org.carbondata.query.executer.impl;

import java.util.List;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.impl.CountAggregator;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.datastorage.TableDataStore;
import org.carbondata.query.executer.SliceExecuter;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.result.iterator.MemoryBasedResultIterator;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class ColumnarCountStartExecuter implements SliceExecuter {
  private List<InMemoryTable> slices;

  private String tableName;

  public ColumnarCountStartExecuter(List<InMemoryTable> slices, String tableName) {
    this.slices = slices;
    this.tableName = tableName;
  }

  @Override
  public CarbonIterator<QueryResult> executeSlices(List<SliceExecutionInfo> infos, int[] sliceIndex)
      throws QueryExecutionException {
    long count = 0;
    for (InMemoryTable slice : slices) {
      TableDataStore dataCache = slice.getDataCache(this.tableName);
      if (null != dataCache) {
        count += dataCache.getData().size();
      }
    }
    MeasureAggregator[] countAgg = new MeasureAggregator[1];
    countAgg[0] = new CountAggregator();
    countAgg[0].setNewValue(count);
    QueryResult result = new QueryResult();
    ByteArrayWrapper wrapper = new ByteArrayWrapper();
    wrapper.setMaskedKey(new byte[0]);
    result.add(wrapper, countAgg);
    return new MemoryBasedResultIterator(result);
  }

}
