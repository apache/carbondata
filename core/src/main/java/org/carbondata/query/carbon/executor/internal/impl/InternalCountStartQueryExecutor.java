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
package org.carbondata.query.carbon.executor.internal.impl;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.impl.CountAggregator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.impl.ListBasedResult;
import org.carbondata.query.carbon.result.iterator.MemoryBasedResultIterator;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * Below class will be used to execute the count star query or any function query
 * like count(1) , in this case block scanning is not required.
 */
public class InternalCountStartQueryExecutor implements InternalQueryExecutor {

  /**
   * data block available for query execution
   */
  private List<AbstractIndex> blockList;

  public InternalCountStartQueryExecutor(List<AbstractIndex> blockList) {
    this.blockList = blockList;
  }

  /**
   * Method to execute the count start query
   *
   * @param block execution info
   * @param slice indexes
   */
  public CarbonIterator<Result> executeQuery(List<BlockExecutionInfo> infos, int[] sliceIndex)
      throws QueryExecutionException {
    long count = 0;
    // for each block get the total number of rows
    for (AbstractIndex tableBlock : this.blockList) {
      count += tableBlock.getTotalNumberOfRows();
    }
    // as this is a count start need to create counter star aggregator
    MeasureAggregator[] countAgg = new MeasureAggregator[1];
    countAgg[0] = new CountAggregator();
    countAgg[0].setNewValue(count);

    ListBasedResultWrapper resultWrapper = new ListBasedResultWrapper();
    Result<List<ListBasedResultWrapper>> result = new ListBasedResult();
    ByteArrayWrapper wrapper = new ByteArrayWrapper();
    wrapper.setDictionaryKey(new byte[0]);
    resultWrapper.setKey(wrapper);
    resultWrapper.setValue(countAgg);
    List<ListBasedResultWrapper> wrapperList = new ArrayList<ListBasedResultWrapper>(1);
    wrapperList.add(resultWrapper);
    result.addScannedResult(wrapperList);
    // returning the iterator over the result
    return new MemoryBasedResultIterator(result);
  }

}
