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
package org.apache.carbondata.core.scan.result.iterator;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.processor.impl.DataBlockIteratorImpl;
import org.apache.carbondata.core.scan.processor.impl.SortMdkDataBlockIteratorImpl;
import org.apache.carbondata.core.scan.result.BatchResult;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public class DetailQuerySortMdkResultIterator
    extends AbstractDetailQueryResultIterator<BatchResult> {

  private final Object lock = new Object();
  private SortOrderType sortType = SortOrderType.ASC;

  public DetailQuerySortMdkResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel,
      ExecutorService execService) {
    super(infos, queryModel, execService);
    // according to limit value to reduce the batch size
    resetBatchSizeByLimit(queryModel.getLimit());

    // sort by MDK key only consider these sort dimensions have the same order
    // type
    QueryDimension singleSortDimesion = queryModel.getSortMdkDimensions().get(0);
    sortType = singleSortDimesion.getSortOrder();
  }

  @Override
  public BatchResult next() {
    BatchResult batchResult = getBatchResult();
    this.decreaseLimit(batchResult.getSize());
    return batchResult;
  }

  @Override
  public boolean hasNext() {
    if (limitFlg && limit <= 0) {
      return false;
    }
    if (dataBlockIterator != null && dataBlockIterator.hasNext()) {
      return true;
    } else if (blockExecutionInfos.size() > 0) {
      return true;
    } else {
      return false;
    }
  }

  private BatchResult getBatchResult() {
    BatchResult batchResult = new BatchResult();
    synchronized (lock) {
      updateDataBlockIterator();
      if (dataBlockIterator != null) {
        batchResult.setRows(dataBlockIterator.next());
      }
    }
    return batchResult;
  }

  protected DataBlockIteratorImpl getDataBlockIterator(BlockExecutionInfo executionInfo) {
    return new SortMdkDataBlockIteratorImpl(executionInfo, fileReader, batchSize,
        queryStatisticsModel, execService, sortType);
  }

}
