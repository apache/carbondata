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
package org.carbondata.query.carbon.result.iterator;

import java.util.List;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchRawResult;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.preparator.QueryResultPreparator;
import org.carbondata.query.carbon.result.preparator.impl.RawQueryResultPreparatorImpl;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public class DetailRawQueryResultIterator
    extends AbstractDetailQueryResultIterator<BatchRawResult> {

  private QueryResultPreparator<BatchRawResult> queryResultPreparator;

  public DetailRawQueryResultIterator(List<BlockExecutionInfo> infos,
      QueryExecutorProperties executerProperties, QueryModel queryModel,
      InternalQueryExecutor queryExecutor) {
    super(infos, executerProperties, queryModel, queryExecutor);
    this.queryResultPreparator = new RawQueryResultPreparatorImpl(executerProperties, queryModel);
  }

  @Override public BatchRawResult next() {
    updateSliceIndexToBeExecuted();
    CarbonIterator<Result> result = null;
    try {
      result = executor.executeQuery(blockExecutionInfos, blockIndexToBeExecuted);
    } catch (QueryExecutionException ex) {
      throw new RuntimeException(ex.getCause());
    }
    for (int i = 0; i < blockIndexToBeExecuted.length; i++) {
      if (blockIndexToBeExecuted[i] != -1) {
        blockExecutionInfos.get(blockIndexToBeExecuted[i]).setFirstDataBlock(
            blockExecutionInfos.get(blockIndexToBeExecuted[i]).getFirstDataBlock()
                .getNextDataRefNode());
      }
    }
    if (null != result) {
      Result next = result.next();
      return queryResultPreparator.prepareQueryResult(next);
    } else {
      return queryResultPreparator.prepareQueryResult(null);
    }
  }
}
