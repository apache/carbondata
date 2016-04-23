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
package org.carbondata.query.carbon.executor.impl;

import java.util.List;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.executor.internal.impl.InternalDetailWithOrderQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.RowResult;
import org.carbondata.query.carbon.result.impl.ListBasedResult;
import org.carbondata.query.carbon.result.iterator.ChunkBasedResultIterator;
import org.carbondata.query.carbon.result.iterator.ChunkRowIterator;
import org.carbondata.query.carbon.result.iterator.MemoryBasedResultIterator;

/**
 * Below method will be used to execute the detail query with order by
 */
public class DetailWithOrderByQueryExecutor extends AbstractQueryExecutor {

  @Override public CarbonIterator<RowResult> execute(QueryModel queryModel)
      throws QueryExecutionException {
    if (queryProperties.dataBlocks.size() == 0) {
      // if there are not block present then set empty row
      // and return
      return new ChunkRowIterator(
          new ChunkBasedResultIterator(new MemoryBasedResultIterator(new ListBasedResult()),
              queryProperties, queryModel));
    }
    // get the execution info
    List<BlockExecutionInfo> blockExecutionInfoList = getBlockExecutionInfos(queryModel);
    // in case of sorting we need to add sort information only for last block as
    // all the previous block data will be updated based last block and after
    // processing of all the block sorting will be applied
    blockExecutionInfoList.get(blockExecutionInfoList.size() - 1)
        .setSortInfo(getSortInfos(queryModel.getQueryDimension(), queryModel));
    InternalQueryExecutor internalQueryExecutor = new InternalDetailWithOrderQueryExecutor();
    return new ChunkRowIterator(new ChunkBasedResultIterator(
        internalQueryExecutor.executeQuery(blockExecutionInfoList, null), queryProperties,
        queryModel));
  }

}
