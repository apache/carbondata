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
package org.apache.carbondata.scan.executor.impl;

import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.result.BatchResult;
import org.apache.carbondata.scan.result.iterator.DetailQueryResultIterator;
import org.apache.carbondata.scan.result.preparator.impl.RawQueryResultPreparatorImpl;

/**
 * Executor for raw records, it does not parse to actual data
 */
public class DetailRawRecordQueryExecutor extends AbstractQueryExecutor<BatchResult> {

  @Override
  public CarbonIterator<BatchResult> execute(QueryModel queryModel)
      throws QueryExecutionException {
    List<BlockExecutionInfo> blockExecutionInfoList = getBlockExecutionInfos(queryModel);
    return new DetailQueryResultIterator(blockExecutionInfoList, queryModel,
        new RawQueryResultPreparatorImpl(queryProperties, queryModel));
  }
}
