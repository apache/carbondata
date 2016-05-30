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

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.executor.internal.impl.InternalCountStartQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.RowResult;
import org.carbondata.query.carbon.result.iterator.ChunkBasedResultIterator;
import org.carbondata.query.carbon.result.iterator.ChunkRowIterator;

/**
 * Below class will be used to execute the count start query
 */
public class CountStarQueryExecutor extends AbstractQueryExecutor<RowResult> {

  @Override public CarbonIterator<RowResult> execute(QueryModel queryModel)
      throws QueryExecutionException {
    initQuery(queryModel);
    InternalQueryExecutor queryExecutor =
        new InternalCountStartQueryExecutor(queryProperties.dataBlocks);
    return new ChunkRowIterator(
        new ChunkBasedResultIterator(queryExecutor.executeQuery(null, null), queryProperties,
            queryModel));
  }

}
