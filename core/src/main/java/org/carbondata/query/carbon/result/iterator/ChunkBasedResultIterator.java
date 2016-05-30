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

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.preparator.QueryResultPreparator;
import org.carbondata.query.carbon.result.preparator.impl.QueryResultPreparatorImpl;

/**
 * Iterator over chunk result
 */
public class ChunkBasedResultIterator extends CarbonIterator<BatchResult> {

  /**
   * query result prepartor which will be used to create a query result
   */
  private QueryResultPreparator<BatchResult> queryResultPreparator;

  /**
   * iterator over result
   */
  private CarbonIterator<Result> queryResultIterator;

  public ChunkBasedResultIterator(CarbonIterator<Result> queryResultIterator,
      QueryExecutorProperties executerProperties, QueryModel queryModel) {
    this.queryResultIterator = queryResultIterator;
    this.queryResultPreparator = new QueryResultPreparatorImpl(executerProperties, queryModel);

  }

  /**
   * Returns {@code true} if the iteration has more elements. (In other words,
   * returns {@code true}
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    return queryResultIterator.hasNext();
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override public BatchResult next() {
    return queryResultPreparator.prepareQueryResult(queryResultIterator.next());
  }

}
