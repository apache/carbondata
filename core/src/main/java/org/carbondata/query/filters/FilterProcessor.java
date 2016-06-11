package org.carbondata.query.filters;

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

import java.util.List;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.FilterResolverIntf;

public interface FilterProcessor {

  /**
   * API will provide the resolved form of filters based on the filter
   * expression tree which is been passed.
   *
   * @param expressionTree  , filter expression tree
   * @param tableIdentifier ,contains carbon store informations.
   * @return
   * @throws QueryExecutionException
   * @throws FilterUnsupportedException
   */
  FilterResolverIntf getFilterResolver(Expression expressionTree,
      AbsoluteTableIdentifier tableIdentifier) throws FilterUnsupportedException;

  /**
   * This API is exposed inorder to get the required block reference node
   * based on the filter.The block list will be send to the executer tasks inorder
   * to apply filters.
   *
   * @param filterResolver DataBlock list with resolved filters
   * @return list of DataRefNode.
   * @throws QueryExecutionException
   */
  List<DataRefNode> getFilterredBlocks(DataRefNode dataRefNode, FilterResolverIntf filterResolver,
      AbstractIndex segmentIndexBuilder, AbsoluteTableIdentifier tableIdentifier)
      throws QueryExecutionException;

}
