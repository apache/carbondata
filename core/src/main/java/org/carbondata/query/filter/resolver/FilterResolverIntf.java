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
package org.carbondata.query.filter.resolver;

import java.io.Serializable;
import java.util.SortedMap;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbonfilterinterface.FilterExecuterType;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

public interface FilterResolverIntf extends Serializable {

  /**
   * This API will resolve the filter expression and generates the
   * dictionaries for executing/evaluating the filter expressions in the
   * executer layer.
   *
   * @throws QueryExecutionException
   * @throws FilterUnsupportedException
   */
  void resolve(AbsoluteTableIdentifier absoluteTableIdentifier) throws FilterUnsupportedException;

  /**
   * This API will provide the left column filter expression
   * inorder to resolve the left expression filter.
   *
   * @return FilterResolverIntf
   */
  FilterResolverIntf getLeft();

  /**
   * API will provide the right column filter expression inorder to resolve
   * the right expression filter.
   *
   * @return FilterResolverIntf
   */
  FilterResolverIntf getRight();

  /**
   * API will return the resolved filter instance, this instance will provide
   * the resolved surrogates based on the applied filter
   *
   * @return DimColumnResolvedFilterInfo object
   */
  DimColumnResolvedFilterInfo getDimColResolvedFilterInfo();

  /**
   * API will get the start key based on the filter applied based on the key generator
   *
   * @param segmentProperties
   * @param startKey
   * @param setOfStartKeyByteArray
   */
  void getStartKey(SegmentProperties segmentProperties, long[] startKey,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray);

  /**
   * API will read the end key based on the max surrogate of
   * particular dimension column
   *
   * @param setOfEndKeyByteArray
   * @param endKeys
   * @return
   * @throws QueryExecutionException
   */
  void getEndKey(SegmentProperties segmentProperties, AbsoluteTableIdentifier tableIdentifier,
      long[] endKeys, SortedMap<Integer, byte[]> setOfEndKeyByteArray)
      throws QueryExecutionException;

  /**
   * API will return the filter executer type which will be used to evaluate
   * the resolved filter while query execution
   *
   * @return FilterExecuterType.
   */
  FilterExecuterType getFilterExecuterType();

  Expression getFilterExpression();

}
