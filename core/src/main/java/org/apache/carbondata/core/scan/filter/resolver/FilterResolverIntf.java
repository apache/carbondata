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

package org.apache.carbondata.core.scan.filter.resolver;

import java.io.Serializable;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.FilterExecutorType;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;

public interface FilterResolverIntf extends Serializable {

  /**
   * This API will resolve the filter expression and generates the
   * dictionaries for executing/evaluating the filter expressions in the
   * executor layer.
   *
   * @throws FilterUnsupportedException
   */
  void resolve() throws FilterUnsupportedException;

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
   * API will return the resolved filter instance, this instance will provide
   * the resolved surrogates based on the applied filter
   *
   * @return MeasureColumnResolvedFilterInfo object
   */
  MeasureColumnResolvedFilterInfo getMsrColResolvedFilterInfo();

  /**
   * API will return the filter executor type which will be used to evaluate
   * the resolved filter while query execution
   *
   * @return FilterExecutorType.
   */
  FilterExecutorType getFilterExecutorType();

  Expression getFilterExpression();

}
