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

import org.apache.carbondata.core.scan.expression.BinaryExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.FilterExecutorType;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;

public class LogicalFilterResolverImpl implements FilterResolverIntf {
  /**
   *
   */
  private static final long serialVersionUID = 5734382980564402914L;

  protected FilterResolverIntf leftEvaluator;

  protected FilterResolverIntf rightEvaluator;

  protected ExpressionType filterExpressionType;

  private BinaryExpression filterExpression;

  public LogicalFilterResolverImpl(FilterResolverIntf leftEvaluator,
      FilterResolverIntf rightEvaluator, BinaryExpression currentExpression) {
    this.leftEvaluator = leftEvaluator;
    this.rightEvaluator = rightEvaluator;
    this.filterExpressionType = currentExpression.getFilterExpressionType();
    this.filterExpression = currentExpression;
  }

  /**
   * Logical filter resolver will return the left and right filter expresion
   * node for filter evaluation, so in this instance no implementation is required.
   *
   */
  @Override
  public void resolve() {

  }

  /**
   * Since its a binary condition expresion the getLeft method will get the left
   * node of filter expression
   *
   * @return FilterResolverIntf.
   */
  public FilterResolverIntf getLeft() {
    return leftEvaluator;
  }

  /**
   * Since its a binary condition expresion the getRight method will get the left
   * node of filter expression
   *
   * @return FilterResolverIntf.
   */
  public FilterResolverIntf getRight() {
    return rightEvaluator;
  }

  @Override
  public DimColumnResolvedFilterInfo getDimColResolvedFilterInfo() {
    return null;
  }

  @Override
  public MeasureColumnResolvedFilterInfo getMsrColResolvedFilterInfo() {
    return null;
  }

  @Override
  public FilterExecutorType getFilterExecutorType() {
    switch (filterExpressionType) {
      case OR:
        return FilterExecutorType.OR;
      case AND:
        return FilterExecutorType.AND;

      default:
        return null;
    }
  }

  @Override
  public Expression getFilterExpression() {
    return filterExpression;
  }
}
