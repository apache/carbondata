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

package org.apache.carbondata.core.scan.filter.resolver.resolverinfo;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.intf.FilterExecutorType;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;

/* The expression with If FALSE will be resolved setting empty bitset. */
public class FalseConditionalResolverImpl extends ConditionalFilterResolverImpl {

  private static final long serialVersionUID = 4599541011924324041L;

  public FalseConditionalResolverImpl(Expression exp, boolean isExpressionResolve,
      boolean isIncludeFilter) {
    super(exp, isExpressionResolve, isIncludeFilter, false);
  }

  @Override
  public void resolve() {
  }

  /**
   * This method will provide the executor type to the callee inorder to identify
   * the executer type for the filter resolution, False Expression will not execute anything.
   * it will return empty bitset
   */
  @Override
  public FilterExecutorType getFilterExecutorType() {
    return FilterExecutorType.FALSE;
  }

  /**
   * Method will the read filter expression corresponding to the resolver.
   * This method is required in row level executor inorder to evaluate the filter
   * expression against spark, as mentioned above row level is a special type
   * filter resolver.
   *
   * @return Expression
   */
  public Expression getFilterExpresion() {
    return exp;
  }

}

