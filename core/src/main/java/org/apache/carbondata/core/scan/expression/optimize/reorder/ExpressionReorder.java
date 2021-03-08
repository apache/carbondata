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

package org.apache.carbondata.core.scan.expression.optimize.reorder;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.optimize.OptimizeRule;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * reorder Expression by storage order
 */
public class ExpressionReorder extends OptimizeRule {

  @Override
  public Expression optimize(CarbonTable table, Expression expression) {
    if (!CarbonProperties.isFilterReorderingEnabled()) {
      return expression;
    }
    MultiExpression multiExpression = MultiExpression.build(expression);
    // unsupported expression
    if (multiExpression == null) {
      return expression;
    }
    // remove redundancy filter
    multiExpression.removeRedundant();
    // combine multiple filters to single filter
    multiExpression.combine();
    // reorder Expression by storage ordinal of columns
    multiExpression.reorder(table);
    return multiExpression.toExpression();
  }
}
