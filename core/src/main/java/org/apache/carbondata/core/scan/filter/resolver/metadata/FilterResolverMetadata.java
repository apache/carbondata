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
package org.apache.carbondata.core.scan.filter.resolver.metadata;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;

public class FilterResolverMetadata {
  private AbsoluteTableIdentifier tableIdentifier;
  private Expression expression;
  private ColumnExpression columnExpression;

  private boolean isIncludeFilter;

  public AbsoluteTableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public void setTableIdentifier(AbsoluteTableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
  }

  public Expression getExpression() {
    return expression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  public ColumnExpression getColumnExpression() {
    return columnExpression;
  }

  public void setColumnExpression(ColumnExpression columnExpression) {
    this.columnExpression = columnExpression;
  }

  public boolean isIncludeFilter() {
    return isIncludeFilter;
  }

  public void setIncludeFilter(boolean isIncludeFilter) {
    this.isIncludeFilter = isIncludeFilter;
  }
}
