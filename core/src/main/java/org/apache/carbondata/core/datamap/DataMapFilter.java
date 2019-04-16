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

package org.apache.carbondata.core.datamap;

import java.io.Serializable;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

/**
 * the filter of DataMap
 */
public class DataMapFilter implements Serializable {

  private CarbonTable table;

  private Expression expression;

  private FilterResolverIntf resolver;

  public DataMapFilter(CarbonTable table, Expression expression) {
    this.table = table;
    this.expression = expression;
    resolve();
  }

  public DataMapFilter(FilterResolverIntf resolver) {
    this.resolver = resolver;
  }

  private void resolve() {
    if (expression != null) {
      table.processFilterExpression(expression, null, null);
      resolver = CarbonTable.resolveFilter(expression, table.getAbsoluteTableIdentifier());
    }
  }

  public Expression getExpression() {
    return expression;
  }

  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  public FilterResolverIntf getResolver() {
    return resolver;
  }

  public void setResolver(FilterResolverIntf resolver) {
    this.resolver = resolver;
  }

  public boolean isEmpty() {
    return resolver == null;
  }

  public boolean isResolvedOnSegment(SegmentProperties segmentProperties) {
    if (expression == null || table == null) {
      return true;
    }
    if (!table.isTransactionalTable()) {
      return false;
    }
    if (table.hasColumnDrift() && RestructureUtil
        .hasColumnDriftOnSegment(table, segmentProperties)) {
      return false;
    }
    return true;
  }
}
