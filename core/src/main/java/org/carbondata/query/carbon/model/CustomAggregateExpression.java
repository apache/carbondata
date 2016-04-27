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
package org.carbondata.query.carbon.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.CustomMeasureAggregator;

/**
 * Holds the information about expression present in the query
 */
public class CustomAggregateExpression implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 4831882661337567735L;

  /**
   * Identified and delegated from Spark Layer for UDAF in Carbon
   */
  private CustomMeasureAggregator aggregator;

  /**
   * Aggregate column name may not be a measure or dimension. Can be a column
   * name given in query
   */
  private String name;

  /**
   * Columns used in the expression where column can be a dimension or a
   * measure.
   */
  private List<CarbonColumn> referredColumns;

  /**
   * Actual expression in query to use in the comparison with other Aggregate
   * expressions.
   */
  private String expression;

  /**
   * Position in the query
   */
  private int queryOrder;

  public CustomAggregateExpression() {
    referredColumns = new ArrayList<CarbonColumn>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public List<CarbonColumn> getReferredColumns() {
    return referredColumns;
  }

  public void setReferredColumns(List<CarbonColumn> referredColumns) {
    this.referredColumns = referredColumns;
  }

  public int getQueryOrder() {
    return queryOrder;
  }

  public void setQueryOrder(int queryOrder) {
    this.queryOrder = queryOrder;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return MeasureAggregator
   */
  public CustomMeasureAggregator getAggregator() {
    return aggregator;
  }

  /**
   * @param aggregator
   */
  public void setAggregator(CustomMeasureAggregator aggregator) {
    this.aggregator = aggregator;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((expression == null) ? 0 : expression.hashCode());
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof CustomAggregateExpression)) {
      return false;
    }

    CustomAggregateExpression other = ((CustomAggregateExpression) obj);

    if ((expression != null) && (expression.equals(other.expression))) {
      return true;
    }

    if (expression != null) {
      return expression.equalsIgnoreCase(other.expression);
    }

    if (other.expression != null) {
      return other.expression.equalsIgnoreCase(expression);
    }

    return true;
  }
}
