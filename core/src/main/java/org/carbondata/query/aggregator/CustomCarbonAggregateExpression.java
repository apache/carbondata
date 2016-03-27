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

package org.carbondata.query.aggregator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.metadata.CarbonMetadata.Dimension;

public class CustomCarbonAggregateExpression implements Serializable {

    private static final long serialVersionUID = 1L;

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
    private List<Dimension> referredColumns;

    /**
     * Actual expression in query to use in the comparison with other Aggregate
     * expressions.
     */
    private String expression;

    /**
     * Position in the query
     */
    private int queryOrder;

    public CustomCarbonAggregateExpression() {
        referredColumns = new ArrayList<Dimension>(10);
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public List<Dimension> getReferredColumns() {
        return referredColumns;
    }

    public void setReferredColumns(List<Dimension> referredColumns) {
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((expression == null) ? 0 : expression.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof CustomCarbonAggregateExpression)) {
            return false;
        }

        CustomCarbonAggregateExpression other = ((CustomCarbonAggregateExpression) obj);

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
