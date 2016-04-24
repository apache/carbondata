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

/**
 *
 */
package org.carbondata.integration.spark.query.metadata;

import java.io.Serializable;

public class CarbonPlanMeasure implements CarbonPlanColumn, Serializable {

  private static final long serialVersionUID = -2944975816723740737L;

  /**
   * Measure.
   * Ex : select employee_name,department_name,sum(salary) from employee, then sum(salary)
   * would be measure. If user does not mention any aggregate type then by default count()
   * would be taken as aggregator.
   */
  private String measure;

  private AggregatorType aggregatorType;

  private boolean isQueryDistinctCount;

  /**
   * queryOrder
   */
  private int queryOrder;

  /**
   * sort order type. default is no order.
   * User can mention sort order to only one measure . if user selects sort order to multiple
   * measures then last measures sort order only be taken.
   */
  private SortOrderType sortOrderType = SortOrderType.NONE;

  /**
   * private boolean isQueryDistinctCount;
   * Constructor to create measure.
   *
   * @param measure
   */
  public CarbonPlanMeasure(String measure) {
    String aggName = null;
    String msrName = measure;
    //we assume the format is like sum(colName). need to handle in proper way.
    int indexOf = measure.indexOf("(");
    if (indexOf > 0) {
      aggName = measure.substring(0, indexOf).toLowerCase();
      msrName = measure.substring(indexOf + 1, measure.length() - 1);
    }
    this.measure = msrName;
    if (aggName != null) {
      this.aggregatorType = AggregatorType.INSTANCE.getAggType(aggName);
    }
  }

  /**
   * @return the measure
   */
  public String getMeasure() {
    return measure;
  }

  /**
   * @return the sortOrderType
   */
  public SortOrderType getSortOrderType() {
    return sortOrderType;
  }

  /**
   * @param sortOrderType the sortOrderType to set
   */
  public void setSortOrderType(SortOrderType sortOrderType) {
    this.sortOrderType = sortOrderType;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((measure == null) ? 0 : measure.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CarbonPlanMeasure other = (CarbonPlanMeasure) obj;
    if (measure == null) {
      if (other.measure != null) {
        return false;
      }
    } else if (!measure.equals(other.measure)) {
      return false;
    }
    return true;
  }

  /**
   * @return the aggregatorType
   */
  public AggregatorType getAggregatorType() {
    return aggregatorType;
  }

  /**
   * @param aggregatorType the aggregatorType to set
   */
  public void setAggregatorType(AggregatorType aggregatorType) {
    this.aggregatorType = aggregatorType;
  }

  public boolean isQueryDistinctCount() {
    return isQueryDistinctCount;
  }

  public void setQueryDistinctCount(boolean isQueryDistinctCount) {
    this.isQueryDistinctCount = isQueryDistinctCount;
  }

  public int getQueryOrder() {
    return queryOrder;
  }

  public void setQueryOrder(int queryOrder) {
    this.queryOrder = queryOrder;
  }

  public enum AggregatorType {
    SUM("SUM"), COUNT("COUNT"), AVG("AVG"), MAX("MAX"), MIN("MIN"), DISTINCT_COUNT(
        "DISTINCT-COUNT"), SUM_DISTINCT("SUM-DISTINCT"), INSTANCE("INSTANCE"), CUSTOM("CUSTOM");

    private String name;

    AggregatorType(String name) {
      this.name = name;
    }

    public String getValue() {
      return name;
    }

    public AggregatorType getAggType(String aggType) {
      if ("sumcarbon".equalsIgnoreCase(aggType) || "sum".equalsIgnoreCase(aggType)) {
        return SUM;
      } else if ("countcarbon".equalsIgnoreCase(aggType) || "count".equalsIgnoreCase(aggType)) {
        return COUNT;
      } else if ("avgcarbon".equalsIgnoreCase(aggType) || "avg".equalsIgnoreCase(aggType)) {
        return AVG;
      } else if (aggType.equalsIgnoreCase("CountDistinctCarbon") || aggType
          .equalsIgnoreCase("distinct_count")) {
        return DISTINCT_COUNT;
      } else if (("MinCarbon").equalsIgnoreCase(aggType) || ("min").equalsIgnoreCase(aggType)) {
        return MIN;
      } else if (("MaxCarbon").equalsIgnoreCase(aggType) || ("max").equalsIgnoreCase(aggType)) {
        return MAX;
      } else if ("SumDistinctCarbon".equalsIgnoreCase(aggType) || "sum-distinct"
          .equalsIgnoreCase(aggType)) {
        return SUM_DISTINCT;
      } else if ("CUSTOM".equalsIgnoreCase(aggType)) {
        return CUSTOM;
      }
      return null;
    }
  }

}
