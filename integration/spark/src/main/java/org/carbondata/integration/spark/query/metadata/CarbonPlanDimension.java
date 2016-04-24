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

/**
 * Dimension class
 */
public class CarbonPlanDimension implements CarbonPlanColumn, Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 8447604537458509117L;

  /**
   * Dimension unique name
   */
  private String dimensionUniqueName;

  /**
   * queryOrder
   */
  private int queryOrder;

  private boolean isDistinctCountQuery;

  /**
   * sort order type. default is no order.
   */
  private SortOrderType sortOrderType = SortOrderType.NONE;

  /**
   * Constructor to create dimension.User needs to pass dimension unique name.
   *
   * @param dimensionUniqueName
   */
  public CarbonPlanDimension(String dimensionUniqueName) {
    this.dimensionUniqueName = dimensionUniqueName;
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

  /**
   * @return the dimensionUniqueName
   */
  public String getDimensionUniqueName() {
    return dimensionUniqueName;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dimensionUniqueName == null) ? 0 : dimensionUniqueName.hashCode());
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
    CarbonDimension other = (CarbonDimension) obj;
    if (dimensionUniqueName == null) {
      if (other.dimensionUniqueName != null) {
        return false;
      }
    } else if (!dimensionUniqueName.equals(other.dimensionUniqueName)) {
      return false;
    }
    return true;
  }

  public int getQueryOrder() {
    return queryOrder;
  }

  public void setQueryOrder(int queryOrder) {
    this.queryOrder = queryOrder;
  }

  public boolean isDistinctCountQuery() {
    return isDistinctCountQuery;
  }

  public void setDistinctCountQuery(boolean isDistinctCountQuery) {
    this.isDistinctCountQuery = isDistinctCountQuery;
  }
}
