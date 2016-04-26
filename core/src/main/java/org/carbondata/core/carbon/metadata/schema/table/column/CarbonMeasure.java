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

package org.carbondata.core.carbon.metadata.schema.table.column;

/**
 * class represent column(measure) in table
 */
public class CarbonMeasure extends CarbonColumn {

  /**
   * serialization version
   */
  private static final long serialVersionUID = 354341488059013977L;

  /**
   * aggregator chosen for measure
   */
  private String aggregateFunction;

  /**
   * Used when this column contains decimal data.
   */
  private int scale;

  /**
   * precision in decimal data
   */
  private int precision;

  public CarbonMeasure(ColumnSchema columnSchema, int ordinal) {
    super(columnSchema, ordinal);
    this.scale = columnSchema.getScale();
    this.precision = columnSchema.getPrecision();
  }

  /**
   * @return the scale
   */
  public int getScale() {
    return scale;
  }

  /**
   * @return the precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @return the aggregator
   */
  public String getAggregateFunction() {
    return aggregateFunction;
  }

  /**
   * @param aggregateFunction the aggregateFunction to set
   */
  public void setAggregateFunction(String aggregateFunction) {
    this.aggregateFunction = aggregateFunction;
  }

  /**
   * to check whether to dimension are equal or not
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof CarbonMeasure)) {
      return false;
    }
    CarbonMeasure other = (CarbonMeasure) obj;
    if (columnSchema == null) {
      if (other.columnSchema != null) {
        return false;
      }
    } else if (!columnSchema.equals(other.columnSchema)) {
      return false;
    }
    return true;
  }
}
