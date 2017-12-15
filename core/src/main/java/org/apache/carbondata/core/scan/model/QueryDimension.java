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

package org.apache.carbondata.core.scan.model;


import java.io.Serializable;

import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;

/**
 * query plan dimension which will holds the information about the query plan dimension
 * this is done to avoid heavy object serialization
 */
public class QueryDimension extends QueryColumn implements Serializable {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -8492704093776645651L;
  /**
   * actual dimension column
   */
  private transient CarbonDimension dimension;

  private List<String> projectionChildColumnNames ;

  public QueryDimension(String columnName) {
    super(columnName);
  }

  /**
   * @return the dimension
   */
  public CarbonDimension getDimension() {
    return dimension;
  }

  /**
   * @param dimension the dimension to set
   */
  public void setDimension(CarbonDimension dimension) {
    this.dimension = dimension;
  }

  public void setProjectionChildColumnNames(List<String> childDimensionNames) {
    this.projectionChildColumnNames = childDimensionNames;
  }

  public List<String> getProjectionChildColumnNames() {
    return projectionChildColumnNames;

  }
}