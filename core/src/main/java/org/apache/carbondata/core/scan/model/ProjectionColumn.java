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

/**
 * Contains information for a column for projection
 */
public class ProjectionColumn {

  /**
   * name of the column
   */
  protected String columnName;

  /**
   * query order in which result of the query will be send
   */
  private int projectionOrdinal;

  ProjectionColumn(String columnName) {
    this.columnName = columnName;
  }

  /**
   * @return the columnName
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * @return the projectionOrdinal
   */
  public int getOrdinal() {
    return projectionOrdinal;
  }

  /**
   * @param projectionOrdinal the projectionOrdinal to set
   */
  public void setOrdinal(int projectionOrdinal) {
    this.projectionOrdinal = projectionOrdinal;
  }

}