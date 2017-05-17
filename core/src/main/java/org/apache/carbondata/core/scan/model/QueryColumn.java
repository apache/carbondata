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

/**
 * query column  which will have information about column
 */
public class QueryColumn implements Serializable {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -4222306600480181084L;

  /**
   * name of the column
   */
  protected String columnName;

  /**
   * query order in which result of the query will be send
   */
  private int queryOrder;

  public QueryColumn(String columnName) {
    this.columnName = columnName;
  }

  /**
   * @return the columnName
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * @return the queryOrder
   */
  public int getQueryOrder() {
    return queryOrder;
  }

  /**
   * @param queryOrder the queryOrder to set
   */
  public void setQueryOrder(int queryOrder) {
    this.queryOrder = queryOrder;
  }

  /**
   * sort order in which column output will be sorted default it will be none
   */
  private SortOrderType sortOrder = SortOrderType.NONE;

  /**
   * @return the sortOrder
   */
  public SortOrderType getSortOrder() {
    return sortOrder;
  }

  /**
   * @param sortOrder the sortOrder to set
   */
  public void setSortOrder(SortOrderType sortOrder) {
    this.sortOrder = sortOrder;
  }

}