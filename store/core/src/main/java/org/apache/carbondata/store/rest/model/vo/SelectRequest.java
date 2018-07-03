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

package org.apache.carbondata.store.rest.model.vo;

import org.apache.carbondata.store.rest.model.dto.Select;

public class SelectRequest {

  private String databaseName;
  private String tableName;
  private String[] select;
  private String filter;
  private int limit;

  public SelectRequest() {

  }

  public SelectRequest(String databaseName, String tableName, String[] select, String filter,
      int limit) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.select = select;
    this.filter = filter;
    this.limit = limit;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String[] getSelect() {
    return select;
  }

  public void setSelect(String[] select) {
    this.select = select;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public Select convertToDto() {
    return new Select(
        databaseName, tableName, select, filter, limit);
  }

  public static class Builder {

    private SelectRequest select;

    private Builder() {
      select = new SelectRequest();
    }

    public Builder databaseName(String databaseName) {
      select.setDatabaseName(databaseName);
      return this;
    }

    public Builder tableName(String tableName) {
      select.setTableName(tableName);
      return this;
    }

    public Builder select(String... columnNames) {
      select.setSelect(columnNames);
      return this;
    }

    public Builder filter(String fitler) {
      select.setFilter(fitler);
      return this;
    }

    public Builder limit(int limit) {
      select.setLimit(limit);
      return this;
    }

    public SelectRequest create() {
      return select;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

}
