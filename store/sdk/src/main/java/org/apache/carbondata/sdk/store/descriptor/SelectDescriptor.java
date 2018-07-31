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

package org.apache.carbondata.sdk.store.descriptor;

import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.scan.expression.Expression;

@InterfaceAudience.User
@InterfaceStability.Evolving
public class SelectDescriptor {

  private TableIdentifier table;
  private String[] projection;
  private Expression filter;
  private long limit;

  private SelectDescriptor() {
  }

  public SelectDescriptor(TableIdentifier table, String[] projection,
      Expression filter, long limit) {
    Objects.requireNonNull(table);
    Objects.requireNonNull(projection);
    this.table = table;
    this.projection = projection;
    this.filter = filter;
    this.limit = limit;
  }

  public TableIdentifier getTableIdentifier() {
    return table;
  }

  public void setTableIdentifier(TableIdentifier table) {
    this.table = table;
  }

  public String[] getProjection() {
    return projection;
  }

  public void setProjection(String[] projection) {
    this.projection = projection;
  }

  public Expression getFilter() {
    return filter;
  }

  public void setFilter(Expression filter) {
    this.filter = filter;
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }

  public static class Builder {
    private SelectDescriptor select;

    private Builder() {
      select = new SelectDescriptor();
    }

    public Builder table(TableIdentifier tableIdentifier) {
      select.setTableIdentifier(tableIdentifier);
      return this;
    }

    public Builder select(String... columnNames) {
      select.setProjection(columnNames);
      return this;
    }

    public Builder filter(Expression filter) {
      select.setFilter(filter);
      return this;
    }

    public Builder limit(long limit) {
      select.setLimit(limit);
      return this;
    }

    public SelectDescriptor create() {
      return select;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
