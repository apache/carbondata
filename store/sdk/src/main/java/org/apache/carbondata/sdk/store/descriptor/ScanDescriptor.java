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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.ObjectSerializationUtil;

import org.apache.hadoop.io.Writable;

@InterfaceAudience.User
@InterfaceStability.Evolving
public class ScanDescriptor implements Writable {

  private TableIdentifier table;
  private String[] projection;
  private Expression filter;
  private long limit = Long.MAX_VALUE;

  private ScanDescriptor() {
  }

  public ScanDescriptor(TableIdentifier table, String[] projection,
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

  @Override
  public void write(DataOutput out) throws IOException {
    table.write(out);
    out.writeInt(projection.length);
    for (String s : projection) {
      out.writeUTF(s);
    }
    out.writeBoolean(filter != null);
    if (filter != null) {
      out.writeUTF(ObjectSerializationUtil.convertObjectToString(filter));
    }
    out.writeLong(limit);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    table = new TableIdentifier();
    table.readFields(in);
    int size = in.readInt();
    projection = new String[size];
    for (int i = 0; i < size; i++) {
      projection[i] = in.readUTF();
    }
    if (in.readBoolean()) {
      filter = (Expression) ObjectSerializationUtil.convertStringToObject(in.readUTF());
    }
    limit = in.readLong();
  }

  public static class Builder {
    private ScanDescriptor select;

    private Builder() {
      select = new ScanDescriptor();
    }

    public Builder table(TableIdentifier tableIdentifier) {
      select.setTableIdentifier(tableIdentifier);
      return this;
    }

    public Builder select(String[] columnNames) {
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

    public ScanDescriptor create() {
      return select;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
