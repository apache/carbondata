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

package org.apache.carbondata.store.rpc.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;

import org.apache.hadoop.io.Writable;

@InterfaceAudience.Internal
public class QueryRequest implements Serializable, Writable {
  private int requestId;
  private CarbonMultiBlockSplit split;
  private TableInfo tableInfo;
  private String[] projectColumns;
  private Expression filterExpression;
  private long limit;

  public QueryRequest() {
  }

  public QueryRequest(int requestId, CarbonMultiBlockSplit split,
      TableInfo tableInfo, String[] projectColumns, Expression filterExpression, long limit) {
    this.requestId = requestId;
    this.split = split;
    this.tableInfo = tableInfo;
    this.projectColumns = projectColumns;
    this.filterExpression = filterExpression;
    this.limit = limit;
  }

  public int getRequestId() {
    return requestId;
  }

  public CarbonMultiBlockSplit getSplit() {
    return split;
  }

  public TableInfo getTableInfo() {
    return tableInfo;
  }

  public String[] getProjectColumns() {
    return projectColumns;
  }

  public Expression getFilterExpression() {
    return filterExpression;
  }

  public long getLimit() {
    return limit;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(requestId);
    split.write(out);
    tableInfo.write(out);
    out.writeInt(projectColumns.length);
    for (String projectColumn : projectColumns) {
      out.writeUTF(projectColumn);
    }
    String filter = ObjectSerializationUtil.convertObjectToString(filterExpression);
    out.writeUTF(filter);
    out.writeLong(limit);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    requestId = in.readInt();
    split = new CarbonMultiBlockSplit();
    split.readFields(in);
    tableInfo = new TableInfo();
    tableInfo.readFields(in);
    projectColumns = new String[in.readInt()];
    for (int i = 0; i < projectColumns.length; i++) {
      projectColumns[i] = in.readUTF();
    }
    String filter = in.readUTF();
    filterExpression = (Expression) ObjectSerializationUtil.convertStringToObject(filter);
    limit = in.readLong();
  }
}