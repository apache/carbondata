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

package org.apache.carbondata.store.impl.service.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.sdk.store.descriptor.TableIdentifier;

import org.apache.hadoop.io.Writable;

public class PruneRequest implements Serializable, Writable {

  private TableIdentifier table;
  private Expression filterExpression;

  public PruneRequest() {
  }

  public PruneRequest(TableIdentifier table, Expression filterExpression) {
    this.table = table;
    this.filterExpression = filterExpression;
  }

  public TableIdentifier getTable() {
    return table;
  }

  public Expression getFilterExpression() {
    return filterExpression;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    table.write(out);
    out.writeUTF(ObjectSerializationUtil.convertObjectToString(filterExpression));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    table = new TableIdentifier();
    table.readFields(in);
    filterExpression = (Expression) (ObjectSerializationUtil.convertStringToObject(in.readUTF()));
  }
}
