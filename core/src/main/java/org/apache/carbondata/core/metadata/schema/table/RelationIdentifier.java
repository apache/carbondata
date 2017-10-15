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
package org.apache.carbondata.core.metadata.schema.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * class to maintain the relation between parent and child
 */
public class RelationIdentifier implements Serializable, Writable {

  private String databaseName;

  private String tableName;

  private String tableId;

  public RelationIdentifier(String databaseName, String tableName, String tableId) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.tableId = tableId;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableId() {
    return tableId;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeUTF(databaseName);
    out.writeUTF(tableName);
    out.writeUTF(tableId);
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.databaseName = in.readUTF();
    this.tableName = in.readUTF();
    this.tableId = in.readUTF();
  }
}
