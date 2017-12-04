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

public class ColumnTableRelation {
  private String parentColumnName;
  private String parentColumnId;
  private String parentTableName;
  private String parentDatabaseName;
  private String parentTableId;

  public ColumnTableRelation(String parentColumnName, String parentColumnId, String parentTableName,
      String parentDatabaseName, String parentTableId) {
    this.parentColumnName = parentColumnName;
    this.parentColumnId = parentColumnId;
    this.parentTableName = parentTableName;
    this.parentDatabaseName = parentDatabaseName;
    this.parentTableId = parentTableId;
  }

  public String getParentColumnName() {
    return parentColumnName;
  }

  public String getParentColumnId() {
    return parentColumnId;
  }

  public String getParentTableName() {
    return parentTableName;
  }

  public String getParentDatabaseName() {
    return parentDatabaseName;
  }

  public String getParentTableId() {
    return parentTableId;
  }
}