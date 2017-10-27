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

package org.apache.carbondata.core.metadata.schema.table.column;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.Writable;

/**
 * to maintain the relationship of child table column to parent table columns
 */
public class ColumnRelation implements Serializable, Writable {

  /**
   * column id
   */
  private String columnId;

  /**
   * column table relation
   */
  private List<ParentColumnTableRelation> columnTableRelationList;

  public ColumnRelation(String columnId, List<ParentColumnTableRelation> columnTableRelationList) {
    this.columnId = columnId;
    this.columnTableRelationList = columnTableRelationList;
  }

  public String getColumnId() {
    return columnId;
  }

  public List<ParentColumnTableRelation> getColumnTableRelationList() {
    return columnTableRelationList;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeUTF(columnId);
    out.writeShort(columnTableRelationList.size());
    for (int i = 0; i < columnTableRelationList.size(); i++) {
      columnTableRelationList.get(i).write(out);
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.columnId = in.readUTF();
    short columnTableRelationSize = in.readShort();
    columnTableRelationList = new ArrayList<>();
    for (int i = 0; i < columnTableRelationSize; i++) {
      ParentColumnTableRelation columnTableRelation =
          new ParentColumnTableRelation(null, null, null);
      columnTableRelation.readFields(in);
    }
  }
}
