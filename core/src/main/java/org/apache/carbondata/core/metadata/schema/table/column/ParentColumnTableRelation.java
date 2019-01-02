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

import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.metadata.schema.table.Writable;

/**
 * To maintain the relation of child column to parent table column
 */
public class ParentColumnTableRelation implements Serializable, Writable {

  private static final long serialVersionUID = 1321746085997166646L;

  private RelationIdentifier relationIdentifier;
  /**
   * parent column id
   */
  private String columnId;

  private String columnName;

  public ParentColumnTableRelation(RelationIdentifier relationIdentifier, String columId,
      String columnName) {
    this.relationIdentifier = relationIdentifier;
    this.columnId = columId;
    this.columnName = columnName;
  }

  public RelationIdentifier getRelationIdentifier() {
    return relationIdentifier;
  }

  public String getColumnId() {
    return columnId;
  }

  public String getColumnName() {
    return columnName;
  }

  @Override public void write(DataOutput out) throws IOException {
    relationIdentifier.write(out);
    out.writeUTF(columnId);
    out.writeUTF(columnName);
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.relationIdentifier = new RelationIdentifier(null, null, null);
    relationIdentifier.readFields(in);
    this.columnId = in.readUTF();
    this.columnName = in.readUTF();
  }
}
