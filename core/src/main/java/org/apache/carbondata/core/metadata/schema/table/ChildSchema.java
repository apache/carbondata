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
 * Child schema class to maintain the child table details inside parent table
 */
public class ChildSchema implements Serializable, Writable {

  private RelationIdentifier relationIdentifier;
  /**
   * child table schema
   */
  private TableSchema tableSchema;

  /**
   * relation properties
   */
  private RelationProperties relationProperties;

  public ChildSchema(RelationIdentifier relationIdentifier, TableSchema tableSchema,
      RelationProperties relationProperties) {
    this.relationIdentifier = relationIdentifier;
    this.tableSchema = tableSchema;
    this.relationProperties = relationProperties;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public RelationProperties getRelationProperties() {
    return relationProperties;
  }

  public RelationIdentifier getRelationIdentifier() {
    return relationIdentifier;
  }

  @Override public void write(DataOutput out) throws IOException {
    this.relationIdentifier.write(out);
    this.tableSchema.write(out);
    this.relationProperties.write(out);
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.relationIdentifier = new RelationIdentifier(null, null, null);
    this.relationIdentifier.readFields(in);
    this.tableSchema = new TableSchema();
    this.tableSchema.readFields(in);
    this.relationProperties = new RelationProperties(RelationType.AGGREGATION, null);
    this.relationProperties.readFields(in);
  }

  @Override public int hashCode() {
    return tableSchema.hashCode();
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ChildSchema other = (ChildSchema) obj;
    if (tableSchema == null) {
      return other.tableSchema == null;
    }
    return tableSchema.equals(other.tableSchema);
  }

}
