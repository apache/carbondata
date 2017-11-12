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
import java.util.HashMap;
import java.util.Map;

/**
 * Child schema class to maintain the child table details inside parent table
 */
public class DataMapSchema implements Serializable, Writable {

  private static final long serialVersionUID = 6577149126264181553L;

  private String dataMapName;

  private String className;

  private RelationIdentifier relationIdentifier;
  /**
   * child table schema
   */
  private TableSchema childSchema;

  /**
   * relation properties
   */
  private Map<String, String> properties;

  public DataMapSchema() {
  }

  public DataMapSchema(String dataMapName, String className) {
    this.dataMapName = dataMapName;
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  public TableSchema getChildSchema() {
    return childSchema;
  }

  public RelationIdentifier getRelationIdentifier() {
    return relationIdentifier;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setRelationIdentifier(RelationIdentifier relationIdentifier) {
    this.relationIdentifier = relationIdentifier;
  }

  public void setChildSchema(TableSchema childSchema) {
    this.childSchema = childSchema;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public String getDataMapName() {
    return dataMapName;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeUTF(dataMapName);
    out.writeUTF(className);
    boolean isRelationIdentifierExists = null != relationIdentifier;
    out.writeBoolean(isRelationIdentifierExists);
    if (isRelationIdentifierExists) {
      this.relationIdentifier.write(out);
    }
    boolean isChildSchemaExists = null != this.childSchema;
    out.writeBoolean(isChildSchemaExists);
    if (isChildSchemaExists) {
      this.childSchema.write(out);
    }
    if (properties == null) {
      out.writeShort(0);
    } else {
      out.writeShort(properties.size());
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeUTF(entry.getValue());
      }
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.dataMapName = in.readUTF();
    this.className = in.readUTF();
    boolean isRelationIdnentifierExists = in.readBoolean();
    if (isRelationIdnentifierExists) {
      this.relationIdentifier = new RelationIdentifier();
      this.relationIdentifier.readFields(in);
    }
    boolean isChildSchemaExists = in.readBoolean();
    if (isChildSchemaExists) {
      this.childSchema = new TableSchema();
      this.childSchema.readFields(in);
    }

    int mapSize = in.readShort();
    this.properties = new HashMap<>(mapSize);
    for (int i = 0; i < mapSize; i++) {
      String key = in.readUTF();
      String value = in.readUTF();
      this.properties.put(key, value);
    }

  }
}
