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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;

import org.apache.hadoop.io.Writable;

public class TableDescriptor implements Writable {

  private TableIdentifier table;
  private boolean ifNotExists;
  private String tablePath;
  private Schema schema;
  private Map<String, String> properties;
  private String comment;

  private TableDescriptor() {
  }

  public TableDescriptor(TableIdentifier table, Schema schema,
      Map<String, String> properties, String tablePath, String comment, boolean ifNotExists) {
    Objects.requireNonNull(table);
    Objects.requireNonNull(schema);
    this.table = table;
    this.ifNotExists = ifNotExists;
    this.schema = schema;
    this.properties = properties;
    this.tablePath = tablePath;
    this.comment = comment;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public TableIdentifier getTable() {
    return table;
  }

  public void setTable(TableIdentifier table) {
    this.table = table;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public void setTablePath(String tablePath) {
    this.tablePath = tablePath;
  }

  public String getTablePath() {
    return tablePath;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    table.write(out);
    out.writeBoolean(ifNotExists);
    out.writeBoolean(tablePath != null);
    if (tablePath != null) {
      out.writeUTF(tablePath);
    }
    out.writeUTF(ObjectSerializationUtil.convertObjectToString(schema));
    out.writeInt(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
    out.writeUTF(comment);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    table = new TableIdentifier();
    table.readFields(in);
    ifNotExists = in.readBoolean();
    if (in.readBoolean()) {
      tablePath = in.readUTF();
    }
    schema = (Schema) ObjectSerializationUtil.convertStringToObject(in.readUTF());
    int size = in.readInt();
    properties = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      properties.put(in.readUTF(), in.readUTF());
    }
    comment = in.readUTF();
  }

  public static class Builder {

    private TableDescriptor table;
    private List<Field> fields;
    private Map<String, String> tblProperties;

    private Builder() {
      table = new TableDescriptor();
      fields = new ArrayList<>();
      tblProperties = new HashMap<>();
    }

    public Builder ifNotExists() {
      table.setIfNotExists(true);
      return this;
    }

    public Builder table(TableIdentifier tableId) {
      table.setTable(tableId);
      return this;
    }

    public Builder tablePath(String tablePath) {
      table.setTablePath(tablePath);
      return this;
    }

    public Builder comment(String tableComment) {
      table.setComment(tableComment);
      return this;
    }

    public Builder column(String name, DataType dataType) {
      fields.add(new Field(name, dataType));
      return this;
    }

    public Builder column(String name, DataType dataType, String comment) {
      Field field = new Field(name, dataType);
      field.setColumnComment(comment);
      fields.add(field);
      return this;
    }

    public Builder column(String name, DataType dataType, int precision, int scale, String comment)
    {
      Field field = new Field(name, dataType);
      field.setColumnComment(comment);
      field.setScale(scale);
      field.setPrecision(precision);
      fields.add(field);
      return this;
    }

    public Builder tblProperties(String key, String value) {
      tblProperties.put(key, value);
      return this;
    }

    public TableDescriptor create() {
      Field[] fieldArray = new Field[fields.size()];
      fieldArray = fields.toArray(fieldArray);
      Schema schema = new Schema(fieldArray);
      table.setSchema(schema);
      table.setProperties(tblProperties);
      return table;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
