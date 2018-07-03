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

package org.apache.carbondata.horizon.rest.model.view;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.horizon.rest.model.descriptor.TableDescriptor;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;

public class CreateTableRequest {

  private boolean ifNotExists;
  private String databaseName;
  private String tableName;
  private FieldRequest[] fields;
  private Map<String, String> properties;
  private String comment;

  public CreateTableRequest() {

  }

  public CreateTableRequest(boolean ifNotExists, String databaseName, String tableName,
      FieldRequest[] fields, Map<String, String> properties, String comment) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.ifNotExists = ifNotExists;
    this.fields = fields;
    this.properties = properties;
    this.comment = comment;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public FieldRequest[] getFields() {
    return fields;
  }

  public void setFields(FieldRequest[] fields) {
    this.fields = fields;
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

  public TableDescriptor convertToDto() {
    Field[] schemaFields = new Field[fields.length];
    Schema schema = new Schema(schemaFields);
    for (int i = 0; i < fields.length; i++) {
      schemaFields[i] = fields[i].convertToDto();
      schemaFields[i].setSchemaOrdinal(i);
    }
    return new TableDescriptor(ifNotExists, databaseName, tableName, schema, properties, comment);
  }

  public static class Builder {

    private CreateTableRequest table;
    private List<FieldRequest> fields;
    private Map<String, String> tblProperties;

    private Builder() {
      table = new CreateTableRequest();
      fields = new ArrayList<>();
      tblProperties = new HashMap<>();
    }

    public Builder ifNotExists() {
      table.setIfNotExists(true);
      return this;
    }

    public Builder databaseName(String databaseName) {
      table.setDatabaseName(databaseName);
      return this;
    }

    public Builder tableName(String tableName) {
      table.setTableName(tableName);
      return this;
    }

    public Builder comment(String comment) {
      table.setComment(comment);
      return this;
    }

    public Builder column(String name, String dataType) {
      fields.add(new FieldRequest(name, dataType));
      return this;
    }

    public Builder column(String name, String dataType, String comment) {
      fields.add(new FieldRequest(name, dataType, comment));
      return this;
    }

    public Builder column(String name, String dataType, int precision, int scale, String comment) {
      fields.add(new FieldRequest(name, dataType, precision, scale, comment));
      return this;
    }

    public Builder tblProperties(String key, String value) {
      tblProperties.put(key, value);
      return this;
    }

    public CreateTableRequest create() {
      FieldRequest[] fieldArray = new FieldRequest[fields.size()];
      fieldArray = fields.toArray(fieldArray);
      table.setFields(fieldArray);
      table.setProperties(tblProperties);
      return table;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
