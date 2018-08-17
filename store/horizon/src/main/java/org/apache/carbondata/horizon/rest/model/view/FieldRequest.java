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

import java.util.LinkedList;

import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.sdk.file.Field;

public class FieldRequest {

  private String name;
  private String dataType;
  private int precision;
  private int scale;
  private String comment;

  public FieldRequest() {

  }

  public FieldRequest(String name, String dataType) {
    this.name = name;
    this.dataType = dataType;
  }

  public FieldRequest(String name, String dataType, String comment) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
  }

  public FieldRequest(String name, String dataType, int precision, int scale, String comment) {
    this.name = name;
    this.dataType = dataType;
    this.precision = precision;
    this.scale = scale;
    this.comment = comment;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public Field convertToDto() {
    if (dataType.equalsIgnoreCase("char") ||
        dataType.equalsIgnoreCase("varchar") ||
        dataType.matches("(varchar)\\(\\d+\\)") ||
        dataType.matches("(char)\\(\\d+\\)")) {
      dataType = "string";
    } else if (dataType.equalsIgnoreCase("float")) {
      dataType = "double";
    }

    Field field = new Field(name, DataTypeUtil.valueOf(dataType));
    field.setPrecision(precision);
    field.setScale(scale);
    field.setColumnComment(comment);
    field.setChildren(new LinkedList<Field>());
    return field;
  }
}
