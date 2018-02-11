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

package org.apache.carbondata.sdk.file;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

/**
 * A field represent one column
 */
@InterfaceAudience.User
@InterfaceStability.Unstable
public class Field {

  private String name;
  private DataType type;

  public Field(String name, String type) {
    this.name = name;
    if (type.equalsIgnoreCase("string")) {
      this.type = DataTypes.STRING;
    } else if (type.equalsIgnoreCase("date")) {
      this.type = DataTypes.DATE;
    } else if (type.equalsIgnoreCase("timestamp")) {
      this.type = DataTypes.TIMESTAMP;
    } else if (type.equalsIgnoreCase("boolean")) {
      this.type = DataTypes.BOOLEAN;
    } else if (type.equalsIgnoreCase("byte")) {
      this.type = DataTypes.BYTE;
    } else if (type.equalsIgnoreCase("short")) {
      this.type = DataTypes.SHORT;
    } else if (type.equalsIgnoreCase("int")) {
      this.type = DataTypes.INT;
    } else if (type.equalsIgnoreCase("long")) {
      this.type = DataTypes.LONG;
    } else if (type.equalsIgnoreCase("float")) {
      this.type = DataTypes.FLOAT;
    } else if (type.equalsIgnoreCase("double")) {
      this.type = DataTypes.DOUBLE;
    } else {
      throw new IllegalArgumentException("unsupported data type: " + type);
    }
  }

  public Field(String name, DataType type) {
    this.name = name;
    this.type = type;
  }

  public String getFieldName() {
    return name;
  }

  public DataType getDataType() {
    return type;
  }
}
