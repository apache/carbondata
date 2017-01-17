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

package org.apache.carbondata.processing.schema.metadata;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Class holds the common column schema details needed for the data load
 */
public class ColumnSchemaDetails {

  /**
   * column Name
   */
  private String columnName;
  /**
   * column datatype
   */
  private DataType columnType;
  /**
   * boolean to identify direct dictionary column
   */
  private Boolean isDirectDictionary;

  /**
   * Constructor to initialize object from the input string separated by comma (,)
   *
   * @param input
   */
  ColumnSchemaDetails(String input) {
    String[] splits = input.split(",");
    columnName = splits[0];
    columnType = DataTypeUtil.getDataType(splits[1]);
    isDirectDictionary = Boolean.parseBoolean(splits[2]);
  }

  /**
   * Constructor to initialize the ColumnSchemaDetails
   *
   * @param columnName
   * @param columnType
   * @param isDirectDictionary
   */
  public ColumnSchemaDetails(String columnName, DataType columnType, Boolean isDirectDictionary) {
    this.columnName = columnName;
    this.columnType = columnType;
    this.isDirectDictionary = isDirectDictionary;

  }

  /**
   * returns the ColumnName
   *
   * @return
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * returns the dataType of the column
   *
   * @return
   */
  public DataType getColumnType() {
    return columnType;
  }

  /**
   * returns boolean value to identify direct dictionary
   *
   * @return
   */
  public Boolean isDirectDictionary() {
    return isDirectDictionary;
  }

  /**
   * @return
   */
  public String toString() {
    return columnName + "," + columnType + "," + isDirectDictionary;
  }
}
