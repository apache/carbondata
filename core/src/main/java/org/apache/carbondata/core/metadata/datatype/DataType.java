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

package org.apache.carbondata.core.metadata.datatype;

public enum DataType {

  STRING(0, "STRING"),
  DATE(1, "DATE"),
  TIMESTAMP(2, "TIMESTAMP"),
  BOOLEAN(1, "BOOLEAN"),
  SHORT(2, "SMALLINT"),
  INT(3, "INT"),
  FLOAT(4, "FLOAT"),
  LONG(5, "BIGINT"),
  DOUBLE(6, "DOUBLE"),
  NULL(7, "NULL"),
  DECIMAL(8, "DECIMAL"),
  ARRAY(9, "ARRAY"),
  STRUCT(10, "STRUCT"),
  MAP(11, "MAP"),
  BYTE(12, "BYTE"),

  // internal use only
  BYTE_ARRAY(13, "BYTE ARRAY");

  private int precedenceOrder;
  private String name ;

  DataType(int value ,String  name) {
    this.precedenceOrder = value;
    this.name = name;
  }

  public int getPrecedenceOrder() {
    return precedenceOrder;
  }

  public String getName() {
    return name;
  }

  public boolean isComplexType() {
    return precedenceOrder >= 9 && precedenceOrder <= 11;
  }
}
