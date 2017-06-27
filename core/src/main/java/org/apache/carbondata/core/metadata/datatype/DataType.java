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

  STRING(0, "STRING", -1),
  DATE(1, "DATE", -1),
  TIMESTAMP(2, "TIMESTAMP", -1),
  BOOLEAN(1, "BOOLEAN", 1),
  SHORT(2, "SMALLINT", 2),
  INT(3, "INT", 4),
  FLOAT(4, "FLOAT", 4),
  LONG(5, "BIGINT", 8),
  DOUBLE(6, "DOUBLE", 8),
  NULL(7, "NULL", 1),
  DECIMAL(8, "DECIMAL", -1),
  ARRAY(9, "ARRAY", -1),
  STRUCT(10, "STRUCT", -1),
  MAP(11, "MAP", -1),
  BYTE(12, "BYTE", 1),

  // internal use only, for variable length data type
  BYTE_ARRAY(13, "BYTE_ARRAY", -1),
  // internal use only, for value compression from integer/long to 3 bytes value
  SHORT_INT(14, "SHORT_INT", 3);

  private int precedenceOrder;
  private String name;

  // size of the value of this data type, negative value means variable length
  private int sizeInBytes;

  DataType(int value ,String name, int sizeInBytes) {
    this.precedenceOrder = value;
    this.name = name;
    this.sizeInBytes = sizeInBytes;
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

  public int getSizeInBytes() {
    return sizeInBytes;
  }

  public int getSizeBits() {
    return (int) (Math.log(getSizeInBytes()) / Math.log(2));
  }
}
