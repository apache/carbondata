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

package org.apache.carbondata.core.datastore;

public enum ColumnType {
  // global dictionary for low cardinality dimension column
  GLOBAL_DICTIONARY,

  // for timestamp and date column
  DIRECT_DICTIONARY,

  // for high cardinality dimension column
  PLAIN_VALUE,

  // complex column (array, struct, map)
  COMPLEX,

  // measure column, numerical data type
  MEASURE,

  COMPLEX_STRUCT,

  COMPLEX_ARRAY,

  COMPLEX_PRIMITIVE,

  PLAIN_LONG_VALUE;

  public static ColumnType valueOf(int ordinal) {
    if (ordinal == GLOBAL_DICTIONARY.ordinal()) {
      return GLOBAL_DICTIONARY;
    } else if (ordinal == DIRECT_DICTIONARY.ordinal()) {
      return DIRECT_DICTIONARY;
    } else if (ordinal == PLAIN_VALUE.ordinal()) {
      return PLAIN_VALUE;
    } else if (ordinal == COMPLEX.ordinal()) {
      return COMPLEX;
    } else if (ordinal == MEASURE.ordinal()) {
      return MEASURE;
    } else if (ordinal == COMPLEX_STRUCT.ordinal()) {
      return COMPLEX_STRUCT;
    } else if (ordinal == COMPLEX_ARRAY.ordinal()) {
      return COMPLEX_ARRAY;
    } else if (ordinal == COMPLEX_PRIMITIVE.ordinal()) {
      return COMPLEX_PRIMITIVE;
    } else if (ordinal == PLAIN_LONG_VALUE.ordinal()) {
      return PLAIN_LONG_VALUE;
    } else {
      throw new RuntimeException("create ColumnType with invalid ordinal: " + ordinal);
    }
  }
}