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

package org.apache.carbondata.core.constants;

/**
 * This enum is used for determining the indexes of the
 * dimension,ignoreDictionary,measure columns.
 */
public enum IgnoreDictionary {
  /**
   * POSITION WHERE DIMENSIONS R STORED IN OBJECT ARRAY.
   */
  DIMENSION_INDEX_IN_ROW(0),

  /**
   * POSITION WHERE BYTE[] (high cardinality) IS STORED IN OBJECT ARRAY.
   */
  BYTE_ARRAY_INDEX_IN_ROW(1),

  /**
   * POSITION WHERE MEASURES R STORED IN OBJECT ARRAY.
   */
  MEASURES_INDEX_IN_ROW(2);

  private final int index;

  IgnoreDictionary(int index) {
    this.index = index;
  }

  public int getIndex() {
    return this.index;
  }

}
