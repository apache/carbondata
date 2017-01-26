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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * Wrapper class to hold the columnschema details
 */
public class ColumnSchemaDetailsWrapper {

  /**
   * Map of the ColumnSchemaDetails
   */
  private Map<String, ColumnSchemaDetails> columnSchemaDetailsMap;

  /**
   * return the string object
   *
   * @return
   */
  public String toString() {
    StringBuilder builder = new StringBuilder();
    Set<Map.Entry<String, ColumnSchemaDetails>> entries = columnSchemaDetailsMap.entrySet();
    Iterator<Map.Entry<String, ColumnSchemaDetails>> iterator = entries.iterator();

    while (iterator.hasNext()) {
      Map.Entry<String, ColumnSchemaDetails>  entry = iterator.next();
      builder.append(entry.getKey());
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      builder.append(entry.getValue().toString());
      if (iterator.hasNext()) {
        builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
      }
    }
    return builder.toString();
  }

  /**
   * default constructor
   */
  public ColumnSchemaDetailsWrapper() {

  }

  /**
   * Constructor take serialized string as input and populates the List of columnschema details
   *
   * @param input
   */
  public ColumnSchemaDetailsWrapper(String input) {
    columnSchemaDetailsMap = new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    String[] split = input.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
    for (int i = 0; i < split.length; i++) {
      String key = split[i++];
      ColumnSchemaDetails details = new ColumnSchemaDetails(split[i]);
      columnSchemaDetailsMap.put(key, details);
    }
  }

  /**
   * returns ColumnSchemaDetails of all columns
   *
   * @return
   */
  public Map<String, ColumnSchemaDetails> getColumnSchemaDetailsMap() {
    return columnSchemaDetailsMap;
  }

  /**
   * sets the map of column schema
   *
   * @param columnSchemaDetailsMap
   */
  public void setColumnSchemaDetailsMap(Map<String, ColumnSchemaDetails> columnSchemaDetailsMap) {
    this.columnSchemaDetailsMap = columnSchemaDetailsMap;
  }

  /**
   * returns the columnSchemaDetails of requested column
   *
   * @param key
   * @return
   */
  public ColumnSchemaDetails get(String key) {
    return columnSchemaDetailsMap.get(key);
  }
}
