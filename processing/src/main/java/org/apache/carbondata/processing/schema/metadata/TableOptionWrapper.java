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
 * The class hold the table option details being used while dataload
 */
public class TableOptionWrapper {
  /**
   * map holds the table options
   */
  private static final Map<String, TableOption> mapOFOptions =
      new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  private static TableOptionWrapper tableOptionWrapper = new TableOptionWrapper();

  /**
   * to  initialize the wrapper object
   */
  private TableOptionWrapper() {
  }

  /**
   * @param input
   */
  public static void populateTableOptions(String input) {
    String[] split =
        null != input ? input.split(CarbonCommonConstants.HASH_SPC_CHARACTER) : new String[0];
    for (String str : split) {
      TableOption tableOption = new TableOption(str);
      mapOFOptions.put(tableOption.getOptionKey(), tableOption);
    }
  }

  /**
   * @param input
   */
  public static void setTableOption(String input) {
    if (null != input) {
      TableOption tableOption = new TableOption(input);
      mapOFOptions.put(tableOption.getOptionKey(), tableOption);
    }
  }

  /**
   * returns TableOptionWrapper instance
   *
   * @return
   */
  public static TableOptionWrapper getTableOptionWrapperInstance() {
    return tableOptionWrapper;
  }

  /**
   * returns the options key value
   * return null if the key is not found in the map
   *
   * @param key
   * @return
   */
  public String get(String key) {
    TableOption tableOption = mapOFOptions.get(key);
    return null != tableOption ? tableOption.getOptionValue() : null;
  }

  /**
   * return the string object
   *
   * @return
   */
  public String toString() {
    StringBuilder builder = new StringBuilder();
    Set<Map.Entry<String, TableOption>> entries = mapOFOptions.entrySet();
    Iterator<Map.Entry<String, TableOption>> iterator = entries.iterator();

    while (iterator.hasNext()) {
      Map.Entry<String, TableOption> entry = iterator.next();
      builder.append(entry.getValue().toString());
      builder.append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    return builder.toString();
  }
}
