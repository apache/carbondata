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
package org.apache.carbondata.core.dictionary.generator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

/**
 * This is the dictionary generator for all tables. It generates dictionary
 * based on @{@link DictionaryMessage}.
 */
public class ServerDictionaryGenerator implements DictionaryGenerator<Integer, DictionaryMessage> {

  /**
   * the map of tableName to TableDictionaryGenerator
   */
  private Map<String, TableDictionaryGenerator> tableMap = new ConcurrentHashMap<>();

  @Override
  public Integer generateKey(DictionaryMessage value)
      throws DictionaryGenerationException {
    initializeGeneratorForColumn(value);
    TableDictionaryGenerator generator = tableMap.get(value.getTableUniqueId());
    return generator.generateKey(value);
  }

  public void initializeGeneratorForTable(CarbonTable carbonTable) {
    // initialize TableDictionaryGenerator first
    String tableId = carbonTable.getCarbonTableIdentifier().getTableId();
    if (tableMap.get(tableId) == null) {
      synchronized (tableMap) {
        if (tableMap.get(tableId) == null) {
          tableMap.put(tableId,
              new TableDictionaryGenerator(carbonTable));
        }
      }
    }
  }

  public void initializeGeneratorForColumn(DictionaryMessage key) {
    tableMap.get(key.getTableUniqueId()).updateGenerator(key);
  }

  public Integer size(DictionaryMessage key) {
    initializeGeneratorForColumn(key);
    TableDictionaryGenerator generator = tableMap.get(key.getTableUniqueId());
    return generator.size(key);
  }

  public void writeTableDictionaryData(String tableUniqueId) throws Exception {
    TableDictionaryGenerator generator = tableMap.get(tableUniqueId);
    if (generator != null) {
      generator.writeDictionaryData();
    }
    // Remove dictionary generator after writing
    tableMap.remove(tableUniqueId);
  }

}
