/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.dictionary.generator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;

/**
 * This is the dictionary generator for all tables. It generates dictionary
 * based on @{@link DictionaryKey}.
 */
public class DictionaryGeneratorForServer implements DictionaryGenerator<Integer, DictionaryKey> {

  private Map<String, TableDictionaryGenerator> tableMap = new ConcurrentHashMap<>();

  @Override public Integer generateKey(DictionaryKey value) throws DictionaryGenerationException {
    TableDictionaryGenerator generator = tableMap.get(value.getTableUniqueName());
    assert generator != null : "Table intialization for generator is not done";
    return generator.generateKey(value);
  }

  public void initializeGeneratorForTable(DictionaryKey key) {
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = metadata.getCarbonTable(key.getTableUniqueName());
    tableMap.put(key.getTableUniqueName(), new TableDictionaryGenerator(carbonTable));
  }

  public Integer size(DictionaryKey key) {
    TableDictionaryGenerator generator = tableMap.get(key.getTableUniqueName());
    assert generator != null : "Table intialization for generator is not done";
    return generator.size(key);
  }

  // write dictionary data
  public void writeDictionaryData() {
    for (TableDictionaryGenerator generator : tableMap.values()) {
      ((DictionaryWriter) generator).writeDictionaryData();
    }
  }

}
