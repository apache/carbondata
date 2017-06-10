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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Dictionary generation for table.
 */
public class TableDictionaryGenerator
    implements DictionaryGenerator<Integer, DictionaryMessage>, DictionaryWriter {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(TableDictionaryGenerator.class.getName());

  /**
   * the map of columnName to dictionaryGenerator
   */
  private Map<String, DictionaryGenerator<Integer, String>> columnMap = new ConcurrentHashMap<>();

  public TableDictionaryGenerator(CarbonDimension dimension) {
    columnMap.put(dimension.getColumnId(),
            new IncrementalColumnDictionaryGenerator(dimension, 1));
  }

  @Override
  public Integer generateKey(DictionaryMessage value)
      throws DictionaryGenerationException {
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = metadata.getCarbonTable(value.getTableUniqueName());
    CarbonDimension dimension = carbonTable.getPrimitiveDimensionByName(
            value.getTableUniqueName(), value.getColumnName());

    DictionaryGenerator<Integer, String> generator =
            columnMap.get(dimension.getColumnId());
    return generator.generateKey(value.getData());
  }

  public Integer size(DictionaryMessage key) {
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = metadata.getCarbonTable(key.getTableUniqueName());
    CarbonDimension dimension = carbonTable.getPrimitiveDimensionByName(
            key.getTableUniqueName(), key.getColumnName());

    DictionaryGenerator<Integer, String> generator =
            columnMap.get(dimension.getColumnId());
    return ((BiDictionary) generator).size();
  }

  @Override public void writeDictionaryData(String tableUniqueName) {
    int numOfCores = 1;
    final String tableName = tableUniqueName;
    try {
      numOfCores = Integer.parseInt(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
                      CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException e) {
      numOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    long start = System.currentTimeMillis();
    List<Future<Void>> taskSubmitList =
            new ArrayList<>(columnMap.size());
    ExecutorService executorService = Executors.newFixedThreadPool(numOfCores);
    for (final DictionaryGenerator generator: columnMap.values()) {
      taskSubmitList.add(executorService.submit(new Callable<Void>() {
        @Override public Void call() throws Exception {
          ((DictionaryWriter) (generator)).writeDictionaryData(tableName);
          return null;
        }
      }));
    }

    try {
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOGGER.error("Error loading the dictionary: " + e.getMessage());
    }
    LOGGER.audit("Total time taken to write dictionary file is: " +
            (System.currentTimeMillis() - start));
  }

  public void updateGenerator(CarbonDimension dimension) {
    // reuse dictionary generator
    if (null == columnMap.get(dimension.getColumnId())) {
      columnMap.put(dimension.getColumnId(),
        new IncrementalColumnDictionaryGenerator(dimension, 1));
    }
  }
}
