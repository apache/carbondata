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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.log4j.Logger;

/**
 * Dictionary generation for table.
 */
public class TableDictionaryGenerator
    implements DictionaryGenerator<Integer, DictionaryMessage>, DictionaryWriter {

  private static final Logger LOGGER =
          LogServiceFactory.getLogService(TableDictionaryGenerator.class.getName());

  private CarbonTable carbonTable;
  /**
   * the map of columnName to dictionaryGenerator
   */
  private Map<String, DictionaryGenerator<Integer, String>> columnMap = new ConcurrentHashMap<>();

  public TableDictionaryGenerator(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
  }

  @Override
  public Integer generateKey(DictionaryMessage value)
      throws DictionaryGenerationException {
    CarbonDimension dimension = carbonTable.getPrimitiveDimensionByName(value.getColumnName());

    if (null == dimension) {
      throw new DictionaryGenerationException("Dictionary Generation Failed");
    }
    DictionaryGenerator<Integer, String> generator =
            columnMap.get(dimension.getColumnId());
    return generator.generateKey(value.getData());
  }

  public Integer size(DictionaryMessage key) {
    CarbonDimension dimension = carbonTable.getPrimitiveDimensionByName(key.getColumnName());

    if (null == dimension) {
      return 0;
    }
    DictionaryGenerator<Integer, String> generator =
            columnMap.get(dimension.getColumnId());
    return ((BiDictionary) generator).size();
  }

  @Override public void writeDictionaryData() {
    int numOfCores = CarbonProperties.getInstance().getNumberOfLoadingCores();
    long start = System.currentTimeMillis();
    ExecutorService executorService = Executors.newFixedThreadPool(numOfCores);
    for (final DictionaryGenerator generator : columnMap.values()) {
      executorService.execute(new WriteDictionaryDataRunnable(generator));
    }

    try {
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOGGER.error("Error loading the dictionary: " + e.getMessage());
    }
    LOGGER.info("Total time taken to write dictionary file is: " +
            (System.currentTimeMillis() - start));
  }

  public void updateGenerator(DictionaryMessage key) {
    CarbonDimension dimension = carbonTable
        .getPrimitiveDimensionByName(key.getColumnName());
    if (null != dimension && null == columnMap.get(dimension.getColumnId())) {
      synchronized (columnMap) {
        if (null == columnMap.get(dimension.getColumnId())) {
          columnMap.put(dimension.getColumnId(),
              new IncrementalColumnDictionaryGenerator(dimension, 1, carbonTable));
        }
      }
    }
  }

  private static class WriteDictionaryDataRunnable implements Runnable {
    private final DictionaryGenerator generator;

    public WriteDictionaryDataRunnable(DictionaryGenerator generator) {
      this.generator = generator;
    }

    @Override public void run() {
      try {
        ((DictionaryWriter)generator).writeDictionaryData();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
