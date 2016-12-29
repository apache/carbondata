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

import org.apache.carbondata.common.factory.CarbonCommonFactory;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.service.DictionaryService;

/**
 * This generator does not maintain the whole cache of dictionary. It just maintains the cache only
 * for the loading session, so what ever the dictionary values it generates in the loading session
 * it keeps in cache.
 */
public class IncrementalColumnDictionaryGenerator
    implements BiDictionary<Integer, String>, DictionaryGenerator<Integer, String>,
    DictionaryWriter {

  private final Object lock = new Object();

  private Map<String, Integer> incrementalCache = new ConcurrentHashMap<>();

  private int maxDictionary;

  private CarbonDimension dimension;

  public IncrementalColumnDictionaryGenerator(CarbonDimension dimension, int maxValue) {
    this.maxDictionary = maxValue;
    this.dimension = dimension;
  }

  @Override public Integer getOrGenerateKey(String value) throws DictionaryGenerationException {
    Integer dict = getKey(value);
    if (dict == null) {
      dict = generateKey(value);
    }
    return dict;
  }

  @Override public Integer getKey(String value) {
    return incrementalCache.get(value);
  }

  @Override public String getValue(Integer key) {
    throw new UnsupportedOperationException();
  }

  @Override public int size() {
    return maxDictionary;
  }

  @Override public Integer generateKey(String value) throws DictionaryGenerationException {
    synchronized (lock) {
      Integer dict = incrementalCache.get(value);
      if (dict == null) {
        dict = ++maxDictionary;
        incrementalCache.put(value, dict);
      }
      return dict;
    }
  }

  @Override public void writeDictionaryData() {
    // TODO write data to file system.
    DictionaryService dictionaryService = CarbonCommonFactory.getDictionaryService();
    // dictionaryService.getDictionaryWriter()
  }
}
