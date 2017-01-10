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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.factory.CarbonCommonFactory;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnIdentifier;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.devapi.DictionaryGenerator;
import org.apache.carbondata.core.service.DictionaryService;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfo;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortInfoPreparator;


/**
 * This generator does not maintain the whole cache of dictionary. It just maintains the cache only
 * for the loading session, so what ever the dictionary values it generates in the loading session
 * it keeps in cache.
 */
public class IncrementalColumnDictionaryGenerator implements BiDictionary<Integer, String>,
        DictionaryGenerator<Integer, String>, DictionaryWriter {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(IncrementalColumnDictionaryGenerator.class.getName());

  private final Object lock = new Object();

  private Map<String, Integer> incrementalCache = new ConcurrentHashMap<>();

  private Map<Integer, String> reverseIncrementalCache = new ConcurrentHashMap<>();

  private int currentDictionarySize;

  private CarbonDimension dimension;

  public IncrementalColumnDictionaryGenerator(CarbonDimension dimension, int maxValue) {
    this.currentDictionarySize = maxValue;
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
    return reverseIncrementalCache.get(key);
  }

  @Override public int size() {
    return currentDictionarySize;
  }

  @Override public Integer generateKey(String value) throws DictionaryGenerationException {
    synchronized (lock) {
      Integer dict = incrementalCache.get(value);
      if (dict == null) {
        dict = ++currentDictionarySize;
        incrementalCache.put(value, dict);
        reverseIncrementalCache.put(dict, value);
      }
      return dict;
    }
  }

  @Override public void writeDictionaryData(String tableUniqueName) throws IOException {
    // initialize params
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = metadata.getCarbonTable(tableUniqueName);
    CarbonTableIdentifier tableIdentifier = carbonTable.getCarbonTableIdentifier();
    ColumnIdentifier columnIdentifier = dimension.getColumnIdentifier();
    String storePath = carbonTable.getStorePath();
    DictionaryService dictionaryService = CarbonCommonFactory.getDictionaryService();
    // create dictionary cache from dictionary File
    DictionaryColumnUniqueIdentifier identifier =
            new DictionaryColumnUniqueIdentifier(tableIdentifier, columnIdentifier,
                    columnIdentifier.getDataType());
    Boolean isDictExists = CarbonUtil.isFileExistsForGivenColumn(storePath, identifier);
    Dictionary dictionary = null;
    long t1 = System.currentTimeMillis();
    if (isDictExists) {
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> dictCache = CacheProvider.getInstance()
              .createCache(CacheType.REVERSE_DICTIONARY, storePath);
      dictionary = dictCache.get(identifier);
    }
    long dictCacheTime = System.currentTimeMillis() - t1;
    long t2 = System.currentTimeMillis();
    // write dictionary
    CarbonDictionaryWriter dictionaryWriter = null;
    dictionaryWriter = dictionaryService
            .getDictionaryWriter(tableIdentifier, columnIdentifier, storePath);
    List<String> distinctValues = writeDictionary(dictionaryWriter, isDictExists);
    long dictWriteTime = System.currentTimeMillis() - t2;
    long t3 = System.currentTimeMillis();
    // write sort index
    if (distinctValues.size() > 0) {
      writeSortIndex(distinctValues, dictionary,
              dictionaryService, tableIdentifier, columnIdentifier, storePath);
    }
    long sortIndexWriteTime = System.currentTimeMillis() - t3;
    // update Meta Data
    updateMetaData(dictionaryWriter);
    LOGGER.audit("\n columnName: " + dimension.getColName() +
            "\n columnId: " + dimension.getColumnId() +
            "\n new distinct values count: " + distinctValues.size() +
            "\n create dictionary cache: " + dictCacheTime +
            "\n sort list, distinct and write: " + dictWriteTime +
            "\n write sort info: " + sortIndexWriteTime);
  }

  /**
   * write dictionary to file
   *
   * @param dictionaryWriter
   * @param isDictExists
   * @return
   * @throws IOException
   */
  private List<String> writeDictionary(CarbonDictionaryWriter dictionaryWriter,
                                       Boolean isDictExists) throws IOException {
    List<String> distinctValues = new ArrayList<>();
    try {
      if (!isDictExists) {
        dictionaryWriter.write(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        distinctValues.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
      }
      // write value to dictionary file
      if (reverseIncrementalCache.size() > 0) {
        for (int index = 2; index < reverseIncrementalCache.size() + 2; index++) {
          String value = reverseIncrementalCache.get(index);
          String parsedValue = DataTypeUtil
                  .normalizeColumnValueForItsDataType(value, dimension);
          if (null != parsedValue) {
            dictionaryWriter.write(parsedValue);
            distinctValues.add(parsedValue);
          }
        }
      }
    } finally {
      if (null != dictionaryWriter) {
        dictionaryWriter.close();
      }
    }

    return distinctValues;
  }

  /**
   * write dictionary sort index to file
   *
   * @param distinctValues
   * @param dictionary
   * @param dictionaryService
   * @param tableIdentifier
   * @param columnIdentifier
   * @param storePath
   * @throws IOException
   */
  private void writeSortIndex(List<String> distinctValues,
                              Dictionary dictionary,
                              DictionaryService dictionaryService,
                              CarbonTableIdentifier tableIdentifier,
                              ColumnIdentifier columnIdentifier,
                              String storePath) throws IOException{
    CarbonDictionarySortIndexWriter carbonDictionarySortIndexWriter = null;
    try {
      CarbonDictionarySortInfoPreparator preparator = new CarbonDictionarySortInfoPreparator();
      CarbonDictionarySortInfo dictionarySortInfo =
              preparator.getDictionarySortInfo(distinctValues, dictionary,
                      dimension.getDataType());
      carbonDictionarySortIndexWriter =
              dictionaryService.getDictionarySortIndexWriter(tableIdentifier, columnIdentifier,
                      storePath);
      carbonDictionarySortIndexWriter.writeSortIndex(dictionarySortInfo.getSortIndex());
      carbonDictionarySortIndexWriter
              .writeInvertedSortIndex(dictionarySortInfo.getSortIndexInverted());
    } finally {
      if (null != carbonDictionarySortIndexWriter) {
        carbonDictionarySortIndexWriter.close();
      }
    }
  }

  /**
   * update dictionary metadata
   *
   * @param dictionaryWriter
   * @throws IOException
   */
  private void updateMetaData(CarbonDictionaryWriter dictionaryWriter) throws IOException{
    if (null != dictionaryWriter) {
      dictionaryWriter.commit();
    }
  }

}
