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

package org.apache.carbondata.processing.loading.converter.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.dictionary.DictionaryServerClientDictionary;
import org.apache.carbondata.processing.loading.dictionary.PreCreatedDictionary;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class DictionaryFieldConverterImpl extends AbstractDictionaryFieldConverterImpl {

  private BiDictionary<Integer, Object> dictionaryGenerator;

  private int index;

  private CarbonDimension carbonDimension;

  private String nullFormat;

  private Dictionary dictionary;

  private DictionaryMessage dictionaryMessage;

  private boolean isEmptyBadRecord;

  public DictionaryFieldConverterImpl(CarbonColumn carbonColumn,
      String tableId, String nullFormat, int index,
      DictionaryClient client, boolean useOnePass, Map<Object, Integer> localCache,
      boolean isEmptyBadRecord, DictionaryColumnUniqueIdentifier identifier) throws IOException {
    this.index = index;
    assert carbonColumn instanceof CarbonDimension;
    this.carbonDimension = (CarbonDimension) carbonColumn;
    this.nullFormat = nullFormat;
    this.isEmptyBadRecord = isEmptyBadRecord;

    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache =
        cacheProvider.createCache(CacheType.REVERSE_DICTIONARY);

    // if use one pass, use DictionaryServerClientDictionary
    if (useOnePass) {
      if (CarbonUtil.isFileExistsForGivenColumn(identifier)) {
        dictionary = cache.get(identifier);
      }
      dictionaryMessage = new DictionaryMessage();
      dictionaryMessage.setColumnName(carbonColumn.getColName());
      // for table initialization
      dictionaryMessage.setTableUniqueId(tableId);
      dictionaryMessage.setData("0");
      // for generate dictionary
      dictionaryMessage.setType(DictionaryMessageType.DICT_GENERATION);
      dictionaryGenerator = new DictionaryServerClientDictionary(dictionary, client,
          dictionaryMessage, localCache);
    } else {
      dictionary = cache.get(identifier);
      dictionaryGenerator = new PreCreatedDictionary(dictionary);
    }
  }

  @Override public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    try {
      row.update(convert(row.getString(index), logHolder), index);
    } catch (RuntimeException e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  @Override
  public Object convert(Object value, BadRecordLogHolder logHolder)
      throws RuntimeException {
    try {
      String parsedValue = null;
      String dimensionValue = (String) value;
      if (dimensionValue == null || dimensionValue.equals(nullFormat)) {
        parsedValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
      } else {
        parsedValue = DataTypeUtil.parseValue(dimensionValue, carbonDimension);
      }
      if (null == parsedValue) {
        if ((dimensionValue.length() > 0) || (dimensionValue.length() == 0 && isEmptyBadRecord)) {
          String message = logHolder.getColumnMessageMap().get(carbonDimension.getColName());
          if (null == message) {
            message = CarbonDataProcessorUtil
                .prepareFailureReason(carbonDimension.getColName(), carbonDimension.getDataType());
            logHolder.getColumnMessageMap().put(carbonDimension.getColName(), message);
          }
          logHolder.setReason(message);
        }
        return CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY;
      } else {
        return dictionaryGenerator.getOrGenerateKey(parsedValue);
      }
    } catch (DictionaryGenerationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Method to clear out the dictionary cache.
   */
  @Override public void clear() {
    CarbonUtil.clearDictionaryCache(dictionary);
  }

  @Override
  public void fillColumnCardinality(List<Integer> cardinality) {
    cardinality.add(dictionaryGenerator.size());
  }

}
