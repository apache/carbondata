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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.dictionary.DictionaryServerClientDictionary;
import org.apache.carbondata.processing.loading.dictionary.PreCreatedDictionary;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class DictionaryFieldConverterImpl extends AbstractDictionaryFieldConverterImpl {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DictionaryFieldConverterImpl.class.getName());

  private BiDictionary<Integer, Object> dictionaryGenerator;

  private int index;

  private CarbonDimension carbonDimension;

  private String nullFormat;

  private Dictionary dictionary;

  private DictionaryMessage dictionaryMessage;

  private boolean isEmptyBadRecord;

  public DictionaryFieldConverterImpl(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier, String nullFormat, int index,
      DictionaryClient client, boolean useOnePass, String storePath,
      Map<Object, Integer> localCache, boolean isEmptyBadRecord,
      DictionaryColumnUniqueIdentifier identifier) throws IOException {
    this.index = index;
    this.carbonDimension = (CarbonDimension) dataField.getColumn();
    this.nullFormat = nullFormat;
    this.isEmptyBadRecord = isEmptyBadRecord;

    // if use one pass, use DictionaryServerClientDictionary
    if (useOnePass) {
      if (CarbonUtil.isFileExistsForGivenColumn(storePath, identifier)) {
        dictionary = cache.get(identifier);
      }
      dictionaryMessage = new DictionaryMessage();
      dictionaryMessage.setColumnName(dataField.getColumn().getColName());
      // for table initialization
      dictionaryMessage.setTableUniqueId(carbonTableIdentifier.getTableId());
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
      String parsedValue = null;
      String dimensionValue = row.getString(index);
      if (dimensionValue == null || dimensionValue.equals(nullFormat)) {
        parsedValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
      } else {
        parsedValue = DataTypeUtil.parseValue(dimensionValue, carbonDimension);
      }
      if (null == parsedValue) {
        if ((dimensionValue.length() > 0) || (dimensionValue.length() == 0 && isEmptyBadRecord)) {
          String message = logHolder.getColumnMessageMap().get(carbonDimension.getColName());
          if (null == message) {
            message = CarbonDataProcessorUtil.prepareFailureReason(
                carbonDimension.getColName(), carbonDimension.getDataType());
            logHolder.getColumnMessageMap().put(carbonDimension.getColName(), message);
          } logHolder.setReason(message);
        }
        row.update(CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY, index);
      } else {
        row.update(dictionaryGenerator.getOrGenerateKey(parsedValue), index);
      }
    } catch (DictionaryGenerationException e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  @Override
  public void fillColumnCardinality(List<Integer> cardinality) {
    cardinality.add(dictionaryGenerator.size());
  }

}
