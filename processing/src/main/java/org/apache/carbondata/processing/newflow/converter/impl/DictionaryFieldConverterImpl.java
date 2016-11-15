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

package org.apache.carbondata.processing.newflow.converter.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.*;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryKey;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.newflow.dictionary.DictionaryServerClientDictionary;
import org.apache.carbondata.processing.newflow.dictionary.PreCreatedDictionary;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

public class DictionaryFieldConverterImpl extends AbstractDictionaryFieldConverterImpl {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DictionaryFieldConverterImpl.class.getName());

  private BiDictionary<Integer, Object> dictionaryGenerator;

  private int index;

  private CarbonDimension carbonDimension;

  private String nullFormat;

  public DictionaryFieldConverterImpl(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier, String nullFormat, int index,
      DictionaryClient client, Boolean useOnePass, String storePath) {
    this.index = index;
    this.carbonDimension = (CarbonDimension) dataField.getColumn();
    this.nullFormat = nullFormat;
    DictionaryColumnUniqueIdentifier identifier =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
            dataField.getColumn().getColumnIdentifier(), dataField.getColumn().getDataType());

    Dictionary dictionary = null;
    // if use one pass, use DictionaryServerClientDictionary
    if (useOnePass) {
      if (CarbonUtil.isFileExistsForGivenColumn(storePath, identifier)) {
        try{
          dictionary = cache.get(identifier);
        } catch (CarbonUtilException e) {
          LOGGER.error(e);
          throw new RuntimeException(e);
        }
      }
      String threadNo = "initial";
      DictionaryKey dictionaryKey = new DictionaryKey();
      dictionaryKey.setColumnName(dataField.getColumn().getColName());
      dictionaryKey.setTableUniqueName(carbonTableIdentifier.getTableUniqueName());
      dictionaryKey.setThreadNo(threadNo);
      // for table initialization
      dictionaryKey.setType("TABLE_INTIALIZATION");
      dictionaryKey.setData("0");
      client.getDictionary(dictionaryKey);
      Map<Object, Integer> localCache = new HashMap<>();
      // for generate dictionary
      dictionaryKey.setType("DICTIONARY_GENERATION");
      dictionaryGenerator = new DictionaryServerClientDictionary(dictionary, client,
              dictionaryKey, localCache);
    } else {
      try {
        dictionary = cache.get(identifier);
        dictionaryGenerator = new PreCreatedDictionary(dictionary);
      } catch (CarbonUtilException e) {
        LOGGER.error(e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override public void convert(CarbonRow row, BadRecordLogHolder logHolder)
      throws CarbonDataLoadingException {
    try {
      String parsedValue = DataTypeUtil.parseValue(row.getString(index), carbonDimension);
      if(null == parsedValue || parsedValue.equals(nullFormat)) {
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
