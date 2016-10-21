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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.dictionary.PreCreatedDictionary;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

public class DictionaryFieldConverterImpl extends AbstractDictionaryFieldConverterImpl {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DictionaryFieldConverterImpl.class.getName());

  private BiDictionary<Integer, String> dictionaryGenerator;

  private int index;

  public DictionaryFieldConverterImpl(DataField dataField,
      Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache,
      CarbonTableIdentifier carbonTableIdentifier, int index) {
    this.index = index;
    DictionaryColumnUniqueIdentifier identifier =
        new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
            dataField.getColumn().getColumnIdentifier(), dataField.getColumn().getDataType());
    try {
      Dictionary dictionary = cache.get(identifier);
      dictionaryGenerator = new PreCreatedDictionary(dictionary);
    } catch (CarbonUtilException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void convert(CarbonRow row) throws CarbonDataLoadingException {
    try {
      row.update(dictionaryGenerator.getOrGenerateKey(row.getString(index)), index);
    } catch (DictionaryGenerationException e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  @Override
  public int getColumnCardinality() {
    return dictionaryGenerator.size();
  }
}
