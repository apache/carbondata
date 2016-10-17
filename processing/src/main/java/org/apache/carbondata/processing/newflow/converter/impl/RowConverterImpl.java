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

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.converter.FieldConverter;
import org.apache.carbondata.processing.newflow.converter.RowConverter;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;

/**
 * It converts the complete row if necessary, dictionary columns are encoded with dictionary values
 * and nondictionary values are converted to binary.
 */
public class RowConverterImpl implements RowConverter {

  private CarbonDataLoadConfiguration configuration;

  private FieldConverter[] fieldConverters;

  public RowConverterImpl(DataField[] fields, CarbonDataLoadConfiguration configuration) {
    this.configuration = configuration;
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache =
        cacheProvider.createCache(CacheType.REVERSE_DICTIONARY,
            configuration.getTableIdentifier().getStorePath());
    List<FieldConverter> fieldConverterList = new ArrayList<>();

    long lruCacheStartTime = System.currentTimeMillis();

    for (int i = 0; i < fields.length; i++) {
      FieldConverter fieldConverter = FieldEncoderFactory.getInstance()
          .createFieldEncoder(fields[i], cache,
              configuration.getTableIdentifier().getCarbonTableIdentifier(), i);
      if (fieldConverter != null) {
        fieldConverterList.add(fieldConverter);
      }
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordLruCacheLoadTime((System.currentTimeMillis() - lruCacheStartTime) / 1000.0);
    fieldConverters = fieldConverterList.toArray(new FieldConverter[fieldConverterList.size()]);
  }

  @Override
  public CarbonRow convert(CarbonRow row) throws CarbonDataLoadingException {
    for (int i = 0; i < fieldConverters.length; i++) {
      fieldConverters[i].convert(row);
    }
    return row;
  }

  @Override
  public void finish() {
    List<Integer> dimCardinality = new ArrayList<>();
    for (int i = 0; i < fieldConverters.length; i++) {
      if (fieldConverters[i] instanceof AbstractDictionaryFieldConverterImpl) {
        dimCardinality.add(
            ((AbstractDictionaryFieldConverterImpl) fieldConverters[i]).getColumnCardinality());
      }
    }
    int[] cardinality = new int[dimCardinality.size()];
    for (int i = 0; i < dimCardinality.size(); i++) {
      cardinality[i] = dimCardinality.get(i);
    }
    // Set the cardinality to configuration, it will be used by further step for mdk key.
    configuration.setDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS, cardinality);
  }

}
