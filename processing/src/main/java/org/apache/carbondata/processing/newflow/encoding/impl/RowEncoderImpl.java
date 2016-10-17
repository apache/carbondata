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
package org.apache.carbondata.processing.newflow.encoding.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.encoding.FieldEncoder;
import org.apache.carbondata.processing.newflow.encoding.RowEncoder;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;

/**
 *
 */
public class RowEncoderImpl implements RowEncoder {

  private CarbonDataLoadConfiguration configuration;

  private AbstractDictionaryFieldEncoderImpl[] dictionaryFieldEncoders;

  private NonDictionaryFieldEncoderImpl[] nonDictionaryFieldEncoders;

  private MeasureFieldEncoderImpl[] measureFieldEncoders;

  public RowEncoderImpl(DataField[] fields, CarbonDataLoadConfiguration configuration) {
    this.configuration = configuration;
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache = cacheProvider
        .createCache(CacheType.REVERSE_DICTIONARY,
            configuration.getTableIdentifier().getStorePath());
    List<AbstractDictionaryFieldEncoderImpl> dictFieldEncoders = new ArrayList<>();
    List<NonDictionaryFieldEncoderImpl> nonDictFieldEncoders = new ArrayList<>();
    List<MeasureFieldEncoderImpl> measureFieldEncoderList = new ArrayList<>();

    long lruCacheStartTime = System.currentTimeMillis();

    for (int i = 0; i < fields.length; i++) {
      FieldEncoder fieldEncoder = FieldEncoderFactory.getInstance()
          .createFieldEncoder(fields[i], cache,
              configuration.getTableIdentifier().getCarbonTableIdentifier(), i);
      if (fieldEncoder instanceof AbstractDictionaryFieldEncoderImpl) {
        dictFieldEncoders.add((AbstractDictionaryFieldEncoderImpl) fieldEncoder);
      } else if (fieldEncoder instanceof NonDictionaryFieldEncoderImpl) {
        nonDictFieldEncoders.add((NonDictionaryFieldEncoderImpl) fieldEncoder);
      } else if (fieldEncoder instanceof MeasureFieldEncoderImpl) {
        measureFieldEncoderList.add((MeasureFieldEncoderImpl)fieldEncoder);
      }
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordLruCacheLoadTime((System.currentTimeMillis() - lruCacheStartTime) / 1000.0);
    dictionaryFieldEncoders =
        dictFieldEncoders.toArray(new AbstractDictionaryFieldEncoderImpl[dictFieldEncoders.size()]);
    nonDictionaryFieldEncoders = nonDictFieldEncoders
        .toArray(new NonDictionaryFieldEncoderImpl[nonDictFieldEncoders.size()]);
    measureFieldEncoders = measureFieldEncoderList
        .toArray(new MeasureFieldEncoderImpl[measureFieldEncoderList.size()]);

  }

  @Override public CarbonRow encode(CarbonRow row) throws CarbonDataLoadingException {

    Object[] newOutArray = new Object[3];

    if (dictionaryFieldEncoders.length > 0) {
      int[] dictArray = new int[dictionaryFieldEncoders.length];
      for (int i = 0; i < dictArray.length; i++) {
        dictArray[i] = dictionaryFieldEncoders[i].encode(row);
      }
      newOutArray[IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex()] = dictArray;
    }

    if (nonDictionaryFieldEncoders.length > 0) {
      ByteBuffer[] byteBuffers = new ByteBuffer[nonDictionaryFieldEncoders.length];
      for (int i = 0; i < byteBuffers.length; i++) {
        byteBuffers[i] = nonDictionaryFieldEncoders[i].encode(row);
      }

      byte[] nonDictionaryCols =
          RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBuffers);
      newOutArray[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()] = nonDictionaryCols;
    }

    if (measureFieldEncoders.length > 0) {
      Object[] measureArray = new Object[measureFieldEncoders.length];
      for (int i = 0; i < measureFieldEncoders.length; i++) {
        measureArray[i] = measureFieldEncoders[i].encode(row);
      }
      newOutArray[IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex()] = measureArray;
    }

    return new CarbonRow(newOutArray);
  }

  @Override public void finish() {
    int[] dimCardinality = new int[dictionaryFieldEncoders.length];
    for (int i = 0; i < dimCardinality.length; i++) {
      dimCardinality[i] = dictionaryFieldEncoders[i].getColumnCardinality();
    }
    // Set the cardinality to configuration, it will be used by further step for mdk key.
    configuration.setDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS, dimCardinality);
  }

}
