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

package org.apache.carbondata.hadoop.readsupport.impl;

import java.io.IOException;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

/**
 *  This is the class to decode dictionary encoded column data back to its original value.
 */
public class DictionaryDecodeReadSupport<T> implements CarbonReadSupport<T> {

  protected Dictionary[] dictionaries;

  protected DataType[] dataTypes;

  /**
   * This initialization is done inside executor task
   * for column dictionary involved in decoding.
   *
   * @param carbonColumns column list
   * @param carbonTable table identifier
   */
  @Override
  public void initialize(CarbonColumn[] carbonColumns,
      CarbonTable carbonTable) throws IOException {
    dictionaries = new Dictionary[carbonColumns.length];
    dataTypes = new DataType[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].hasEncoding(Encoding.DICTIONARY) && !carbonColumns[i]
          .hasEncoding(Encoding.DIRECT_DICTIONARY) && !carbonColumns[i].isComplex()) {
        CacheProvider cacheProvider = CacheProvider.getInstance();
        Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider
            .createCache(CacheType.FORWARD_DICTIONARY);
        dataTypes[i] = carbonColumns[i].getDataType();
        dictionaries[i] = forwardDictionaryCache.get(new DictionaryColumnUniqueIdentifier(
            carbonTable.getAbsoluteTableIdentifier(),
            carbonColumns[i].getColumnIdentifier(), dataTypes[i]));
      } else {
        dataTypes[i] = carbonColumns[i].getDataType();
      }
    }
  }

  @Override
  public T readRow(Object[] data) {
    assert (data.length == dictionaries.length);
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = dictionaries[i].getDictionaryValueForKey((int) data[i]);
      }
    }
    return (T)data;
  }

  /**
   * to book keep the dictionary cache or update access count for each
   * column involved during decode, to facilitate LRU cache policy if memory
   * threshold is reached
   */
  @Override
  public void close() {
    if (dictionaries == null) {
      return;
    }
    for (int i = 0; i < dictionaries.length; i++) {
      CarbonUtil.clearDictionaryCache(dictionaries[i]);
    }
  }
}
