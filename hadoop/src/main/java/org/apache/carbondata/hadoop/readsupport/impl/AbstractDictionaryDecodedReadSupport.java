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
package org.apache.carbondata.hadoop.readsupport.impl;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

/**
 * Its an abstract class provides necessary information to decode dictionary data
 */
public abstract class AbstractDictionaryDecodedReadSupport<T> implements CarbonReadSupport<T> {

  protected Dictionary[] dictionaries;

  protected DataType[] dataTypes;
  /**
   * carbon columns
   */
  protected CarbonColumn[] carbonColumns;

  /**
   * It would be instantiated in side the task so the dictionary would be loaded inside every mapper
   * instead of driver.
   *
   * @param carbonColumns
   * @param absoluteTableIdentifier
   */
  @Override public void initialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.carbonColumns = carbonColumns;
    dictionaries = new Dictionary[carbonColumns.length];
    dataTypes = new DataType[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].hasEncoding(Encoding.DICTIONARY) && !carbonColumns[i]
          .hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        CacheProvider cacheProvider = CacheProvider.getInstance();
        Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider
            .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath());
        try {
          dataTypes[i] = carbonColumns[i].getDataType();
          dictionaries[i] = forwardDictionaryCache.get(new DictionaryColumnUniqueIdentifier(
              absoluteTableIdentifier.getCarbonTableIdentifier(),
              carbonColumns[i].getColumnIdentifier(), dataTypes[i]));
        } catch (CarbonUtilException e) {
          throw new RuntimeException(e);
        }
      } else {
        dataTypes[i] = carbonColumns[i].getDataType();
      }
    }
  }

  /**
   * This method iwll be used to clear the dictionary cache and update access count for each
   * column involved which will be used during eviction of columns from LRU cache if memory
   * reaches threshold
   */
  @Override public void close() {
    for (int i = 0; i < dictionaries.length; i++) {
      CarbonUtil.clearDictionaryCache(dictionaries[i]);
    }
  }
}
