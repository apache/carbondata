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
package org.apache.carbondata.processing.loading.dictionary;

import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.dictionary.client.DictionaryClient;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessage;
import org.apache.carbondata.core.dictionary.generator.key.DictionaryMessageType;

/**
 * Dictionary implementation along with dictionary server client to get new dictionary values
 */
public class DictionaryServerClientDictionary implements BiDictionary<Integer, Object> {

  private Dictionary dictionary;

  private DictionaryClient client;

  private Map<Object, Integer> localCache;

  private DictionaryMessage dictionaryMessage;

  private int base;

  public DictionaryServerClientDictionary(Dictionary dictionary, DictionaryClient client,
      DictionaryMessage key, Map<Object, Integer> localCache) {
    this.dictionary = dictionary;
    this.client = client;
    this.dictionaryMessage = key;
    this.localCache = localCache;
    this.base = (dictionary == null ? 0 : dictionary.getDictionaryChunks().getSize() - 1);
  }

  @Override public Integer getOrGenerateKey(Object value) throws DictionaryGenerationException {
    Integer key = getKey(value);
    if (key == null) {
      dictionaryMessage.setData(value.toString());
      DictionaryMessage dictionaryValue = client.getDictionary(dictionaryMessage);
      key = dictionaryValue.getDictionaryValue();
      synchronized (localCache) {
        localCache.put(value, key);
      }
      return key + base;
    }
    return key;
  }

  @Override public Integer getKey(Object value) {
    Integer key = -1;
    if (dictionary != null) {
      key = dictionary.getSurrogateKey(value.toString());
    }
    if (key == CarbonCommonConstants.INVALID_SURROGATE_KEY) {
      key = localCache.get(value);
      if (key != null) {
        return key + base;
      }
    }
    return key;
  }

  @Override public Object getValue(Integer key) {
    throw new UnsupportedOperationException("Not supported here");
  }

  @Override public int size() {
    dictionaryMessage.setType(DictionaryMessageType.SIZE);
    return client.getDictionary(dictionaryMessage).getDictionaryValue() + base;
  }
}
