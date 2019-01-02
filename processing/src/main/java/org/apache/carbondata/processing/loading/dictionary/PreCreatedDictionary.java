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

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.devapi.BiDictionary;
import org.apache.carbondata.core.devapi.DictionaryGenerationException;

public class PreCreatedDictionary implements BiDictionary<Integer, Object> {

  private Dictionary dictionary;

  public PreCreatedDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public Integer getOrGenerateKey(Object value) throws DictionaryGenerationException {
    Integer key = getKey(value);
    if (key == null) {
      throw new UnsupportedOperationException("trying to add new entry in PreCreatedDictionary");
    }
    return key;
  }

  @Override
  public Integer getKey(Object value) {
    return dictionary.getSurrogateKey(value.toString());
  }

  @Override
  public String getValue(Integer key) {
    return dictionary.getDictionaryValueForKey(key);
  }

  @Override
  public int size() {
    return dictionary.getDictionaryChunks().getSize();
  }
}
