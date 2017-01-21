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

package org.apache.carbondata.core.devapi;

public abstract class GeneratingBiDictionary<K, V> implements BiDictionary<K, V> {

  private DictionaryGenerator<K, V> generator;

  public GeneratingBiDictionary(DictionaryGenerator<K, V> generator) {
    this.generator = generator;
  }

  @Override
  public K getOrGenerateKey(V value) throws DictionaryGenerationException {
    K key = getKey(value);
    if (key != null) {
      return key;
    } else {
      K newKey = generator.generateKey(value);
      assert(newKey != null);
      put(newKey, value);
      return newKey;
    }
  }

  /**
   * put the input key value pair into the dictionary
   * @param key dictionary key
   * @param value dictionary value
   */
  protected abstract void put(K key, V value);

}
