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

public interface BiDictionary<K, V> {

  /**
   * Get the dictionary key corresponding to the input value, generate a new key if value is
   * not exist. The new key value pair will be added to this dictionary
   * @param value dictionary value
   * @return dictionary key
   */
  K getOrGenerateKey(V value);

  /**
   * Get the dictionary key corresponding to the input value, return null if value is not exist in
   * the dictionary.
   * @param value dictionary value
   * @return dictionary key
   */
  K getKey(V value);

  /**
   * Get dictionary value corresponding to the input key, return null if key is not exist in the
   * dictionary.
   * @param key dictionary key
   * @return dictionary value
   */
  V getValue(K key);

  /**
   * Return the size of the dictionary
   * @return size
   */
  int size();
}
