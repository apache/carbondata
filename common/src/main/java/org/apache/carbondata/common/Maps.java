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

package org.apache.carbondata.common;

import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;

@InterfaceAudience.Developer
public class Maps {

  /**
   * Return value if key is contained in the map, else return defauleValue.
   * This is added to avoid JDK 8 dependency
   */
  public static <K, V> V getOrDefault(Map<K, V> map, K key, V defaultValue) {
    V value = map.get(key);
    if (value != null) {
      return value;
    } else {
      return defaultValue;
    }
  }
}
