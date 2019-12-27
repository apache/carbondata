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

package org.apache.carbondata.core.util.comparator;

public class ShortSerializableComparator implements SerializableComparator {
  @Override
  public int compare(Object key1, Object key2) {
    if (key1 == null && key2 == null) {
      return 0;
    } else if (key1 == null) {
      return -1;
    } else if (key2 == null) {
      return 1;
    }
    if ((short) key1 < (short) key2) {
      return -1;
    } else if ((short) key1 > (short) key2) {
      return 1;
    } else {
      return 0;
    }
  }
}
