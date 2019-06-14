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
package org.apache.carbondata.processing.loading.parser.impl;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.processing.loading.complexobjects.ArrayObject;

import org.apache.commons.lang.ArrayUtils;


public class MapParserImpl extends ArrayParserImpl {

  private String keyValueDelimiter;

  public MapParserImpl(String delimiter, String nullFormat, String keyValueDelimiter) {
    super(delimiter, nullFormat);
    this.keyValueDelimiter = keyValueDelimiter;
  }

  //The Key for Map will always be a PRIMITIVE type so Set<Object> here will work fine
  //The last occurance of the key, value pair will be added and all others will be overwritten
  @Override public ArrayObject parse(Object data) {
    if (data != null) {
      String value = data.toString();
      if (!value.isEmpty() && !value.equals(nullFormat)) {
        String[] split = pattern.split(value, -1);
        if (ArrayUtils.isNotEmpty(split)) {
          ArrayList<Object> array = new ArrayList<>();
          Map<Object, String> map = new HashMap<>();
          for (int i = 0; i < split.length; i++) {
            Object currKey = split[i].split(keyValueDelimiter)[0];
            map.put(currKey, split[i]);
          }
          for (Map.Entry<Object, String> entry : map.entrySet()) {
            array.add(child.parse(entry.getValue()));
          }
          return new ArrayObject(array.toArray());
        }
      }
    }
    return null;
  }
}
