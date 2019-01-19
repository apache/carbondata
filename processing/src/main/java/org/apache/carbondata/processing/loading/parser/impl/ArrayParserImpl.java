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

import java.util.regex.Pattern;

import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.complexobjects.ArrayObject;
import org.apache.carbondata.processing.loading.parser.ComplexParser;
import org.apache.carbondata.processing.loading.parser.GenericParser;

import org.apache.commons.lang.ArrayUtils;

/**
 * It parses the string to @{@link ArrayObject} using delimiter.
 * It is thread safe as the state of class don't change while
 * calling @{@link GenericParser#parse(Object)} method
 */
public class ArrayParserImpl implements ComplexParser<ArrayObject> {

  protected Pattern pattern;

  protected GenericParser child;

  protected String nullFormat;

  public ArrayParserImpl(String delimiter, String nullFormat) {
    pattern = Pattern.compile(CarbonUtil.delimiterConverter(delimiter));
    this.nullFormat = nullFormat;
  }

  @Override
  public ArrayObject parse(Object data) {
    if (data != null) {
      String value = data.toString();
      if (!value.isEmpty() && !value.equals(nullFormat)) {
        String[] split = pattern.split(value, -1);
        if (ArrayUtils.isNotEmpty(split)) {
          Object[] array = new Object[split.length];
          for (int i = 0; i < split.length; i++) {
            array[i] = child.parse(split[i]);
          }
          return new ArrayObject(array);
        }
      } else if (value.isEmpty()) {
        Object[] array = new Object[1];
        array[0] = child.parse(value);
        return new ArrayObject(array);
      }
    }
    return null;
  }

  @Override
  public void addChildren(GenericParser parser) {
    child = parser;
  }
}
