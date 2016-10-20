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
package org.apache.carbondata.processing.newflow.parser.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.newflow.complexobjects.ArrayObject;
import org.apache.carbondata.processing.newflow.parser.ComplexParser;
import org.apache.carbondata.processing.newflow.parser.GenericParser;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

/**
 * It parses the string to @{@link ArrayObject} using delimiter.
 * It is thread safe as the state of class don't change while
 * calling @{@link GenericParser#parse(String)} method
 */
public class ArrayParserImpl implements ComplexParser<ArrayObject> {

  private Pattern pattern;

  private List<GenericParser> children = new ArrayList<>();

  public ArrayParserImpl(String delimiter) {
    pattern = Pattern.compile(CarbonUtil.delimiterConverter(delimiter));
  }

  @Override
  public ArrayObject parse(String data) {
    if (StringUtils.isNotEmpty(data)) {
      String[] split = pattern.split(data, -1);
      if (ArrayUtils.isNotEmpty(split)) {
        Object[] array = new Object[children.size()];
        for (int i = 0; i < children.size(); i++) {
          array[i] = children.get(i).parse(split[i]);
        }
        return new ArrayObject(array);
      }
    }
    return null;
  }

  @Override
  public void addChildren(GenericParser parser) {
    children.add(parser);
  }
}
