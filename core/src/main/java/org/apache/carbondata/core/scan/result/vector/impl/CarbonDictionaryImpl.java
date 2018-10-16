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
package org.apache.carbondata.core.scan.result.vector.impl;

import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;

public class CarbonDictionaryImpl implements CarbonDictionary {

  private byte[][] dictionary;

  private int actualSize;

  private boolean isDictUsed;

  public CarbonDictionaryImpl(byte[][] dictionary, int actualSize) {
    this.dictionary = dictionary;
    this.actualSize = actualSize;
  }

  @Override public int getDictionaryActualSize() {
    return actualSize;
  }

  @Override public int getDictionarySize() {
    return this.dictionary.length;
  }

  @Override public boolean isDictionaryUsed() {
    return this.isDictUsed;
  }

  @Override public void setDictionaryUsed() {
    this.isDictUsed = true;
  }

  @Override public byte[] getDictionaryValue(int index) {
    return dictionary[index];
  }

  @Override public byte[][] getAllDictionaryValues() {
    return dictionary;
  }
}
