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

package org.apache.carbondata.core.scan.result.vector.impl.directread;

import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.scanner.LazyPageLoad;

public abstract class AbstractCarbonColumnarVector
    implements CarbonColumnVector, ConvertableVector {

  @Override public void putShorts(int rowId, int count, short value) {

  }

  @Override public void putInts(int rowId, int count, int value) {

  }

  @Override public void putLongs(int rowId, int count, long value) {

  }

  @Override public void putDecimals(int rowId, int count, BigDecimal value, int precision) {

  }

  @Override public void putDoubles(int rowId, int count, double value) {

  }

  @Override public void putBytes(int rowId, int count, byte[] value) {

  }

  @Override public void putNulls(int rowId, int count) {

  }

  @Override public void putNotNull(int rowId) {

  }

  @Override public void putNotNull(int rowId, int count) {

  }

  @Override public boolean isNull(int rowId) {
    return false;
  }

  @Override public void putObject(int rowId, Object obj) {

  }

  @Override public Object getData(int rowId) {
    return null;
  }

  @Override public void reset() {

  }

  @Override public DataType getType() {
    return null;
  }

  @Override public DataType getBlockDataType() {
    return null;
  }

  @Override public void setBlockDataType(DataType blockDataType) {

  }

  @Override public void setFilteredRowsExist(boolean filteredRowsExist) {

  }

  @Override public void setDictionary(CarbonDictionary dictionary) {

  }

  @Override public boolean hasDictionary() {
    return false;
  }

  @Override public CarbonColumnVector getDictionaryVector() {
    return null;
  }

  @Override public void convert() {

  }

  @Override public void setLazyPage(LazyPageLoad lazyPage) {

  }
}
