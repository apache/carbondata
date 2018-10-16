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

public abstract class AbstractCarbonColumnarVector
    implements CarbonColumnVector, ConvertableVector {

  @Override
  public void putShorts(int rowId, int count, short value) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putDecimals(int rowId, int count, BigDecimal value, int precision) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putBytes(int rowId, int count, byte[] value) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putNulls(int rowId, int count) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putNotNull(int rowId) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putNotNull(int rowId, int count) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public boolean isNull(int rowId) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void putObject(int rowId, Object obj) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public Object getData(int rowId) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public DataType getType() {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public DataType getBlockDataType() {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void setBlockDataType(DataType blockDataType) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void setFilteredRowsExist(boolean filteredRowsExist) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void setDictionary(CarbonDictionary dictionary) {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public boolean hasDictionary() {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public CarbonColumnVector getDictionaryVector() {
    throw new UnsupportedOperationException("Not allowed from here");
  }

  @Override
  public void convert() {
    // Do nothing
  }
}
