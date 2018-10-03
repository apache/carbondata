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

package org.apache.carbondata.spark.vectorreader.directread;

import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.spark.vectorreader.ColumnarVectorWrapperDirect;

import org.apache.spark.sql.CarbonVectorProxy;
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil;
import org.apache.spark.sql.types.Decimal;

class ColumnarVectorWrapperDirectWithInvertedIndex extends ColumnarVectorWrapperDirect {

  private int[] invertedIndex;

  public ColumnarVectorWrapperDirectWithInvertedIndex(ColumnarVectorWrapperDirect vectorWrapper,
      int[] invertedIndex) {
    super(vectorWrapper.carbonVectorProxy, vectorWrapper.ordinal);
    this.invertedIndex = invertedIndex;
  }

  @Override public void putBoolean(int rowId, boolean value) {
    sparkColumnVectorProxy.putBoolean(invertedIndex[rowId], value, ordinal);
  }

  @Override public void putFloat(int rowId, float value) {
    sparkColumnVectorProxy.putFloat(invertedIndex[rowId], value, ordinal);
  }

  @Override public void putShort(int rowId, short value) {
    sparkColumnVectorProxy.putShort(invertedIndex[rowId], value, ordinal);
  }

  @Override public void putInt(int rowId, int value) {
    if (isDictionary) {
      sparkColumnVectorProxy.putDictionaryInt(invertedIndex[rowId], value, ordinal);
    } else {
      sparkColumnVectorProxy.putInt(invertedIndex[rowId], value, ordinal);
    }
  }

  @Override public void putLong(int rowId, long value) {
    sparkColumnVectorProxy.putLong(invertedIndex[rowId], value, ordinal);
  }

  @Override public void putDecimal(int rowId, BigDecimal value, int precision) {
    Decimal toDecimal = Decimal.apply(value);
    sparkColumnVectorProxy.putDecimal(invertedIndex[rowId], toDecimal, precision, ordinal);
  }

  @Override public void putDouble(int rowId, double value) {
    sparkColumnVectorProxy.putDouble(invertedIndex[rowId], value, ordinal);
  }

  @Override public void putBytes(int rowId, byte[] value) {
    sparkColumnVectorProxy.putByteArray(invertedIndex[rowId], value, ordinal);
  }

  @Override public void putBytes(int rowId, int offset, int length, byte[] value) {
    sparkColumnVectorProxy.putByteArray(invertedIndex[rowId], value, offset, length, ordinal);
  }


  @Override public void putByte(int rowId, byte value) {
    sparkColumnVectorProxy.putByte(invertedIndex[rowId], value, ordinal);
  }
}
