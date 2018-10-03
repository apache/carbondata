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
import java.util.BitSet;

import org.apache.spark.sql.types.Decimal;

class ColumnarVectorWrapperDirectWithDeleteDelta extends ColumnarVectorWrapperDirect {

  private BitSet deletedRows;

  private int counter;

  public ColumnarVectorWrapperDirectWithDeleteDelta(ColumnarVectorWrapperDirect vectorWrapper,
      BitSet deletedRows) {
    super(vectorWrapper.carbonVectorProxy, vectorWrapper.ordinal);
    this.deletedRows = deletedRows;
  }

  @Override public void putBoolean(int rowId, boolean value) {
    if (!deletedRows.get(rowId)) {
      sparkColumnVectorProxy.putBoolean(counter++, value, ordinal);
    }
  }

  @Override public void putFloat(int rowId, float value) {
    if (!deletedRows.get(rowId)) {
      sparkColumnVectorProxy.putFloat(counter++, value, ordinal);
    }
  }

  @Override public void putShort(int rowId, short value) {
    if (!deletedRows.get(rowId)) {
      sparkColumnVectorProxy.putShort(counter++, value, ordinal);
    }
  }

  @Override public void putInt(int rowId, int value) {
    if (!deletedRows.get(rowId)) {
      if (isDictionary) {
        sparkColumnVectorProxy.putDictionaryInt(counter++, value, ordinal);
      } else {
        sparkColumnVectorProxy.putInt(counter++, value, ordinal);
      }
    }
  }

  @Override public void putLong(int rowId, long value) {
    if (!deletedRows.get(rowId)) {
      sparkColumnVectorProxy.putLong(counter++, value, ordinal);
    }
  }

  @Override public void putDecimal(int rowId, BigDecimal value, int precision) {
    if (!deletedRows.get(rowId)) {
      Decimal toDecimal = Decimal.apply(value);
      sparkColumnVectorProxy.putDecimal(counter++, toDecimal, precision, ordinal);
    }
  }

  @Override public void putDouble(int rowId, double value) {
    if (!deletedRows.get(rowId)) {
      sparkColumnVectorProxy.putDouble(counter++, value, ordinal);
    }
  }

  @Override public void putBytes(int rowId, byte[] value) {
    if (!deletedRows.get(rowId)) {
      sparkColumnVectorProxy.putByteArray(counter++, value, ordinal);
    }
  }

  @Override public void putBytes(int rowId, int offset, int length, byte[] value) {
    if (!deletedRows.get(rowId)) {
      sparkColumnVectorProxy.putByteArray(counter++, value, offset, length, ordinal);
    }
  }

  @Override public void putByte(int rowId, byte value) {
    if (!deletedRows.get(rowId)) {
      sparkColumnVectorProxy.putByte(counter++, value, ordinal);
    }
  }
}
