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

import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

class ColumnarVectorWrapperDirectWithInvertedIndex extends AbstractCarbonColumnarVector {

  private int[] invertedIndex;

  private CarbonColumnVector columnVector;

  public ColumnarVectorWrapperDirectWithInvertedIndex(CarbonColumnVector columnVector,
      int[] invertedIndex) {
    this.invertedIndex = invertedIndex;
    this.columnVector = columnVector;
  }

  @Override public void putBoolean(int rowId, boolean value) {
    columnVector.putBoolean(invertedIndex[rowId], value);
  }

  @Override public void putFloat(int rowId, float value) {
    columnVector.putFloat(invertedIndex[rowId], value);
  }

  @Override public void putShort(int rowId, short value) {
    columnVector.putShort(invertedIndex[rowId], value);
  }

  @Override public void putInt(int rowId, int value) {
    columnVector.putInt(invertedIndex[rowId], value);
  }

  @Override public void putLong(int rowId, long value) {
    columnVector.putLong(invertedIndex[rowId], value);
  }

  @Override public void putDecimal(int rowId, BigDecimal value, int precision) {
    columnVector.putDecimal(invertedIndex[rowId], value, precision);
  }

  @Override public void putDouble(int rowId, double value) {
    columnVector.putDouble(invertedIndex[rowId], value);
  }

  @Override public void putBytes(int rowId, byte[] value) {
    columnVector.putBytes(invertedIndex[rowId], value);
  }

  @Override public void putBytes(int rowId, int offset, int length, byte[] value) {
    columnVector.putBytes(invertedIndex[rowId], offset, length, value);
  }


  @Override public void putByte(int rowId, byte value) {
    columnVector.putByte(invertedIndex[rowId], value);
  }

  @Override public void putNull(int rowId) {
    columnVector.putNull(invertedIndex[rowId]);
  }
}
