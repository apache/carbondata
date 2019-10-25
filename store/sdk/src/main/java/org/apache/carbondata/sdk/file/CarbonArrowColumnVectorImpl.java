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
package org.apache.carbondata.sdk.file;

import java.math.BigDecimal;
import java.util.BitSet;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.scanner.LazyPageLoader;
import org.apache.carbondata.sdk.file.arrow.ArrowFieldWriter;

public class CarbonArrowColumnVectorImpl implements CarbonColumnVector {

  protected BitSet nullBytes;

  private DataType dataType;

  private DataType blockDataType;

  private int[] lengths;

  private int[] offsets;

  private int batchSize;

  private ArrowFieldWriter arrowFieldWriter;

  private CarbonDictionary carbonDictionary;

  /**
   * True if there is at least one NULL byte set. This is an optimization for the writer, to skip
   * having to clear NULL bits.
   */
  protected boolean anyNullsSet;

  private LazyPageLoader lazyPage;

  private boolean loaded;

  public CarbonArrowColumnVectorImpl(int batchSize, DataType dataType,
      ArrowFieldWriter arrowFieldWriter) {
    this.batchSize = batchSize;
    nullBytes = new BitSet(batchSize);
    this.dataType = dataType;
    this.arrowFieldWriter = arrowFieldWriter;
  }

  @Override public void putBoolean(int rowId, boolean value) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putFloat(int rowId, float value) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putShort(int rowId, short value) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putShorts(int rowId, int count, short value) {
    for (int i = 0; i < count; ++i) {
      arrowFieldWriter.write(value, i + rowId);
    }
  }

  @Override public void putInt(int rowId, int value) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putInts(int rowId, int count, int value) {
    for (int i = 0; i < count; ++i) {
      arrowFieldWriter.write(value, i + rowId);
    }
  }

  @Override public void putLong(int rowId, long value) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putLongs(int rowId, int count, long value) {
    for (int i = 0; i < count; ++i) {
      arrowFieldWriter.write(value, i + rowId);
    }
  }

  @Override public void putDecimal(int rowId, BigDecimal  value, int precision) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putDecimals(int rowId, int count, BigDecimal value, int precision) {
    for (int i = 0; i < count; ++i) {
      arrowFieldWriter.write(value, i + rowId);
    }
  }

  @Override public void putDouble(int rowId, double value) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putDoubles(int rowId, int count, double value) {
    for (int i = 0; i < count; ++i) {
      arrowFieldWriter.write(value, i + rowId);
    }
  }

  @Override public void putByteArray(int rowId, byte[] value) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putByte(int rowId, byte value) {
    arrowFieldWriter.write(value, rowId);
  }

  @Override public void putByteArray(int rowId, int count, byte[] value) {
    for (int i = 0; i < count; ++i) {
      arrowFieldWriter.write(value, i + rowId);
    }
  }

  @Override public void putByteArray(int rowId, int offset, int length, byte[] value) {
    arrowFieldWriter.write(value, rowId, offset, length);
  }

  @Override public void putNull(int rowId) {
    nullBytes.set(rowId);
    anyNullsSet = true;
  }

  @Override public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; ++i) {
      nullBytes.set(rowId + i);
    }
    anyNullsSet = true;
  }

  @Override public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      arrowFieldWriter.write(src[i], rowId++);
    }
  }

  @Override public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      arrowFieldWriter.write(src[i], rowId++);
    }
  }

  @Override public void putInts(int rowId, int count, int[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      arrowFieldWriter.write(src[i], rowId++);
    }
  }

  @Override public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      arrowFieldWriter.write(src[i], rowId++);
    }
  }

  @Override public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      arrowFieldWriter.write(src[i], rowId++);
    }
  }

  @Override public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      arrowFieldWriter.write(src[i], rowId++);
    }
  }

  @Override public void putNotNull(int rowId) {

  }

  @Override public void putNotNull(int rowId, int count) {

  }

  public boolean isNullAt(int rowId) {
    return nullBytes.get(rowId);
  }


  @Override public boolean isNull(int rowId) {
    return nullBytes.get(rowId);
  }

  @Override public void putObject(int rowId, Object obj) {
    arrowFieldWriter.write(obj, rowId);
  }

  @Override public Object getData(int rowId) {
    if (!loaded) {
      loadPage();
    }
    return arrowFieldWriter.getValueVector();
  }

  @Override public void reset() {
    nullBytes.clear();
    loaded = false;

  }

  @Override public DataType getType() {
    return dataType;
  }

  @Override
  public DataType getBlockDataType() {
    return blockDataType;
  }

  @Override
  public void setBlockDataType(DataType blockDataType) {
    this.blockDataType = blockDataType;
  }

  @Override public void setFilteredRowsExist(boolean filteredRowsExist) {

  }

  @Override public void setDictionary(CarbonDictionary dictionary) {
    this.carbonDictionary = dictionary;
  }

  @Override public boolean hasDictionary() {
    return null != this.carbonDictionary;
  }

  @Override public CarbonColumnVector getDictionaryVector() {
    return null;
  }

  /**
   * Returns true if any of the nulls indicator are set for this column. This can be used
   * as an optimization to prevent setting nulls.
   */
  public final boolean anyNullsSet() { return anyNullsSet; }



  @Override public void setLazyPage(LazyPageLoader lazyPage) {
    this.lazyPage = lazyPage;
  }

  public void loadPage() {
    if (lazyPage != null) {
      lazyPage.loadPage();
    }
    loaded = true;
  }

  @Override public void putArray(int rowId, int offset, int length) {
    if (offsets == null) {
      offsets = new int[batchSize];
      lengths = new int[batchSize];
    }
    offsets[rowId] = offset;
    lengths[rowId] = length;
  }

  @Override public void putAllByteArray(byte[] data, int offset, int length) {
    // offset is 0 as need to copy whole byte array
    arrowFieldWriter.write(data, 0, 0, length);
  }


}
