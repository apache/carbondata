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

import java.util.Arrays;
import java.util.BitSet;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class CarbonColumnVectorImpl implements CarbonColumnVector {

  private Object[] data;

  private int[] ints;

  private long[] longs;

  private Decimal[] decimals;

  private byte[][] bytes;

  private double[] doubles;

  private BitSet nullBytes;

  private DataType dataType;

  public CarbonColumnVectorImpl(int batchSize, DataType dataType) {
    nullBytes = new BitSet(batchSize);
    this.dataType = dataType;
    switch (dataType) {
      case INT:
        ints = new int[batchSize];
        break;
      case LONG:
        longs = new long[batchSize];
        break;
      case DOUBLE:
        doubles = new double[batchSize];
        break;
      case STRING:
        bytes = new byte[batchSize][];
        break;
      case DECIMAL:
        decimals = new Decimal[batchSize];
        break;
      default:
        data = new Object[batchSize];
    }
  }

  @Override public void putShort(int rowId, short value) {

  }

  @Override public void putInt(int rowId, int value) {
    ints[rowId] = value;
  }

  @Override public void putLong(int rowId, long value) {
    longs[rowId] = value;
  }

  @Override public void putDecimal(int rowId, Decimal value, int precision) {
    decimals[rowId] = value;
  }

  @Override public void putDouble(int rowId, double value) {
    doubles[rowId] = value;
  }

  @Override public void putBytes(int rowId, byte[] value) {
    bytes[rowId] = value;
  }

  @Override public void putBytes(int rowId, int offset, int length, byte[] value) {

  }

  @Override public void putNull(int rowId) {
    nullBytes.set(rowId);
  }

  @Override public boolean isNull(int rowId) {
    return nullBytes.get(rowId);
  }

  @Override public void putObject(int rowId, Object obj) {
    data[rowId] = obj;
  }

  @Override public Object getData(int rowId) {
    if (nullBytes.get(rowId)) {
      return null;
    }
    switch (dataType) {
      case INT:
        return ints[rowId];
      case LONG:
        return longs[rowId];
      case DOUBLE:
        return doubles[rowId];
      case STRING:
        return UTF8String.fromBytes(bytes[rowId]);
      case DECIMAL:
        return decimals[rowId];
      default:
        return data[rowId];
    }
  }

  @Override public void reset() {
    nullBytes.clear();
    switch (dataType) {
      case INT:
        Arrays.fill(ints, 0);
        break;
      case LONG:
        Arrays.fill(longs, 0);
        break;
      case DOUBLE:
        Arrays.fill(doubles, 0);
        break;
      case STRING:
        Arrays.fill(bytes, null);
        break;
      case DECIMAL:
        Arrays.fill(decimals, null);
        break;
      default:
        Arrays.fill(data, null);
    }
  }
}
