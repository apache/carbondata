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

package org.apache.carbondata.core.datastore.page;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;

/**
 * Represent a columnar data in one page for one column of decimal data type
 */
public abstract class DecimalColumnPage extends VarLengthColumnPageBase {

  /**
   * decimal converter instance
   */
  DecimalConverterFactory.DecimalConverter decimalConverter;

  DecimalColumnPage(TableSpec.ColumnSpec columnSpec, DataType dataType, int pageSize) {
    super(columnSpec, dataType, pageSize);
    decimalConverter = DecimalConverterFactory.INSTANCE
        .getDecimalConverter(columnSpec.getPrecision(), columnSpec.getScale());
  }

  public DecimalConverterFactory.DecimalConverter getDecimalConverter() {
    return decimalConverter;
  }

  @Override
  public byte[] getBytePage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public short[] getShortPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getShortIntPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public int[] getIntPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public long[] getLongPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public float[] getFloatPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public double[] getDoublePage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[][] getByteArrayPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setFloatPage(float[] floatData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

}
