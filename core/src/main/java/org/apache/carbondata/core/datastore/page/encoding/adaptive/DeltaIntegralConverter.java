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

package org.apache.carbondata.core.datastore.page.encoding.adaptive;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class DeltaIntegralConverter implements ColumnPageValueConverter {
  private DataType targetDataType;
  private ColumnPage encodedPage;
  private long max;

  public DeltaIntegralConverter(ColumnPage encodedPage, DataType targetDataType,
      DataType srcDataType, Object max) {
    this.targetDataType = targetDataType;
    this.encodedPage = encodedPage;
    switch (srcDataType) {
      case BYTE:
        this.max = (byte) max;
        break;
      case SHORT:
        this.max = (short) max;
        break;
      case INT:
        this.max = (int) max;
        break;
      case LONG:
        this.max = (long) max;
        break;
      case FLOAT:
      case DOUBLE:
        this.max = (long)(max);
        break;
    }
  }

  @Override
  public void encode(int rowId, byte value) {
    switch (targetDataType) {
      case BYTE:
        encodedPage.putByte(rowId, (byte)(max - value));
        break;
      default:
        throw new RuntimeException("internal error");
    }
  }

  @Override
  public void encode(int rowId, short value) {
    switch (targetDataType) {
      case BYTE:
        encodedPage.putByte(rowId, (byte)(max - value));
        break;
      case SHORT:
        encodedPage.putShort(rowId, (short)(max - value));
        break;
      default:
        throw new RuntimeException("internal error");
    }
  }

  @Override
  public void encode(int rowId, int value) {
    switch (targetDataType) {
      case BYTE:
        encodedPage.putByte(rowId, (byte)(max - value));
        break;
      case SHORT:
        encodedPage.putShort(rowId, (short)(max - value));
        break;
      case SHORT_INT:
        encodedPage.putShortInt(rowId, (int)(max - value));
        break;
      case INT:
        encodedPage.putInt(rowId, (int)(max - value));
        break;
      default:
        throw new RuntimeException("internal error");
    }
  }

  @Override
  public void encode(int rowId, long value) {
    switch (targetDataType) {
      case BYTE:
        encodedPage.putByte(rowId, (byte)(max - value));
        break;
      case SHORT:
        encodedPage.putShort(rowId, (short)(max - value));
        break;
      case SHORT_INT:
        encodedPage.putShortInt(rowId, (int)(max - value));
        break;
      case INT:
        encodedPage.putInt(rowId, (int)(max - value));
        break;
      case LONG:
        encodedPage.putLong(rowId, max - value);
        break;
      default:
        throw new RuntimeException("internal error");
    }
  }

  @Override
  public void encode(int rowId, float value) {
    switch (targetDataType) {
      case BYTE:
        encodedPage.putByte(rowId, (byte)(max - value));
        break;
      case SHORT:
        encodedPage.putShort(rowId, (short)(max - value));
        break;
      case SHORT_INT:
        encodedPage.putShortInt(rowId, (int)(max - value));
        break;
      case INT:
        encodedPage.putInt(rowId, (int)(max - value));
        break;
      case LONG:
        encodedPage.putLong(rowId, (long)(max - value));
        break;
      default:
        throw new RuntimeException("internal error");
    }
  }

  @Override
  public void encode(int rowId, double value) {
    switch (targetDataType) {
      case BYTE:
        encodedPage.putByte(rowId, (byte)(max - value));
        break;
      case SHORT:
        encodedPage.putShort(rowId, (short)(max - value));
        break;
      case SHORT_INT:
        encodedPage.putShortInt(rowId, (int)(max - value));
        break;
      case INT:
        encodedPage.putInt(rowId, (int)(max - value));
        break;
      case LONG:
        encodedPage.putLong(rowId, (long)(max - value));
        break;
      default:
        throw new RuntimeException("internal error");
    }
  }

  @Override
  public long decodeLong(byte value) {
    return max - value;
  }

  @Override
  public long decodeLong(short value) {
    return max - value;
  }

  @Override
  public long decodeLong(int value) {
    return max - value;
  }

  @Override
  public double decodeDouble(byte value) {
    return max - value;
  }

  @Override
  public double decodeDouble(short value) {
    return max - value;
  }

  @Override
  public double decodeDouble(int value) {
    return max - value;
  }

  @Override
  public double decodeDouble(long value) {
    return max - value;
  }

  @Override
  public double decodeDouble(float value) {
    // this codec is for integer type only
    throw new RuntimeException("internal error");
  }

  @Override
  public double decodeDouble(double value) {
    // this codec is for integer type only
    throw new RuntimeException("internal error");
  }
}
