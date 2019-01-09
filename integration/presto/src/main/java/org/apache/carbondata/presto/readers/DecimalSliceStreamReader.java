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

package org.apache.carbondata.presto.readers;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import static java.math.RoundingMode.HALF_UP;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.util.DataTypeUtil;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;




/**
 * Reader for DecimalValues
 */
public class DecimalSliceStreamReader extends CarbonColumnVectorImpl
    implements PrestoVectorBlockBuilder {

  private final char[] buffer = new char[100];
  protected int batchSize;
  protected Type type;
  protected BlockBuilder builder;
  private Dictionary dictionary;

  public DecimalSliceStreamReader(int batchSize, DataType dataType,
      org.apache.carbondata.core.metadata.datatype.DecimalType decimalDataType,
      Dictionary dictionary) {
    super(batchSize, dataType);
    this.type =
        DecimalType.createDecimalType(decimalDataType.getPrecision(), decimalDataType.getScale());
    this.batchSize = batchSize;
    this.builder = type.createBlockBuilder(null, batchSize);
    this.dictionary = dictionary;
  }

  @Override public Block buildBlock() {
    return builder.build();
  }

  @Override public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override public void putInt(int rowId, int value) {
    DecimalType decimalType = (DecimalType) type;
    Object data = DataTypeUtil.getDataBasedOnDataType(dictionary.getDictionaryValueForKey(value),
        DataTypes.createDecimalType(decimalType.getPrecision(), decimalType.getScale()));
    if (Objects.isNull(data)) {
      builder.appendNull();
    } else {
      decimalBlockWriter((BigDecimal) data);
    }
  }

  @Override public void putDecimal(int rowId, BigDecimal value, int precision) {
    decimalBlockWriter(value);
  }

  @Override public void putDecimals(int rowId, int count, BigDecimal value, int precision) {
    for (int i = 0; i < count; i++) {
      putDecimal(rowId++, value, precision);
    }
  }

  @Override public void putNulls(int rowId, int count) {
    for (int i = 0; i < count; i++) {
      builder.appendNull();
    }
  }

  @Override public void putNull(int rowId) {
    builder.appendNull();
  }

  @Override public void reset() {
    builder = type.createBlockBuilder(null, batchSize);
  }

  private void decimalBlockWriter(BigDecimal value) {
    if (isShortDecimal(type)) {
      long rescaledDecimal = Decimals.rescale(value.unscaledValue().longValue(), value.scale(),
          ((DecimalType) type).getScale());
      type.writeLong(builder, rescaledDecimal);
    } else {
      Slice slice = getSlice(value, type);
      type.writeSlice(builder, parseSlice((DecimalType) type, slice, slice.length()));
    }
  }

  private Slice getSlice(Object value, Type type) {
    if (type instanceof DecimalType) {
      DecimalType actual = (DecimalType) type;
      BigDecimal bigDecimalValue = (BigDecimal) value;
      if (isShortDecimal(type)) {
        return utf8Slice(value.toString());
      } else {
        if (bigDecimalValue.scale() > actual.getScale()) {
          BigInteger unscaledDecimal =
              rescale(bigDecimalValue.unscaledValue(), bigDecimalValue.scale(),
                  bigDecimalValue.scale());
          Slice decimalSlice = Decimals.encodeUnscaledValue(unscaledDecimal);
          return utf8Slice(Decimals.toString(decimalSlice, actual.getScale()));
        } else {
          BigInteger unscaledDecimal =
              rescale(bigDecimalValue.unscaledValue(), bigDecimalValue.scale(), actual.getScale());
          Slice decimalSlice = Decimals.encodeUnscaledValue(unscaledDecimal);
          return utf8Slice(Decimals.toString(decimalSlice, actual.getScale()));
        }
      }
    } else {
      return utf8Slice(value.toString());
    }
  }

  private Slice parseSlice(DecimalType type, Slice slice, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, length);
    return encodeUnscaledValue(decimal.unscaledValue());
  }

  private BigDecimal parseBigDecimal(DecimalType type, Slice slice, int length) {
    int offset = 0;
    checkArgument(length < buffer.length);
    for (int i = 0; i < length; i++) {
      buffer[i] = (char) slice.getByte(offset + i);
    }
    BigDecimal decimal = new BigDecimal(buffer, 0, length);
    checkState(decimal.scale() <= type.getScale(),
        "Read decimal value scale larger than column scale");
    decimal = decimal.setScale(type.getScale(), HALF_UP);
    checkState(decimal.precision() <= type.getPrecision(),
        "Read decimal precision larger than column precision");
    return decimal;
  }

  @Override public void putObject(int rowId, Object value) {
    if (value == null) {
      putNull(rowId);
    } else {
      if (dictionary == null) {
        decimalBlockWriter((BigDecimal) value);
      } else {
        putInt(rowId, (int) value);
      }
    }
  }
}
