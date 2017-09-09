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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
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
import static java.math.RoundingMode.HALF_UP;

/**
 * Reader for DecimalValues
 */
public class DecimalSliceStreamReader  extends AbstractStreamReader {


  private final char[] buffer = new char[100];

  public DecimalSliceStreamReader() {

  }

  /**
   * Create Block for DecimalType
   * @param type
   * @return
   * @throws IOException
   */
  public Block readBlock(Type type)
      throws IOException
  {
    int numberOfRows = 0;
    BlockBuilder builder = null;
    if(isVectorReader) {
      numberOfRows = batchSize;
      builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
      int scale = ((DecimalType)type).getScale();
      int precision = ((DecimalType)type).getPrecision();
      if (columnVector != null) {
        for(int i = 0; i < numberOfRows ; i++ ){
          if(columnVector.isNullAt(i)) {
            builder.appendNull();
          } else {
            Slice slice =
                getSlice(columnVector.getDecimal(i, precision, scale).toJavaBigDecimal(), type);
            if (isShortDecimal(type)) {
              type.writeLong(builder, parseLong((DecimalType) type, slice, 0, slice.length()));
            } else {
              type.writeSlice(builder, parseSlice((DecimalType) type, slice, 0, slice.length()));
            }
          }
        }
      }

    } else {
      if (streamData != null) {
        numberOfRows = streamData.length;
        builder = type.createBlockBuilder(new BlockBuilderStatus(), numberOfRows);
        for(int i = 0; i < numberOfRows ; i++ ){
          Slice slice = getSlice(streamData[i], type);
          if (isShortDecimal(type)) {
            type.writeLong(builder, parseLong((DecimalType) type, slice, 0, slice.length()));
          } else {
            type.writeSlice(builder, parseSlice((DecimalType) type, slice, 0, slice.length()));
          }
        }
      }
    }
    if (builder == null) {
      return null;
    }
    return builder.build();
  }

  /**
   * Function to getSlice from Decimal Object
   * @param value
   * @param type
   * @return
   */
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

  /**
   * Function to parse ShortDecimalType as it is internally treated as Long
   * @param type
   * @param slice
   * @param offset
   * @param length
   * @return
   */
  private long parseLong(DecimalType type, Slice slice, int offset, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, offset, length);
    return decimal.unscaledValue().longValue();
  }

  /**
   * Function for parsing the Slice
   * @param type
   * @param slice
   * @param offset
   * @param length
   * @return
   */
  private Slice parseSlice(DecimalType type, Slice slice, int offset, int length) {
    BigDecimal decimal = parseBigDecimal(type, slice, offset, length);
    return encodeUnscaledValue(decimal.unscaledValue());
  }

  /**
   * Function for parsing the BigDecimal
   * @param type
   * @param slice
   * @param offset
   * @param length
   * @return
   */
  private BigDecimal parseBigDecimal(DecimalType type, Slice slice, int offset, int length) {
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
}
