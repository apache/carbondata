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

package org.apache.carbondata.core.datastore.chunk.store.impl.safe;

import java.math.BigDecimal;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectWithInvertedIndex;
import org.apache.carbondata.core.scan.result.vector.impl.directread.SequentialFill;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

@InterfaceAudience.Internal
@InterfaceStability.Stable
public abstract class AbstractNonDictionaryVectorFiller {

  protected int numberOfRows;

  public AbstractNonDictionaryVectorFiller(int numberOfRows) {
    this.numberOfRows = numberOfRows;
  }

  public abstract void fillVector(byte[] data, CarbonColumnVector vector);

}

class NonDictionaryVectorFillerFactory {

  public static AbstractNonDictionaryVectorFiller getVectorFiller(int length, DataType type,
      int numberOfRows, int actualDataLength, DataType lengthStoredType, boolean isComplexPrimitive,
      DecimalConverterFactory.DecimalConverter decimalConverter) {
    if (type == DataTypes.STRING) {
      if (length > DataTypes.SHORT.getSizeInBytes()) {
        return new LongStringVectorFiller(numberOfRows, actualDataLength, lengthStoredType,
            isComplexPrimitive);
      } else {
        return new StringVectorFiller(numberOfRows, actualDataLength, lengthStoredType,
            isComplexPrimitive);
      }
    } else if (type == DataTypes.VARCHAR || type == DataTypes.BINARY) {
      return new LongStringVectorFiller(numberOfRows, actualDataLength, lengthStoredType,
          isComplexPrimitive);
    } else if (type == DataTypes.TIMESTAMP) {
      return new TimeStampVectorFiller(numberOfRows);
    } else if (type == DataTypes.BOOLEAN) {
      return new BooleanVectorFiller(numberOfRows);
    } else if (type == DataTypes.SHORT) {
      return new ShortVectorFiller(numberOfRows);
    } else if (type == DataTypes.INT) {
      return new IntVectorFiller(numberOfRows);
    } else if (type == DataTypes.LONG) {
      return new LongVectorFiller(numberOfRows);
    } else if (type == DataTypes.DATE && isComplexPrimitive) {
      return new DateVectorFiller(numberOfRows);
    } else if (DataTypes.isDecimal(type) && isComplexPrimitive) {
      return new DecimalVectorFiller(numberOfRows, lengthStoredType, decimalConverter);
    } else {
      throw new UnsupportedOperationException("Not supported datatype : " + type);
    }

  }

}

class StringVectorFiller extends AbstractNonDictionaryVectorFiller {

  private int actualDataLength;

  private DataType lengthStoredType;

  private boolean isComplexPrimitive;

  public StringVectorFiller(int numberOfRows, int actualDataLength, DataType lengthStoredType,
      boolean isComplexPrimitive) {
    super(numberOfRows);
    this.actualDataLength = actualDataLength;
    this.lengthStoredType = lengthStoredType;
    this.isComplexPrimitive = isComplexPrimitive;
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    boolean addSequential = vector instanceof ColumnarVectorWrapperDirectWithInvertedIndex
        || vector instanceof SequentialFill;

    int localOffset = 0;
    ByteUtil.UnsafeComparer comparator = ByteUtil.UnsafeComparer.INSTANCE;
    // In case of inverted index and sequential fill, add data to vector sequentially instead of
    // adding offsets and data separately.
    if (addSequential) {
      for (int i = 0; i < numberOfRows; i++) {
        if (null != lengthStoredType && lengthStoredType == DataTypes.BYTE) {
          byte length = data[localOffset];
          localOffset += 1;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            if (isComplexPrimitive) {
              byte[] row = new byte[length];
              System.arraycopy(data, localOffset, row, 0, length);
              vector.putObject(i, row);
            } else {
              vector.putByteArray(i, localOffset, length, data);
            }
          }
          localOffset += length;
        } else {
          int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
          localOffset += 2;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            vector.putByteArray(i, localOffset, length, data);
          }
          localOffset += length;
        }
      }
    } else {
      for (int i = 0; i < numberOfRows; i++) {
        if (null != lengthStoredType && lengthStoredType == DataTypes.BYTE) {
          byte length = data[localOffset];
          localOffset += 1;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            vector.putArray(i, localOffset, length);
          }
          localOffset += length;
        } else {
          int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
          localOffset += 2;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            vector.putArray(i, localOffset, length);
          }
          localOffset += length;
        }
      }
      vector.putAllByteArray(data, 0, actualDataLength);
    }
  }
}

class LongStringVectorFiller extends AbstractNonDictionaryVectorFiller {

  private int actualDataLength;

  private DataType lengthStoredType;

  private boolean isComplexPrimitive;

  public LongStringVectorFiller(int numberOfRows, int actualDataLength, DataType lengthStoredType,
      boolean isComplexPrimitive) {
    super(numberOfRows);
    this.actualDataLength = actualDataLength;
    this.lengthStoredType = lengthStoredType;
    this.isComplexPrimitive = isComplexPrimitive;
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    boolean invertedIndex = vector instanceof ColumnarVectorWrapperDirectWithInvertedIndex
        || vector instanceof SequentialFill;
    int localOffset = 0;
    ByteUtil.UnsafeComparer comparator = ByteUtil.UnsafeComparer.INSTANCE;
    if (invertedIndex) {
      if (lengthStoredType != null && lengthStoredType == DataTypes.BYTE) {
        for (int i = 0; i < numberOfRows; i++) {
          int length = data[localOffset];
          localOffset += 1;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            if (isComplexPrimitive) {
              if (length == 0) {
                vector.putNull(i);
                continue;
              }
              byte[] row = new byte[length];
              System.arraycopy(data, localOffset, row, 0, length);
              vector.putObject(i, row);
            } else {
              vector.putByteArray(i, localOffset, length, data);
            }
          }
          localOffset += length;
        }
      } else if (lengthStoredType != null && lengthStoredType == DataTypes.SHORT) {
        for (int i = 0; i < numberOfRows; i++) {
          int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
          localOffset += 2;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            if (isComplexPrimitive) {
              if (length == 0) {
                vector.putNull(i);
                continue;
              }
              byte[] row = new byte[length];
              System.arraycopy(data, localOffset, row, 0, length);
              vector.putObject(i, row);
            } else {
              vector.putByteArray(i, localOffset, length, data);
            }
          }
          localOffset += length;
        }
      } else if (lengthStoredType != null && lengthStoredType == DataTypes.SHORT_INT) {
        for (int i = 0; i < numberOfRows; i++) {
          int length =
              (((data[localOffset] & 0xFF) << 16) | ((data[localOffset + 1] & 0xFF) << 8) | (
                  data[localOffset + 2] & 0xFF));
          localOffset += 3;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            if (isComplexPrimitive) {
              if (length == 0) {
                vector.putNull(i);
                continue;
              }
              byte[] row = new byte[length];
              System.arraycopy(data, localOffset, row, 0, length);
              vector.putObject(i, row);
            } else {
              vector.putByteArray(i, localOffset, length, data);
            }
          }
          localOffset += length;
        }
      } else {
        for (int i = 0; i < numberOfRows; i++) {
          int length =
              (((data[localOffset] & 0xFF) << 24) | ((data[localOffset + 1] & 0xFF) << 16) | (
                  (data[localOffset + 2] & 0xFF) << 8) | (data[localOffset + 3] & 0xFF));
          localOffset += 4;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            if (isComplexPrimitive) {
              if (length == 0) {
                vector.putNull(i);
                continue;
              }
              byte[] row = new byte[length];
              System.arraycopy(data, localOffset, row, 0, length);
              vector.putObject(i, row);
            } else {
              vector.putByteArray(i, localOffset, length, data);
            }
          }
          localOffset += length;
        }
      }
    } else {
      if (lengthStoredType != null && lengthStoredType == DataTypes.BYTE) {
        for (int i = 0; i < numberOfRows; i++) {
          int length = data[localOffset];
          localOffset += 1;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            vector.putArray(i, localOffset, length);
          }
          localOffset += length;
        }
      } else if (lengthStoredType != null && lengthStoredType == DataTypes.SHORT) {
        for (int i = 0; i < numberOfRows; i++) {
          int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
          localOffset += 2;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            vector.putArray(i, localOffset, length);
          }
          localOffset += length;
        }
      } else if (lengthStoredType != null && lengthStoredType == DataTypes.SHORT_INT) {
        for (int i = 0; i < numberOfRows; i++) {
          int length =
              (((data[localOffset] & 0xFF) << 16) | ((data[localOffset + 1] & 0xFF) << 8) | (
                  data[localOffset + 2] & 0xFF));
          localOffset += 3;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            vector.putArray(i, localOffset, length);
          }
          localOffset += length;
        }
      } else {
        for (int i = 0; i < numberOfRows; i++) {
          int length =
              (((data[localOffset] & 0xFF) << 24) | ((data[localOffset + 1] & 0xFF) << 16) | (
                  (data[localOffset + 2] & 0xFF) << 8) | (data[localOffset + 3] & 0xFF));
          localOffset += 4;
          if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
              CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
            vector.putNull(i);
          } else {
            vector.putArray(i, localOffset, length);
          }
          localOffset += length;
        }
      }
      vector.putAllByteArray(data, 0, actualDataLength);
    }
  }
}

class BooleanVectorFiller extends AbstractNonDictionaryVectorFiller {

  public BooleanVectorFiller(int numberOfRows) {
    super(numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    int localOffset = 0;
    for (int i = 0; i < numberOfRows; i++) {
      int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
      localOffset += 2;
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putBoolean(i, ByteUtil.toBoolean(data[localOffset]));
      }
      localOffset += length;
    }
  }
}

class ShortVectorFiller extends AbstractNonDictionaryVectorFiller {

  public ShortVectorFiller(int numberOfRows) {
    super(numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    int localOffset = 0;
    for (int i = 0; i < numberOfRows; i++) {
      int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
      localOffset += 2;
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putShort(i, ByteUtil.toXorShort(data, localOffset, length));
      }
      localOffset += length;
    }
  }
}

class IntVectorFiller extends AbstractNonDictionaryVectorFiller {

  public IntVectorFiller(int numberOfRows) {
    super(numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    int localOffset = 0;
    for (int i = 0; i < numberOfRows; i++) {
      int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
      localOffset += 2;
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putInt(i, ByteUtil.toXorInt(data, localOffset, length));
      }
      localOffset += length;
    }
  }
}

class LongVectorFiller extends AbstractNonDictionaryVectorFiller {

  public LongVectorFiller(int numberOfRows) {
    super(numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    int localOffset = 0;
    for (int i = 0; i < numberOfRows; i++) {
      int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
      localOffset += 2;
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putLong(i, DataTypeUtil
            .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), localOffset,
                length));
      }
      localOffset += length;
    }
  }
}

class TimeStampVectorFiller extends AbstractNonDictionaryVectorFiller {

  public TimeStampVectorFiller(int numberOfRows) {
    super(numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    int localOffset = 0;
    for (int i = 0; i < numberOfRows; i++) {
      int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
      localOffset += 2;
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putLong(i, ByteUtil.toXorLong(data, localOffset, length) * 1000L);
      }
      localOffset += length;
    }
  }
}

class DateVectorFiller extends AbstractNonDictionaryVectorFiller {

  public DateVectorFiller(int numberOfRows) {
    super(numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    int localOffset = 0;
    for (int i = 0; i < numberOfRows; i++) {
      byte length = data[localOffset];
      localOffset += 1;
      if (length == 0) {
        vector.putObject(0, null);
      } else {
        int value = ByteUtil.toXorInt(data, localOffset, length);
        vector.putObject(i, value - DateDirectDictionaryGenerator.cutOffDate);
      }
      localOffset += length;
    }
  }
}

class DecimalVectorFiller extends AbstractNonDictionaryVectorFiller {

  private DataType lengthStoredType;

  private DecimalConverterFactory.DecimalConverter decimalConverter;

  public DecimalVectorFiller(int numberOfRows, DataType lengthStoredType,
      DecimalConverterFactory.DecimalConverter decimalConverter) {
    super(numberOfRows);
    this.lengthStoredType = lengthStoredType;
    this.decimalConverter = decimalConverter;
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    if (decimalConverter instanceof DecimalConverterFactory.DecimalIntConverter) {
      int localOffset = 0;
      int precision;
      int newMeasureScale;
      // complex primitive decimal flow comes as dimension
      precision = ((DecimalType) vector.getType()).getPrecision();
      newMeasureScale = ((DecimalType) vector.getType()).getScale();
      for (int i = 0; i < numberOfRows; i++) {
        if (lengthStoredType == DataTypes.BYTE) {
          int len = data[localOffset];
          localOffset += 1;
          if (len == 0) {
            vector.putNull(i);
            continue;
          }
          BigDecimal value = DataTypeUtil.byteToBigDecimal(data, localOffset, len);
          if (value.scale() < newMeasureScale) {
            value = value.setScale(newMeasureScale);
          }
          vector.putDecimal(i, value, precision);
          localOffset += len;
        }
      }
    }
  }

}

