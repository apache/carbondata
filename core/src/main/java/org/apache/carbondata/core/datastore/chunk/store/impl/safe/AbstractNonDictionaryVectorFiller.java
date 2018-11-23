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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectWithInvertedIndex;
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
      int numberOfRows, int actualDataLength) {
    if (type == DataTypes.STRING) {
      if (length > DataTypes.SHORT.getSizeInBytes()) {
        return new LongStringVectorFiller(numberOfRows, actualDataLength);
      } else {
        return new StringVectorFiller(numberOfRows, actualDataLength);
      }
    } else if (type == DataTypes.VARCHAR) {
      return new LongStringVectorFiller(numberOfRows, actualDataLength);
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
    } else {
      throw new UnsupportedOperationException("Not supported datatype : " + type);
    }

  }

}

class StringVectorFiller extends AbstractNonDictionaryVectorFiller {

  private int actualDataLength;

  public StringVectorFiller(int numberOfRows, int actualDataLength) {
    super(numberOfRows);
    this.actualDataLength = actualDataLength;
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    int localOffset = 0;
    ByteUtil.UnsafeComparer comparator = ByteUtil.UnsafeComparer.INSTANCE;
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
    vector.putAllByteArray(data, 0, actualDataLength);
  }
}

class LongStringVectorFiller extends AbstractNonDictionaryVectorFiller {

  private int actualDataLength;

  public LongStringVectorFiller(int numberOfRows, int actualDataLength) {
    super(numberOfRows);
    this.actualDataLength = actualDataLength;
  }

  @Override public void fillVector(byte[] data, CarbonColumnVector vector) {
    // start position will be used to store the current data position
    boolean invertedIndex = vector instanceof ColumnarVectorWrapperDirectWithInvertedIndex;
    int localOffset = 0;
    ByteUtil.UnsafeComparer comparator = ByteUtil.UnsafeComparer.INSTANCE;
    if (invertedIndex) {
      for (int i = 0; i < numberOfRows; i++) {
        int length =
            (((data[localOffset] & 0xFF) << 24) | ((data[localOffset + 1] & 0xFF) << 16) | (
                (data[localOffset + 2] & 0xFF) << 8) | (data[localOffset + 3] & 0xFF));
        localOffset += 4;
        if (comparator.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
            CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, localOffset, length)) {
          vector.putNull(i);
        } else {
          vector.putByteArray(i, localOffset, length, data);
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
