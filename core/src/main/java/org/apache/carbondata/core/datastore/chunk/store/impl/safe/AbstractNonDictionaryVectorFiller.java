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

import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

public abstract class AbstractNonDictionaryVectorFiller {

  protected int lengthSize;
  protected int numberOfRows;

  public AbstractNonDictionaryVectorFiller(int lengthSize, int numberOfRows) {
    this.lengthSize = lengthSize;
    this.numberOfRows = numberOfRows;
  }

  public abstract void fillVector(byte[] data, CarbonColumnVector vector, ByteBuffer buffer);

  public int getLengthFromBuffer(ByteBuffer buffer) {
    return buffer.getShort();
  }
}

class NonDictionaryVectorFillerFactory {

  public static AbstractNonDictionaryVectorFiller getVectorFiller(DataType type, int lengthSize,
      int numberOfRows) {
    if (type == DataTypes.STRING || type == DataTypes.VARCHAR) {
      if (lengthSize == 2) {
        return new StringVectorFiller(lengthSize, numberOfRows);
      } else {
        return new LongStringVectorFiller(lengthSize, numberOfRows);
      }
    } else if (type == DataTypes.TIMESTAMP) {
      return new TimeStampVectorFiller(lengthSize, numberOfRows);
    } else if (type == DataTypes.BOOLEAN) {
      return new BooleanVectorFiller(lengthSize, numberOfRows);
    } else if (type == DataTypes.SHORT) {
      return new ShortVectorFiller(lengthSize, numberOfRows);
    } else if (type == DataTypes.INT) {
      return new IntVectorFiller(lengthSize, numberOfRows);
    } else if (type == DataTypes.LONG) {
      return new LongStringVectorFiller(lengthSize, numberOfRows);
    }
    return new StringVectorFiller(lengthSize, numberOfRows);
  }

}

class StringVectorFiller extends AbstractNonDictionaryVectorFiller {

  public StringVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector, ByteBuffer buffer) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    ByteUtil.UnsafeComparer comparer = ByteUtil.UnsafeComparer.INSTANCE;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (comparer.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
          CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
        vector.putNull(i);
      } else {
        vector.putByteArray(i, currentOffset, length, data);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (comparer.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
        CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
      vector.putNull(numberOfRows - 1);
    } else {
      vector.putByteArray(numberOfRows - 1, currentOffset, length, data);
    }
  }
}

class LongStringVectorFiller extends StringVectorFiller {
  public LongStringVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override
  public int getLengthFromBuffer(ByteBuffer buffer) {
    return buffer.getInt();
  }
}

class BooleanVectorFiller extends AbstractNonDictionaryVectorFiller {

  public BooleanVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector, ByteBuffer buffer) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putBoolean(i, ByteUtil.toBoolean(data[currentOffset]));
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(numberOfRows - 1);
    } else {
      vector.putBoolean(numberOfRows - 1, ByteUtil.toBoolean(data[currentOffset]));
    }
  }
}

class ShortVectorFiller extends AbstractNonDictionaryVectorFiller {

  public ShortVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector, ByteBuffer buffer) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putShort(i, ByteUtil.toXorShort(data, currentOffset, length));
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(numberOfRows - 1);
    } else {
      vector.putShort(numberOfRows - 1, ByteUtil.toXorShort(data, currentOffset, length));
    }
  }
}

class IntVectorFiller extends AbstractNonDictionaryVectorFiller {

  public IntVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector, ByteBuffer buffer) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putInt(i, ByteUtil.toXorInt(data, currentOffset, length));
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(numberOfRows - 1);
    } else {
      vector.putInt(numberOfRows - 1, ByteUtil.toXorInt(data, currentOffset, length));
    }
  }
}

class LongVectorFiller extends AbstractNonDictionaryVectorFiller {

  public LongVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector, ByteBuffer buffer) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putLong(i, DataTypeUtil
            .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), currentOffset,
                length));
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(numberOfRows - 1);
    } else {
      vector.putLong(numberOfRows - 1, DataTypeUtil
          .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), currentOffset,
              length));
    }
  }
}

class TimeStampVectorFiller extends AbstractNonDictionaryVectorFiller {

  public TimeStampVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override
  public void fillVector(byte[] data, CarbonColumnVector vector, ByteBuffer buffer) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(i);
      } else {
        vector.putLong(i, ByteUtil.toXorLong(data, currentOffset, length) * 1000L);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(numberOfRows - 1);
    } else {
      vector.putLong(numberOfRows - 1, ByteUtil.toXorLong(data, currentOffset, length) * 1000L);
    }
  }
}
