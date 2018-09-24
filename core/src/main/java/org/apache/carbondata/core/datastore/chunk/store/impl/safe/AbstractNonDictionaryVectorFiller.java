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
import java.util.BitSet;

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

  public abstract void fillVector(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer);

  public abstract void fillVectorWithInvertedIndex(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, int[] invertedIndex);

  public abstract void fillVectorWithDeleteDelta(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, BitSet deleteDelta);

  public abstract void fillVectorWithInvertedIndexAndDeleteDelta(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, BitSet deleteDelta, int[] invertedIndex);

  public int getLengthFromBuffer(ByteBuffer buffer) {
    return buffer.getShort();
  }
}

class NonDictionaryVectorFillerFactory {

  public static AbstractNonDictionaryVectorFiller getVectorFiller(DataType type, int lengthSize,
      int numberOfRows) {
    if (type == DataTypes.STRING) {
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

  @Override public void fillVector(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (ByteUtil.UnsafeComparer.INSTANCE.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
          CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
        vector.putNull(i);
      } else {
        vector.putBytes(i, currentOffset, length, data);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (short) (data.length - currentOffset);
    if (ByteUtil.UnsafeComparer.INSTANCE.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
        CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
      vector.putNull(numberOfRows - 1);
    } else {
      vector.putBytes(numberOfRows - 1, currentOffset, length, data);
    }
  }

  @Override
  public void fillVectorWithInvertedIndex(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (ByteUtil.UnsafeComparer.INSTANCE.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
          CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
        vector.putNull(invertedIndex[i]);
      } else {
        vector.putBytes(invertedIndex[i], currentOffset, length, data);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (short) (data.length - currentOffset);
    if (ByteUtil.UnsafeComparer.INSTANCE.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
        CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
      vector.putNull(invertedIndex[numberOfRows - 1]);
    } else {
      vector.putBytes(invertedIndex[numberOfRows - 1], currentOffset, length, data);
    }
  }

  @Override
  public void fillVectorWithDeleteDelta(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, BitSet deleteDelta) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (!deleteDelta.get(i)) {
        if (ByteUtil.UnsafeComparer.INSTANCE
            .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
                CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset,
                length)) {
          vector.putNull(k);
        } else {
          vector.putBytes(k, currentOffset, length, data);
        }
        k++;
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (short) (data.length - currentOffset);
    if (!deleteDelta.get(numberOfRows - 1)) {
      if (ByteUtil.UnsafeComparer.INSTANCE.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
          CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
        vector.putNull(k);
      } else {
        vector.putBytes(k, currentOffset, length, data);
      }
    }
  }

  @Override
  public void fillVectorWithInvertedIndexAndDeleteDelta(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, BitSet deleteDelta, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    Object[] finalData = new Object[numberOfRows];
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (ByteUtil.UnsafeComparer.INSTANCE.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
          CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
        finalData[invertedIndex[i]] = null;
      } else {
        finalData[invertedIndex[i]] = new byte[length];
        System.arraycopy(data, currentOffset, finalData[i], 0, length);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (short) (data.length - currentOffset);
    if (ByteUtil.UnsafeComparer.INSTANCE.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
        CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentOffset, length)) {
      finalData[invertedIndex[numberOfRows - 1]] = null;
    } else {
      finalData[invertedIndex[numberOfRows - 1]] = new byte[length];
      System.arraycopy(data, currentOffset, finalData[invertedIndex[numberOfRows - 1]], 0, length);
    }
    int k = 0;
    for (int i = 0; i < finalData.length; i++) {
      if (!deleteDelta.get(i)) {
        if (finalData[i] == null) {
          vector.putNull(k);
        } else {
          byte[] val = (byte[]) finalData[i];
          vector.putBytes(k, val);
        }
        k++;
      }
    }
  }
}

class LongStringVectorFiller extends StringVectorFiller {
  public LongStringVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override public int getLengthFromBuffer(ByteBuffer buffer) {
    return buffer.getInt();
  }
}

class BooleanVectorFiller extends AbstractNonDictionaryVectorFiller {

  public BooleanVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override public void fillVector(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer) {
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

  @Override
  public void fillVectorWithInvertedIndex(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(invertedIndex[i]);
      } else {
        vector.putBoolean(invertedIndex[i], ByteUtil.toBoolean(data[currentOffset]));
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(invertedIndex[numberOfRows - 1]);
    } else {
      vector.putBoolean(invertedIndex[numberOfRows - 1], ByteUtil.toBoolean(data[currentOffset]));
    }
  }

  @Override
  public void fillVectorWithDeleteDelta(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, BitSet deleteDelta) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (!deleteDelta.get(i)) {
        if (length == 0) {
          vector.putNull(k);
        } else {
          vector.putBoolean(k, ByteUtil.toBoolean(data[currentOffset]));
        }
        k++;
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (!deleteDelta.get(numberOfRows - 1)) {
      if (length == 0) {
        vector.putNull(k);
      } else {
        vector.putBoolean(k, ByteUtil.toBoolean(data[currentOffset]));
      }
    }
  }

  @Override public void fillVectorWithInvertedIndexAndDeleteDelta(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, BitSet deleteDelta, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    Object[] finalData = new Object[numberOfRows];
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        finalData[invertedIndex[i]] = null;
      } else {
        finalData[invertedIndex[i]] = ByteUtil.toBoolean(data[currentOffset]);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      finalData[invertedIndex[numberOfRows - 1]] = null;
    } else {
      finalData[invertedIndex[numberOfRows - 1]] = ByteUtil.toBoolean(data[currentOffset]);
    }
    for (int i = 0; i < finalData.length; i++) {
      if (!deleteDelta.get(i)) {
        if (finalData[i] == null) {
          vector.putNull(k);
        } else {
          vector.putBoolean(k, (Boolean) finalData[i]);
        }
        k++;
      }
    }
  }
}

class ShortVectorFiller extends AbstractNonDictionaryVectorFiller {

  public ShortVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override public void fillVector(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer) {
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

  @Override
  public void fillVectorWithInvertedIndex(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(invertedIndex[i]);
      } else {
        vector.putShort(invertedIndex[i], ByteUtil.toXorShort(data, currentOffset, length));
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(invertedIndex[numberOfRows - 1]);
    } else {
      vector.putShort(invertedIndex[numberOfRows - 1],
          ByteUtil.toXorShort(data, currentOffset, length));
    }
  }

  @Override
  public void fillVectorWithDeleteDelta(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, BitSet deleteDelta) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (!deleteDelta.get(i)) {
        if (length == 0) {
          vector.putNull(k);
        } else {
          vector.putShort(k, ByteUtil.toXorShort(data, currentOffset, length));
        }
        k++;
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (!deleteDelta.get(numberOfRows - 1)) {
      if (length == 0) {
        vector.putNull(k);
      } else {
        vector.putShort(k, ByteUtil.toXorShort(data, currentOffset, length));
      }
    }
  }

  @Override public void fillVectorWithInvertedIndexAndDeleteDelta(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, BitSet deleteDelta, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    Object[] finalData = new Object[numberOfRows];
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        finalData[invertedIndex[i]] = null;
      } else {
        finalData[invertedIndex[i]] = ByteUtil.toXorShort(data, currentOffset, length);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      finalData[invertedIndex[numberOfRows - 1]] = null;
    } else {
      finalData[invertedIndex[numberOfRows - 1]] = ByteUtil.toXorShort(data, currentOffset, length);
    }
    for (int i = 0; i < finalData.length; i++) {
      if (!deleteDelta.get(i)) {
        if (finalData[i] == null) {
          vector.putNull(k);
        } else {
          vector.putShort(k, (Short) finalData[i]);
        }
        k++;
      }
    }
  }
}

class IntVectorFiller extends AbstractNonDictionaryVectorFiller {

  public IntVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override public void fillVector(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer) {
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

  @Override
  public void fillVectorWithInvertedIndex(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(invertedIndex[i]);
      } else {
        vector.putInt(invertedIndex[i], ByteUtil.toXorInt(data, currentOffset, length));
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(invertedIndex[numberOfRows - 1]);
    } else {
      vector
          .putInt(invertedIndex[numberOfRows - 1], ByteUtil.toXorInt(data, currentOffset, length));
    }
  }

  @Override
  public void fillVectorWithDeleteDelta(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, BitSet deleteDelta) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (!deleteDelta.get(i)) {
        if (length == 0) {
          vector.putNull(k);
        } else {
          vector.putInt(k, ByteUtil.toXorInt(data, currentOffset, length));
        }
        k++;
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (!deleteDelta.get(numberOfRows - 1)) {
      if (length == 0) {
        vector.putNull(k);
      } else {
        vector.putInt(k, ByteUtil.toXorInt(data, currentOffset, length));
      }
    }
  }

  @Override public void fillVectorWithInvertedIndexAndDeleteDelta(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, BitSet deleteDelta, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    Object[] finalData = new Object[numberOfRows];
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        finalData[invertedIndex[i]] = null;
      } else {
        finalData[invertedIndex[i]] = ByteUtil.toXorInt(data, currentOffset, length);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      finalData[invertedIndex[numberOfRows - 1]] = null;
    } else {
      finalData[invertedIndex[numberOfRows - 1]] = ByteUtil.toXorInt(data, currentOffset, length);
    }
    for (int i = 0; i < finalData.length; i++) {
      if (!deleteDelta.get(i)) {
        if (finalData[i] == null) {
          vector.putNull(k);
        } else {
          vector.putInt(k, (Integer) finalData[i]);
        }
        k++;
      }
    }
  }
}

class LongVectorFiller extends AbstractNonDictionaryVectorFiller {

  public LongVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override public void fillVector(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer) {
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

  @Override
  public void fillVectorWithInvertedIndex(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(invertedIndex[i]);
      } else {
        vector.putLong(invertedIndex[i], DataTypeUtil
            .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), currentOffset,
                length));
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(invertedIndex[numberOfRows - 1]);
    } else {
      vector.putLong(invertedIndex[numberOfRows - 1], DataTypeUtil
          .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), currentOffset,
              length));
    }
  }

  @Override
  public void fillVectorWithDeleteDelta(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, BitSet deleteDelta) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (!deleteDelta.get(i)) {
        if (length == 0) {
          vector.putNull(k);
        } else {
          vector.putLong(k, DataTypeUtil
              .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), currentOffset,
                  length));
        }
        k++;
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (!deleteDelta.get(numberOfRows - 1)) {
      if (length == 0) {
        vector.putNull(k);
      } else {
        vector.putLong(k, DataTypeUtil
            .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), currentOffset,
                length));
      }
    }
  }

  @Override public void fillVectorWithInvertedIndexAndDeleteDelta(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, BitSet deleteDelta, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    Object[] finalData = new Object[numberOfRows];
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        finalData[invertedIndex[i]] = null;
      } else {
        finalData[invertedIndex[i]] = DataTypeUtil
            .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), currentOffset,
                length);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      finalData[invertedIndex[numberOfRows - 1]] = null;
    } else {
      finalData[invertedIndex[numberOfRows - 1]] = DataTypeUtil
          .getDataBasedOnRestructuredDataType(data, vector.getBlockDataType(), currentOffset,
              length);
    }
    for (int i = 0; i < finalData.length; i++) {
      if (!deleteDelta.get(i)) {
        if (finalData[i] == null) {
          vector.putNull(k);
        } else {
          vector.putLong(k, (Long) finalData[i]);
        }
        k++;
      }
    }
  }
}

class TimeStampVectorFiller extends AbstractNonDictionaryVectorFiller {

  public TimeStampVectorFiller(int lengthSize, int numberOfRows) {
    super(lengthSize, numberOfRows);
  }

  @Override public void fillVector(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer) {
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

  @Override
  public void fillVectorWithInvertedIndex(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        vector.putNull(invertedIndex[i]);
      } else {
        vector.putLong(invertedIndex[i], ByteUtil.toXorLong(data, currentOffset, length) * 1000L);
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      vector.putNull(invertedIndex[numberOfRows - 1]);
    } else {
      vector.putLong(invertedIndex[numberOfRows - 1],
          ByteUtil.toXorLong(data, currentOffset, length) * 1000L);
    }
  }

  @Override
  public void fillVectorWithDeleteDelta(byte[] data, CarbonColumnVector vector,
      ByteBuffer buffer, BitSet deleteDelta) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (!deleteDelta.get(i)) {
        if (length == 0) {
          vector.putNull(k);
        } else {
          vector.putLong(k, ByteUtil.toXorLong(data, currentOffset, length) * 1000L);
        }
        k++;
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (!deleteDelta.get(numberOfRows - 1)) {
      if (length == 0) {
        vector.putNull(k);
      } else {
        vector.putLong(k, ByteUtil.toXorLong(data, currentOffset, length) * 1000L);
      }
    }
  }

  @Override public void fillVectorWithInvertedIndexAndDeleteDelta(byte[] data,
      CarbonColumnVector vector, ByteBuffer buffer, BitSet deleteDelta, int[] invertedIndex) {
    // start position will be used to store the current data position
    int startOffset = 0;
    int currentOffset = lengthSize;
    int k = 0;
    Object[] finalData = new Object[numberOfRows];
    for (int i = 0; i < numberOfRows - 1; i++) {
      buffer.position(startOffset);
      startOffset += getLengthFromBuffer(buffer) + lengthSize;
      int length = startOffset - (currentOffset);
      if (length == 0) {
        finalData[invertedIndex[i]] = null;
      } else {
        finalData[invertedIndex[i]] = ByteUtil.toXorLong(data, currentOffset, length) * 1000L;
      }
      currentOffset = startOffset + lengthSize;
    }
    int length = (data.length - currentOffset);
    if (length == 0) {
      finalData[invertedIndex[numberOfRows - 1]] = null;
    } else {
      finalData[invertedIndex[numberOfRows - 1]] =
          ByteUtil.toXorLong(data, currentOffset, length) * 1000L;
    }
    for (int i = 0; i < finalData.length; i++) {
      if (!deleteDelta.get(i)) {
        if (finalData[i] == null) {
          vector.putNull(k);
        } else {
          vector.putLong(k, (Long) finalData[i]);
        }
        k++;
      }
    }
  }
}
