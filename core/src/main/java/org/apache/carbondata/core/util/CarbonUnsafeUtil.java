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

package org.apache.carbondata.core.util;

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

public class CarbonUnsafeUtil {

  /**
   * Put the data to unsafe memory
   *
   * @param dataType
   * @param data
   * @param baseObject
   * @param address
   * @param size
   * @param sizeInBytes
   */
  public static void putDataToUnsafe(DataType dataType, Object data, Object baseObject,
      long address, int size, int sizeInBytes) {
    dataType = DataTypeUtil.valueOf(dataType.getName());
    if (dataType == DataTypes.BOOLEAN) {
      CarbonUnsafe.getUnsafe().putBoolean(baseObject, address + size, (boolean) data);
    } else if (dataType == DataTypes.BYTE) {
      CarbonUnsafe.getUnsafe().putByte(baseObject, address + size, (byte) data);
    } else if (dataType == DataTypes.SHORT) {
      CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, (short) data);
    } else if (dataType == DataTypes.INT) {
      CarbonUnsafe.getUnsafe().putInt(baseObject, address + size, (int) data);
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      CarbonUnsafe.getUnsafe().putLong(baseObject, address + size, (long) data);
    } else if (DataTypes.isDecimal(dataType) || dataType == DataTypes.DOUBLE) {
      CarbonUnsafe.getUnsafe().putDouble(baseObject, address + size, (double) data);
    } else if (dataType == DataTypes.FLOAT) {
      CarbonUnsafe.getUnsafe().putFloat(baseObject, address + size, (float) data);
    } else if (dataType == DataTypes.BYTE_ARRAY) {
      CarbonUnsafe.getUnsafe()
          .copyMemory(data, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject, address + size,
              sizeInBytes);
    }
  }

  /**
   * Retrieve/Get the data from unsafe memory
   *
   * @param dataType
   * @param baseObject
   * @param address
   * @param size
   * @param sizeInBytes
   * @return
   */
  public static Object getDataFromUnsafe(DataType dataType, Object baseObject, long address,
      int size, int sizeInBytes) {
    dataType = DataTypeUtil.valueOf(dataType.getName());
    Object data = new Object();
    if (dataType == DataTypes.BOOLEAN) {
      data = CarbonUnsafe.getUnsafe().getBoolean(baseObject, address + size);
    } else if (dataType == DataTypes.BYTE) {
      data = CarbonUnsafe.getUnsafe().getByte(baseObject, address + size);
    } else if (dataType == DataTypes.SHORT) {
      data = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
    } else if (dataType == DataTypes.INT) {
      data = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      data = CarbonUnsafe.getUnsafe().getLong(baseObject, address + size);
    } else if (DataTypes.isDecimal(dataType) || dataType == DataTypes.DOUBLE) {
      data = CarbonUnsafe.getUnsafe().getDouble(baseObject, address + size);
    } else if (dataType == DataTypes.FLOAT) {
      data = CarbonUnsafe.getUnsafe().getFloat(baseObject, address + size);
    } else if (dataType == DataTypes.BYTE_ARRAY) {
      CarbonUnsafe.getUnsafe()
          .copyMemory(baseObject, address + size, data, CarbonUnsafe.BYTE_ARRAY_OFFSET,
              sizeInBytes);
    }
    return data;
  }
}
