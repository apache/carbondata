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
package org.apache.carbondata.core.scan.complextypes;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * handles querying map data type columns
 */
public class MapQueryType extends ArrayQueryType {

  public MapQueryType(String name, String parentName, int blockIndex) {
    super(name, parentName, blockIndex);
  }

  /**
   * Map data is internally stored as Array<Struct<key,Value>>. So first the data is filled in the
   * stored format and then each record is separated out to fill key and value separately. This is
   * because for spark integration it expects the data as ArrayBasedMapData(keyArray, valueArray)
   * and for SDK it will be an object array in the same format as returned to spark
   *
   * @param dataBuffer
   * @return
   */
  @Override
  public Object getDataBasedOnDataType(ByteBuffer dataBuffer) {
    Object[] data = fillData(dataBuffer);
    if (data == null) {
      return null;
    }
    Object[] keyArray = new Object[data.length];
    Object[] valueArray = new Object[data.length];
    for (int i = 0; i < data.length; i++) {
      Object[] keyValue = DataTypeUtil.getDataTypeConverter().unwrapGenericRowToObject(data[i]);
      keyArray[i] = keyValue[0];
      valueArray[i] = keyValue[1];
    }
    return DataTypeUtil.getDataTypeConverter().wrapWithArrayBasedMapData(keyArray, valueArray);
  }

}
