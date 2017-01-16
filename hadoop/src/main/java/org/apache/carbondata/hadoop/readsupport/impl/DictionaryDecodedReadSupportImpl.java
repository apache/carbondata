/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.hadoop.readsupport.impl;

import java.math.BigDecimal;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.GenericArrayData;

/**
 * It decodes the dictionary values to actual values.
 */
public class DictionaryDecodedReadSupportImpl
    extends AbstractDictionaryDecodedReadSupport<Object[]> {

  @Override public Object[] readRow(Object[] data) {
    assert(data.length == dictionaries.length);
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = dictionaries[i].getDictionaryValueForKey((int) data[i]);
      }  else if (carbonColumns[i].getDataType().equals(DataType.DECIMAL)) {
        data[i] = Decimal.apply((BigDecimal) data[i]);
      } else if (carbonColumns[i].getDataType().equals(DataType.ARRAY)) {
        data[i] = new GenericArrayData((Object[]) data[i]);
      } else if (carbonColumns[i].getDataType().equals(DataType.STRUCT)) {
        data[i] = new GenericInternalRow((Object[]) data[i]);
      }
    }
    return data;
  }
}
