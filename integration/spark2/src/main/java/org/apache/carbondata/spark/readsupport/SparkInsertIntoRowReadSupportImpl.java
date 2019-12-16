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

package org.apache.carbondata.spark.readsupport;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

/**
 * For carbon to carbon insert into flow, prepare scan results.
 */

public class SparkInsertIntoRowReadSupportImpl implements CarbonReadSupport {
  private List<Integer> timeStampIndexes;

  private List<Integer> directDictionaryIndexes;

  @Override
  public void initialize(CarbonColumn[] carbonColumns, CarbonTable carbonTable) {
    timeStampIndexes = new ArrayList<>();
    directDictionaryIndexes = new ArrayList<>();
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].getDataType() == DataTypes.TIMESTAMP) {
        timeStampIndexes.add(i);
      }
      if (carbonColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        directDictionaryIndexes.add(i);
      }
    }
  }

  @Override
  public InternalRow readRow(Object[] data) {
    for (int index : timeStampIndexes) {
      // need to remove local granularity as spark will add it in the query result
      data[index] = ((long)data[index]) / 1000;
    }
    for (int index : directDictionaryIndexes) {
      // null values must be replaced with direct dictionary null values
      if (data[index] == null) {
        data[index] = CarbonCommonConstants.DIRECT_DICT_VALUE_NULL;
      }
    }
    // TODO: need to handle null values for all the column types
    return new GenericInternalRow(data);
  }

  @Override public void close() {

  }
}
