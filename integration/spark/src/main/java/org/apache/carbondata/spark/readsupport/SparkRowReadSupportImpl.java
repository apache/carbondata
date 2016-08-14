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
package org.apache.carbondata.spark.readsupport;

import java.sql.Timestamp;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.hadoop.readsupport.impl.AbstractDictionaryDecodedReadSupport;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkRowReadSupportImpl extends AbstractDictionaryDecodedReadSupport<Row> {

  @Override public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    super.intialize(carbonColumns, absoluteTableIdentifier);
    //can intialize and generate schema here.
  }

  @Override public Row readRow(Object[] data) {
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = DataTypeUtil
            .getDataBasedOnDataType(dictionaries[i].getDictionaryValueForKey((int) data[i]),
                dataTypes[i]);
        switch (dataTypes[i]) {
          case STRING:
            data[i] = UTF8String.fromString(data[i].toString());
            break;
          case TIMESTAMP:
            data[i] = new Timestamp((long) data[i] / 1000);
            break;
          default:
        }
      } else if (carbonColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        //convert the long to timestamp in case of direct dictionary column
        if (DataType.TIMESTAMP == carbonColumns[i].getDataType()) {
          data[i] = new Timestamp((long) data[i] / 1000);
        }
      }
    }
    return new GenericRow(data);
  }
}
