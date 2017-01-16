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

<<<<<<< HEAD
import java.math.BigDecimal;
import java.sql.Timestamp;
=======
import java.io.IOException;
>>>>>>> bc5a061e9fac489f997cfd68238622e348512d6f

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.impl.AbstractDictionaryDecodedReadSupport;

<<<<<<< HEAD
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
=======
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
>>>>>>> bc5a061e9fac489f997cfd68238622e348512d6f

public class SparkRowReadSupportImpl extends AbstractDictionaryDecodedReadSupport<InternalRow> {

  @Override public void initialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException {
    super.initialize(carbonColumns, absoluteTableIdentifier);
    //can initialize and generate schema here.
  }

  @Override public InternalRow readRow(Object[] data) {
    for (int i = 0; i < dictionaries.length; i++) {
      if (data[i] == null) {
        continue;
      }
      if (dictionaries[i] == null) {
        if(dataTypes[i].equals(DataType.INT)) {
          data[i] = ((Long)(data[i])).intValue();
        } else if(dataTypes[i].equals(DataType.SHORT)) {
          data[i] = ((Long)(data[i])).shortValue();
        }
      } else if (carbonColumns[i].getDataType().equals(DataType.DECIMAL)) {
        data[i] = Decimal.apply((BigDecimal) data[i]);
      } else if (carbonColumns[i].getDataType().equals(DataType.ARRAY)) {
        data[i] = new GenericArrayData(data[i]);
      } else if (carbonColumns[i].getDataType().equals(DataType.STRUCT)) {
        data[i] = new GenericInternalRow((Object[]) data[i]);
      }
    }
    return new GenericInternalRow(data);
  }
}
