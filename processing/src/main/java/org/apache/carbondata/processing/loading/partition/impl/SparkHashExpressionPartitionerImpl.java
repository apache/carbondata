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

package org.apache.carbondata.processing.loading.partition.impl;

import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.loading.partition.Partitioner;

import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Hash partitioner implementation spark_hash_expression which using Murmur3_x86_32 keep the
 * same hash value as spark for given input.
 */
@InterfaceAudience.Internal
public class SparkHashExpressionPartitionerImpl implements Partitioner<CarbonRow> {

  private int numberOfBuckets;

  private Hash[] hashes;

  public SparkHashExpressionPartitionerImpl(List<Integer> indexes, List<ColumnSchema> columnSchemas,
                                            int numberOfBuckets) {
    this.numberOfBuckets = numberOfBuckets;
    hashes = new Hash[indexes.size()];
    for (int i = 0; i < indexes.size(); i++) {
      DataType dataType = columnSchemas.get(i).getDataType();
      if (dataType == DataTypes.LONG || dataType == DataTypes.DOUBLE) {
        hashes[i] = new LongHash(indexes.get(i));
      } else if (dataType == DataTypes.SHORT || dataType == DataTypes.INT
          || dataType == DataTypes.FLOAT || dataType == DataTypes.BOOLEAN
          || dataType == DataTypes.DATE) {
        hashes[i] = new IntegralHash(indexes.get(i));
      } else if (DataTypes.isDecimal(dataType)) {
        hashes[i] = new DecimalHash(indexes.get(i));
      } else if (dataType == DataTypes.TIMESTAMP) {
        hashes[i] = new TimestampHash(indexes.get(i));
      } else {
        hashes[i] = new StringHash(indexes.get(i));
      }
    }
  }

  @Override
  public int getPartition(CarbonRow key) {
    int hashCode = 0;
    for (Hash hash : hashes) {
      hashCode += hash.getHash(key.getData());
    }
    int reminder = hashCode % numberOfBuckets;
    if (reminder < 0) {
      return (reminder + numberOfBuckets) % numberOfBuckets;
    } else {
      return reminder;
    }
  }

  private interface Hash {
    int getHash(Object[] value);
  }

  private static class IntegralHash implements Hash {

    private int index;

    private IntegralHash(int index) {
      this.index = index;
    }

    public int getHash(Object[] value) {
      if (value[index] == null) {
        return 42;
      }
      int intValue = 0;
      if (value[index] instanceof Boolean) {
        boolean boolValue = (boolean) value[index];
        intValue = boolValue ? 1 : 0;
      } else if (value[index] instanceof Float) {
        intValue = Float.floatToIntBits((float) value[index]);
      } else {
        intValue = Integer.parseInt(value[index].toString());
      }
      return Murmur3_x86_32.hashInt(intValue, 42);
    }
  }

  private static class LongHash implements Hash {

    private int index;

    private LongHash(int index) {
      this.index = index;
    }

    public int getHash(Object[] value) {
      if (value[index] == null) {
        return 42;
      }
      long longValue = 0L;
      if (value[index] instanceof java.lang.Double) {
        longValue = Double.doubleToLongBits((double) value[index]);
      } else {
        longValue = Long.parseLong(value[index].toString());
      }
      return Murmur3_x86_32.hashLong(longValue, 42);
    }
  }

  private static class TimestampHash implements Hash {

    private int index;

    private TimestampHash(int index) {
      this.index = index;
    }

    public int getHash(Object[] value) {
      if (value[index] == null) {
        return 42;
      }
      long timeMilSec = (long) value[index];
      long timeMicSec = timeMilSec * 1000;
      return Murmur3_x86_32.hashLong(timeMicSec, 42);
    }
  }

  private static class DecimalHash implements Hash {

    private int index;

    private DecimalHash(int index) {
      this.index = index;
    }

    public int getHash(Object[] value) {
      if (value[index] == null) {
        return 42;
      }
      return Double.valueOf(value[index].toString()).hashCode();
    }
  }

  private static class StringHash implements Hash {

    private int index;

    private StringHash(int index) {
      this.index = index;
    }

    @Override
    public int getHash(Object[] value) {
      // we should use the same hash method as spark, otherwise the same value will hash into diff
      // bucket in carbon/parquet bucket tables the result of join will not correct.
      if (value[index] == null) {
        return 42;
      }
      UTF8String utf8String = UTF8String.fromBytes((byte[]) value[index]);
      return utf8String.hashCode();
    }
  }
}
