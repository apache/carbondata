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

/**
 * Hash partitioner implementation
 */
@InterfaceAudience.Internal
public class HashPartitionerImpl implements Partitioner<CarbonRow> {

  private int numberOfBuckets;

  private Hash[] hashes;

  public HashPartitionerImpl(List<Integer> indexes, List<ColumnSchema> columnSchemas,
      int numberOfBuckets) {
    this.numberOfBuckets = numberOfBuckets;
    hashes = new Hash[indexes.size()];
    for (int i = 0; i < indexes.size(); i++) {
      DataType dataType = columnSchemas.get(i).getDataType();
      if (dataType == DataTypes.SHORT || dataType == DataTypes.INT || dataType == DataTypes.LONG) {
        hashes[i] = new IntegralHash(indexes.get(i));
      } else if (dataType == DataTypes.DOUBLE || dataType == DataTypes.FLOAT ||
          DataTypes.isDecimal(dataType)) {
        hashes[i] = new DecimalHash(indexes.get(i));
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
    return (hashCode & Integer.MAX_VALUE) % numberOfBuckets;
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
      return value[index] != null ? Long.valueOf(value[index].toString()).hashCode() : 0;
    }
  }

  private static class DecimalHash implements Hash {

    private int index;

    private DecimalHash(int index) {
      this.index = index;
    }

    public int getHash(Object[] value) {
      return value[index] != null ? Double.valueOf(value[index].toString()).hashCode() : 0;
    }
  }

  private static class StringHash implements Hash {

    private int index;

    private StringHash(int index) {
      this.index = index;
    }

    @Override public int getHash(Object[] value) {
      return value[index] != null ? value[index].hashCode() : 0;
    }
  }
}
