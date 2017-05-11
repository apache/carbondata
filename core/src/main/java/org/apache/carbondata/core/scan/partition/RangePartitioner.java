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

package org.apache.carbondata.core.scan.partition;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Range Partitioner
 */
public class RangePartitioner implements Partitioner {

  private int numPartitions;
  private Object[] bounds;
  private SerializableComparator comparator;

  public RangePartitioner(PartitionInfo partitionInfo) {
    List<String> values = partitionInfo.getRangeInfo();
    DataType partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();
    numPartitions = values.size();
    bounds = new Object[numPartitions];
    if (partitionColumnDataType == DataType.STRING) {
      for (int i = 0; i < numPartitions; i++) {
        bounds[i] = ByteUtil.toBytes(values.get(i));
      }
    } else {
      for (int i = 0; i < numPartitions; i++) {
        bounds[i] = PartitionUtil.getDataBasedOnDataType(values.get(i), partitionColumnDataType);
      }
    }

    switch (partitionColumnDataType) {
      case INT:
        comparator = new IntSerializableComparator();
        break;
      case SHORT:
        comparator = new ShortSerializableComparator();
        break;
      case DOUBLE:
        comparator = new DoubleSerializableComparator();
        break;
      case LONG:
      case DATE:
      case TIMESTAMP:
        comparator = new LongSerializableComparator();
        break;
      case DECIMAL:
        comparator = new BigDecimalSerializableComparator();
        break;
      default:
        comparator = new ByteArraySerializableComparator();
    }
  }

  /**
   * number of partitions
   * add extra default partition
   * @return
   */
  @Override public int numPartitions() {
    return numPartitions + 1;
  }

  @Override public int getPartition(Object key) {
    if (key == null) {
      return numPartitions;
    } else {
      for (int i = 0; i < numPartitions; i++) {
        if (comparator.compareTo(key, bounds[i])) {
          return i;
        }
      }
      return numPartitions;
    }
  }

  interface SerializableComparator extends Serializable {
    boolean compareTo(Object key1, Object key2);
  }

  class ByteArraySerializableComparator implements SerializableComparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return ByteUtil.compare((byte[]) key1, (byte[]) key2) < 0;
    }
  }

  class IntSerializableComparator implements SerializableComparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return (int) key1 - (int) key2 < 0;
    }
  }

  class ShortSerializableComparator implements SerializableComparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return (short) key1 - (short) key2 < 0;
    }
  }

  class DoubleSerializableComparator implements SerializableComparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return (double) key1 - (double) key2 < 0;
    }
  }

  class LongSerializableComparator implements SerializableComparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return (long) key1 - (long) key2 < 0;
    }
  }

  class BigDecimalSerializableComparator implements SerializableComparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return ((BigDecimal) key1).compareTo((BigDecimal) key2) < 0;
    }
  }

}
