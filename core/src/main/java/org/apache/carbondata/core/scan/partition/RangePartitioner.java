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

  private int partitions;
  private Object[] bounds;
  private Comparator comparator;

  public RangePartitioner(PartitionInfo partitionInfo) {
    List<String> values = partitionInfo.getRangeInfo();
    DataType partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();
    partitions = values.size();
    bounds = new Object[partitions];
    if (partitionColumnDataType == DataType.STRING) {
      for (int i = 0; i < partitions; i++) {
        bounds[i] = ByteUtil.toBytes(values.get(i));
      }
    } else {
      for (int i = 0; i < partitions; i++) {
        bounds[i] = PartitionUtil.getDataBasedOnDataType(values.get(i), partitionColumnDataType);
      }
    }

    switch (partitionColumnDataType) {
      case INT:
        comparator = new IntComparator();
        break;
      case SHORT:
        comparator = new ShortComparator();
        break;
      case DOUBLE:
        comparator = new DoubleComparator();
        break;
      case LONG:
      case DATE:
      case TIMESTAMP:
        comparator = new LongComparator();
        break;
      case DECIMAL:
        comparator = new BigDecimalComparator();
        break;
      default:
        comparator = new ByteArrayComparator();
    }
  }

  @Override public int numPartitions() {
    return partitions + 1;
  }

  @Override public int getPartition(Object key) {
    if (key == null) {
      return partitions;
    } else {
      for (int i = 0; i < partitions; i++) {
        if (comparator.compareTo(key, bounds[i])) {
          return i;
        }
      }
      return partitions;
    }
  }

  interface Comparator extends Serializable {
    boolean compareTo(Object key1, Object key2);
  }

  class ByteArrayComparator implements Comparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return ByteUtil.compare((byte[]) key1, (byte[]) key2) < 0;
    }
  }

  class IntComparator implements Comparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return (int) key1 - (int) key2 < 0;
    }
  }

  class ShortComparator implements Comparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return (short) key1 - (short) key2 < 0;
    }
  }

  class DoubleComparator implements Comparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return (double) key1 - (double) key2 < 0;
    }
  }

  class LongComparator implements Comparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return (long) key1 - (long) key2 < 0;
    }
  }

  class BigDecimalComparator implements Comparator {
    @Override public boolean compareTo(Object key1, Object key2) {
      return ((BigDecimal) key1).compareTo((BigDecimal) key2) < 0;
    }
  }

}
