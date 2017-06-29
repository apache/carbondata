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

package org.apache.carbondata.core.scan.filter.partition;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.scan.partition.ListPartitioner;
import org.apache.carbondata.core.scan.partition.PartitionUtil;
import org.apache.carbondata.core.scan.partition.RangeIntervalPartitioner;
import org.apache.carbondata.core.scan.partition.RangePartitioner;
import org.apache.carbondata.core.util.ByteUtil;

public class PartitionFilterUtil {

  /**
   * create Comparator for range filter
   * @param dataType
   * @return
   */
  public static Comparator getComparatorByDataType(DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntComparator();
      case SHORT:
        return new ShortComparator();
      case DOUBLE:
        return new DoubleComparator();
      case LONG:
      case DATE:
      case TIMESTAMP:
        return new LongComparator();
      case DECIMAL:
        return new BigDecimalComparator();
      default:
        return new ByteArrayComparator();
    }
  }

  static class ByteArrayComparator implements Comparator<Object> {
    @Override public int compare(Object key1, Object key2) {
      return ByteUtil.compare((byte[]) key1, (byte[]) key2);
    }
  }

  static class IntComparator implements Comparator<Object> {
    @Override public int compare(Object key1, Object key2) {
      return (int) key1 - (int) key2;
    }
  }

  static class ShortComparator implements Comparator<Object> {
    @Override public int compare(Object key1, Object key2) {
      return (short) key1 - (short) key2;
    }
  }

  static class DoubleComparator implements Comparator<Object> {
    @Override public int compare(Object key1, Object key2) {
      double result = (double) key1 - (double) key2;
      if (result < 0) {
        return -1;
      } else if (result > 0) {
        return 1;
      } else {
        return 0;
      }

    }
  }

  static class LongComparator implements Comparator<Object> {
    @Override public int compare(Object key1, Object key2) {
      long result = (long) key1 - (long) key2;
      if (result < 0) {
        return -1;
      } else if (result > 0) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  static class BigDecimalComparator implements Comparator<Object> {
    @Override public int compare(Object key1, Object key2) {
      return ((BigDecimal) key1).compareTo((BigDecimal) key2);
    }
  }

  /**
   * get partition map of range filter on list partition table
   * @param partitionInfo
   * @param partitioner
   * @param filterValue
   * @param isGreaterThan
   * @param isEqualTo
   * @return
   */
  public static BitSet getPartitionMapForRangeFilter(PartitionInfo partitionInfo,
      ListPartitioner partitioner, Object filterValue,  boolean isGreaterThan, boolean isEqualTo,
      DateFormat timestampFormatter, DateFormat dateFormatter) {

    List<List<String>> values = partitionInfo.getListInfo();
    DataType partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();

    Comparator comparator =
        PartitionFilterUtil.getComparatorByDataType(partitionColumnDataType);

    BitSet partitionMap = PartitionUtil.generateBitSetBySize(partitioner.numPartitions(), false);
    // add default partition
    partitionMap.set(partitioner.numPartitions() - 1);

    int partitions = values.size();
    if (isGreaterThan) {
      if (isEqualTo) {
        // GreaterThanEqualTo(>=)
        outer1:
        for (int i = 0; i < partitions; i++) {
          for (String value : values.get(i)) {
            Object listValue = PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType,
                timestampFormatter, dateFormatter);
            if (comparator.compare(listValue, filterValue) >= 0) {
              partitionMap.set(i);
              continue outer1;
            }
          }
        }
      } else {
        // GreaterThan(>)
        outer2:
        for (int i = 0; i < partitions; i++) {
          for (String value : values.get(i)) {
            Object listValue = PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType,
                timestampFormatter, dateFormatter);
            if (comparator.compare(listValue, filterValue) > 0) {
              partitionMap.set(i);
              continue outer2;
            }
          }
        }
      }
    } else {
      if (isEqualTo) {
        // LessThanEqualTo(<=)
        outer3:
        for (int i = 0; i < partitions; i++) {
          for (String value : values.get(i)) {
            Object listValue = PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType,
                timestampFormatter, dateFormatter);
            if (comparator.compare(listValue, filterValue) <= 0) {
              partitionMap.set(i);
              continue outer3;
            }
          }
        }
      } else {
        // LessThanEqualTo(<)
        outer4:
        for (int i = 0; i < partitions; i++) {
          for (String value : values.get(i)) {
            Object listValue = PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType,
                timestampFormatter, dateFormatter);
            if (comparator.compare(listValue, filterValue) < 0) {
              partitionMap.set(i);
              continue outer4;
            }
          }
        }
      }
    }

    return partitionMap;
  }

  /**
   * get partition map of range filter on range partition table
   * @param partitionInfo
   * @param partitioner
   * @param filterValue
   * @param isGreaterThan
   * @param isEqualTo
   * @return
   */
  public static BitSet getPartitionMapForRangeFilter(PartitionInfo partitionInfo,
      RangePartitioner partitioner, Object filterValue, boolean isGreaterThan, boolean isEqualTo,
      DateFormat timestampFormatter, DateFormat dateFormatter) {

    List<String> values = partitionInfo.getRangeInfo();
    DataType partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();

    Comparator comparator =
        PartitionFilterUtil.getComparatorByDataType(partitionColumnDataType);

    BitSet partitionMap = PartitionUtil.generateBitSetBySize(partitioner.numPartitions(), false);

    int numPartitions = values.size();
    int result = 0;
    // the partition index of filter value
    int partitionIndex = 0;
    // find the partition of filter value
    for (; partitionIndex < numPartitions; partitionIndex++) {
      result = comparator.compare(filterValue, PartitionUtil.getDataBasedOnDataType(
          values.get(partitionIndex), partitionColumnDataType, timestampFormatter, dateFormatter));
      if (result <= 0) {
        break;
      }
    }
    if (partitionIndex == numPartitions) {
      // filter value is in default partition
      if (isGreaterThan) {
        // GreaterThan(>), GreaterThanEqualTo(>=)
        partitionMap.set(numPartitions);
      } else {
        // LessThan(<), LessThanEqualTo(<=)
        partitionMap.set(0, partitioner.numPartitions());
      }
    } else {
      // filter value is not in default partition
      if (result == 0) {
        // if result is 0, the filter value is a bound value of range partition.
        if (isGreaterThan) {
          // GreaterThan(>), GreaterThanEqualTo(>=)
          partitionMap.set(partitionIndex + 1, partitioner.numPartitions());
        } else {
          if (isEqualTo) {
            // LessThanEqualTo(<=)
            partitionMap.set(0, partitionIndex + 2);
          } else {
            // LessThan(<)
            partitionMap.set(0, partitionIndex + 1);
          }
        }
      } else {
        // the filter value is not a bound value of range partition
        if (isGreaterThan) {
          // GreaterThan(>), GreaterThanEqualTo(>=)
          partitionMap.set(partitionIndex, partitioner.numPartitions());
        } else {
          // LessThan(<), LessThanEqualTo(<=)
          partitionMap.set(0, partitionIndex + 1);
        }
      }
    }
    return partitionMap;
  }

  /**
   * get partition map of range interval filter on range interval partition table
   * @param partitionInfo
   * @param partitioner
   * @param filterValue
   * @param isGreaterThan
   * @param isEqualTo
   * @return
   */
  public static BitSet getPartitionMapForRangeIntervalFilter(PartitionInfo partitionInfo,
      RangeIntervalPartitioner partitioner, Object filterValue,
      boolean isGreaterThan, boolean isEqualTo,
      DateFormat timestampFormatter, DateFormat dateFormatter) {

    List<String> values = partitionInfo.getRangeIntervalInfo();
    DataType partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();

    Comparator comparator =
        PartitionFilterUtil.getComparatorByDataType(partitionColumnDataType);

    BitSet partitionMap = PartitionUtil
        .generateBitSetBySize(partitioner.numPartitions(), false);

    int numPartitions = values.size() - 1;
    int result = 0;
    // the partition index of filter value
    int partitionIndex = 0;
    // find the partition of filter value
    for (; partitionIndex < numPartitions; partitionIndex++) {
      result = comparator.compare(filterValue, PartitionUtil.getDataBasedOnDataType(
          values.get(partitionIndex), partitionColumnDataType, timestampFormatter, dateFormatter));
      if (result <= 0) {
        break;
      }
    }
    if (partitionIndex == numPartitions) {
      // filter value is not in given partition, should get partitionId dynamically
      int partitionId = partitioner.getPartition(filterValue);
      partitionMap.set(0);
      if (isGreaterThan) {
        // GreaterThan(>), GreaterThanEqualTo(>=)
        partitionMap.set(partitionId, partitioner.numPartitions());
      } else {
        // LessThan(<), LessThanEqualTo(<=)
        partitionMap.set(1, partitionId + 1);
      }
    } else {
      // filter value is in given partition and is a bound of range interval partition
      if (result == 0) {
        // if result is 0, the filter value is a bound value of range partition.
        if (isGreaterThan) {
          // GreaterThan(>), GreaterThanEqualTo(>=)
          partitionMap.set(partitionIndex, partitioner.numPartitions());
          partitionMap.set(0);
        } else {
          if (isEqualTo) {
            // LessThanEqualTo(<=)
            partitionMap.set(1, partitionIndex + 1);
          } else {
            // LessThan(<)
            partitionMap.set(1, partitionIndex);
          }
        }
      } else {
        // the filter value is in given parition and is not a bound value of range partition
        if (isGreaterThan) {
          // GreaterThan(>), GreaterThanEqualTo(>=)
          partitionMap.set(partitionIndex, partitioner.numPartitions());
          partitionMap.set(0);
        } else {
          // LessThan(<), LessThanEqualTo(<=)
          partitionMap.set(1, partitionIndex + 1);
        }
      }
    }
    return partitionMap;
  }
}
