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

import java.text.DateFormat;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.scan.partition.ListPartitioner;
import org.apache.carbondata.core.scan.partition.PartitionUtil;
import org.apache.carbondata.core.scan.partition.RangePartitioner;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class PartitionFilterUtil {

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

    List<List<String>> listInfo = partitionInfo.getListInfo();
    DataType partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();

    SerializableComparator comparator =
        Comparator.getComparator(partitionColumnDataType);

    BitSet partitionMap = PartitionUtil.generateBitSetBySize(partitioner.numPartitions(), false);
    // add default partition
    partitionMap.set(0);
    int partitions = listInfo.size();
    if (isGreaterThan) {
      if (isEqualTo) {
        // GreaterThanEqualTo(>=)
        outer1:
        for (int i = 0; i < partitions; i++) {
          for (String value : listInfo.get(i)) {
            Object listValue = PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType,
                timestampFormatter, dateFormatter);
            if (listValue instanceof String) {
              listValue = ByteUtil.toBytesForPlainValue((String)listValue);
            }
            if (comparator.compare(listValue, filterValue) >= 0) {
              partitionMap.set(i + 1);
              continue outer1;
            }
          }
        }
      } else {
        // GreaterThan(>)
        outer2:
        for (int i = 0; i < partitions; i++) {
          for (String value : listInfo.get(i)) {
            Object listValue = PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType,
                timestampFormatter, dateFormatter);
            if (listValue instanceof String) {
              listValue = ByteUtil.toBytesForPlainValue((String)listValue);
            }
            if (comparator.compare(listValue, filterValue) > 0) {
              partitionMap.set(i + 1);
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
          for (String value : listInfo.get(i)) {
            Object listValue = PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType,
                timestampFormatter, dateFormatter);
            if (listValue instanceof String) {
              listValue = ByteUtil.toBytesForPlainValue((String)listValue);
            }
            if (comparator.compare(listValue, filterValue) <= 0) {
              partitionMap.set(i + 1);
              continue outer3;
            }
          }
        }
      } else {
        // LessThan(<)
        outer4:
        for (int i = 0; i < partitions; i++) {
          for (String value : listInfo.get(i)) {
            Object listValue = PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType,
                timestampFormatter, dateFormatter);
            if (listValue instanceof String) {
              listValue = ByteUtil.toBytesForPlainValue((String)listValue);
            }
            if (comparator.compare(listValue, filterValue) < 0) {
              partitionMap.set(i + 1);
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

    SerializableComparator comparator =
        Comparator.getComparator(partitionColumnDataType);

    BitSet partitionMap = PartitionUtil.generateBitSetBySize(partitioner.numPartitions(), false);

    int numPartitions = values.size();
    int result = 0;
    // the partition index of filter value
    int partitionIndex = 0;
    // find the partition of filter value
    for (; partitionIndex < numPartitions; partitionIndex++) {
      Object value = PartitionUtil.getDataBasedOnDataType(
          values.get(partitionIndex), partitionColumnDataType, timestampFormatter, dateFormatter);
      if (value instanceof String) {
        value = ByteUtil.toBytesForPlainValue((String)value);
      }
      result = comparator.compare(filterValue, value);
      if (result <= 0) {
        break;
      }
    }
    if (partitionIndex == numPartitions) {
      // filter value is in default partition
      if (isGreaterThan) {
        // GreaterThan(>), GreaterThanEqualTo(>=)
        partitionMap.set(0);
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
          partitionMap.set(partitionIndex + 2, partitioner.numPartitions());
          partitionMap.set(0);
        } else {
          if (isEqualTo) {
            // LessThanEqualTo(<=)
            partitionMap.set(1, partitionIndex + 3);
          } else {
            // LessThan(<)
            partitionMap.set(1, partitionIndex + 2);
          }
        }
      } else {
        // the filter value is not a bound value of range partition
        if (isGreaterThan) {
          // GreaterThan(>), GreaterThanEqualTo(>=)
          partitionMap.set(partitionIndex + 1, partitioner.numPartitions());
          partitionMap.set(0);
        } else {
          // LessThan(<), LessThanEqualTo(<=)
          partitionMap.set(1, partitionIndex + 2);
        }
      }
    }
    return partitionMap;
  }

}
