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

import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

/**
 * Range Partitioner
 */
public class RangePartitioner implements Partitioner {

  private int numPartitions;
  private Object[] bounds;
  private SerializableComparator comparator;

  private SimpleDateFormat timestampFormatter = new SimpleDateFormat(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));

  private SimpleDateFormat dateFormatter = new SimpleDateFormat(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
          CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT));

  public RangePartitioner(PartitionInfo partitionInfo) {
    List<String> values = partitionInfo.getRangeInfo();
    DataType partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();
    numPartitions = values.size();
    bounds = new Object[numPartitions];
    if (partitionColumnDataType == DataType.STRING) {
      for (int i = 0; i < numPartitions; i++) {
        bounds[i] = ByteUtil.toBytesForPlainValue(values.get(i));
      }
    } else {
      for (int i = 0; i < numPartitions; i++) {
        bounds[i] = PartitionUtil.getDataBasedOnDataType(values.get(i), partitionColumnDataType,
            timestampFormatter, dateFormatter);
      }
    }
    comparator = Comparator.getComparator(partitionColumnDataType);
  }

  /**
   * number of partitions
   * add extra default partition
   *
   * @return
   */
  @Override public int numPartitions() {
    return numPartitions + 1;
  }

  @Override public int getPartition(Object key) {
    if (key == null) {
      return 0;
    } else {
      for (int i = 0; i < numPartitions; i++) {
        if (comparator.compare(key, bounds[i]) < 0) {
          return i + 1;
        }
      }
      return 0;
    }
  }
}
