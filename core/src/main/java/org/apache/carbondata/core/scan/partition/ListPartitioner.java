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

import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;

/**
 * List Partitioner
 */
public class ListPartitioner implements Partitioner {

  /**
   * map the value of ListPartition to partition id.
   */
  private Map<Object, Integer> map = new java.util.HashMap<Object, Integer>();

  private int numPartitions;

  public ListPartitioner(PartitionInfo partitionInfo) {
    List<List<String>> values = partitionInfo.getListInfo();
    DataType partitionColumnDataType = partitionInfo.getColumnSchemaList().get(0).getDataType();
    numPartitions = values.size();
    for (int i = 0; i < numPartitions; i++) {
      for (String value : values.get(i)) {
        map.put(PartitionUtil.getDataBasedOnDataType(value, partitionColumnDataType), i);
      }
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
    Integer partition = map.get(key);
    if (partition == null) {
      return numPartitions;
    }
    return partition;
  }
}
