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
package org.apache.carbondata.core.stats;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.metadata.schema.partition.AbstractPartition;

public class PartitionStatistic implements Serializable {

  /**
   * total count of partitions
   */
  private int numberOfPartitions;

  /**
   * partition id, increase only
   */
  private int partitionIndex;

  private Map<Integer, AbstractPartition> partitionMap;

  public PartitionStatistic() {
    this.partitionIndex = 0;
    this.numberOfPartitions = 0;
    this.partitionMap = new HashMap<>();
  }

  public void addNewPartition(int id, AbstractPartition partition) {
    partitionMap.put(id, partition);
    partitionIndex ++;
    numberOfPartitions ++;
  }

  public void deletePartition(int id) {
    partitionMap.remove(id);
    numberOfPartitions --;
  }

  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }
}