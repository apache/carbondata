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

import java.util.BitSet;

import org.apache.carbondata.core.scan.partition.PartitionUtil;
import org.apache.carbondata.core.scan.partition.Partitioner;

/**
 * the default implement of partition filter
 */
public class DefaultPartitionFilterImpl implements PartitionFilterIntf {

  /**
   * true: all partitions are required
   * false: no partition is required
   */
  private boolean isContainAll;

  public DefaultPartitionFilterImpl(boolean isContainAll) {
    this.isContainAll = isContainAll;
  }

  @Override public BitSet applyFilter(Partitioner partitioner) {
    return PartitionUtil.generateBitSetBySize(partitioner.numPartitions(), this.isContainAll);
  }
}
