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

import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.partition.ListPartitioner;
import org.apache.carbondata.core.scan.partition.PartitionUtil;
import org.apache.carbondata.core.scan.partition.Partitioner;
import org.apache.carbondata.core.scan.partition.RangePartitioner;

/**
 * the implement of Range filter(include <=, <, >=, >)
 */
public class RangeFilterImpl implements PartitionFilterIntf {

  private LiteralExpression literal;
  private boolean isGreaterThan;
  private boolean isEqualTo;
  private PartitionInfo partitionInfo;

  public RangeFilterImpl(LiteralExpression literal, boolean isGreaterThan, boolean isEqualTo,
      PartitionInfo partitionInfo) {
    this.literal = literal;
    this.isGreaterThan = isGreaterThan;
    this.isEqualTo = isEqualTo;
    this.partitionInfo = partitionInfo;
  }

  @Override public BitSet applyFilter(Partitioner partitioner) {

    switch (partitionInfo.getPartitionType()) {
      case LIST:
        Object filterValueOfList = PartitionUtil.getDataBasedOnDataTypeForFilter(
            literal.getLiteralExpValue().toString(),
            partitionInfo.getColumnSchemaList().get(0).getDataType());
        return PartitionFilterUtil.getPartitionMapForRangeFilter(partitionInfo,
            (ListPartitioner) partitioner, filterValueOfList, isGreaterThan, isEqualTo);
      case RANGE:
        Object filterValueOfRange = PartitionUtil.getDataBasedOnDataTypeForFilter(
            literal.getLiteralExpValue().toString(),
            partitionInfo.getColumnSchemaList().get(0).getDataType());
        return PartitionFilterUtil.getPartitionMapForRangeFilter(partitionInfo,
            (RangePartitioner) partitioner, filterValueOfRange, isGreaterThan, isEqualTo);
      default:
        return PartitionUtil.generateBitSetBySize(partitioner.numPartitions(), true);
    }
  }

}
