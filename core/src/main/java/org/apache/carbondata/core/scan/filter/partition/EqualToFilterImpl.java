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
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.partition.PartitionUtil;
import org.apache.carbondata.core.scan.partition.Partitioner;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * the implement of EqualTo filter
 */
public class EqualToFilterImpl implements PartitionFilterIntf {

  private EqualToExpression equalTo;
  private PartitionInfo partitionInfo;

  public EqualToFilterImpl(EqualToExpression equalTo, PartitionInfo partitionInfo) {
    this.equalTo = equalTo;
    this.partitionInfo = partitionInfo;
  }

  @Override public BitSet applyFilter(Partitioner partitioner) {
    BitSet partitionMap = PartitionUtil.generateBitSetBySize(partitioner.numPartitions(), false);
    if (equalTo.isNull) {
      partitionMap.set(partitioner.getPartition(null));
    } else {
      LiteralExpression literal = (LiteralExpression) equalTo.getRight();
      Object value = PartitionUtil.getDataBasedOnDataTypeForFilter(
          literal.getLiteralExpValue().toString(),
          partitionInfo.getColumnSchemaList().get(0).getDataType());
      if (PartitionType.RANGE == partitionInfo.getPartitionType() && value instanceof String) {
        value = ByteUtil.toBytesForPlainValue((String)value);
      }
      partitionMap.set(partitioner.getPartition(value));
    }
    return partitionMap;
  }
}
