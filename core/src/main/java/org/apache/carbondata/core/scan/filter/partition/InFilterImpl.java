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
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.partition.PartitionUtil;
import org.apache.carbondata.core.scan.partition.Partitioner;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * the implement of In filter
 */
public class InFilterImpl implements PartitionFilterIntf {

  private InExpression in;
  private PartitionInfo partitionInfo;

  public InFilterImpl(InExpression in, PartitionInfo partitionInfo) {
    this.in = in;
    this.partitionInfo = partitionInfo;
  }

  @Override public BitSet applyFilter(Partitioner partitioner) {
    BitSet partitionMap = PartitionUtil.generateBitSetBySize(partitioner.numPartitions(), false);
    ListExpression list = (ListExpression) in.getRight();
    for (Expression expr : list.getChildren()) {
      LiteralExpression literal = (LiteralExpression) expr;
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
