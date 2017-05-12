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

import org.apache.carbondata.core.scan.partition.Partitioner;

/**
 * the implement of OR logical filter
 */
public class OrFilterImpl implements PartitionFilterIntf {

  protected PartitionFilterIntf left;
  protected PartitionFilterIntf right;

  public OrFilterImpl(PartitionFilterIntf left, PartitionFilterIntf right) {
    this.left = left;
    this.right = right;
  }

  @Override public BitSet applyFilter(Partitioner partitioner) {
    BitSet leftBitSet = left.applyFilter(partitioner);
    BitSet rightBitSet = right.applyFilter(partitioner);
    leftBitSet.or(rightBitSet);
    return leftBitSet;
  }
}
