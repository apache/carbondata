/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.unibi.molap.engine.querystats;

import java.io.Serializable;

import org.apache.spark.AccumulatorParam;

/**
 * Accumulator, in spark, is like global variable. It is registered at driver side and updated
 * at executor side. This class does the merging task i.e all executor value it merges to one.
 * @author A00902717
 *
 */
public class PartitionAccumulator implements AccumulatorParam<PartitionDetail>, Serializable
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public PartitionDetail addInPlace(PartitionDetail part1, PartitionDetail part2)
    {

        return mergePartitions(part1, part2);
    }

    @Override
    public PartitionDetail zero(PartitionDetail part)
    {
        return part;
    }

    @Override
    public PartitionDetail addAccumulator(PartitionDetail part1, PartitionDetail part2)
    {
        return mergePartitions(part1, part2);
    }

    private PartitionDetail mergePartitions(PartitionDetail part1, PartitionDetail part2)
    {
        if(null == part1)
        {
            return part2;
        }
        if(null == part2)
        {
            return part1;
        }
        PartitionDetail merged = new PartitionDetail();
        merged.addNumberOfNodesScanned(part1.getNumberOfNodesScanned());
        merged.addNumberOfNodesScanned(part2.getNumberOfNodesScanned());

        merged.addNumberOfRowsScanned(part1.getNoOfRowsScanned());
        merged.addNumberOfRowsScanned(part2.getNoOfRowsScanned());

        return merged;
    }

}
