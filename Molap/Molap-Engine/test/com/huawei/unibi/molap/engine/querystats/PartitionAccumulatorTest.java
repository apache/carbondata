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

import org.junit.Test;

public class PartitionAccumulatorTest
{
    @Test
    public void testAddInPlace()
    {
        PartitionAccumulator pac=new PartitionAccumulator();
        PartitionDetail partDetail1=createPartitionDetail();
        PartitionDetail partDetail2=createPartitionDetail();
        pac.addInPlace(partDetail1, partDetail2);
    }
    @Test
    public void testAddAccumulator()
    {
        PartitionAccumulator pac=new PartitionAccumulator();
        pac.zero(null);
        PartitionDetail partDetail1=createPartitionDetail();
        PartitionDetail partDetail2=createPartitionDetail();
        pac.addAccumulator(partDetail1, partDetail2);
    }
    @Test
    public void testAddAccumulator_nulldata()
    {
        PartitionAccumulator pac=new PartitionAccumulator();
        pac.zero(null);
        PartitionDetail partDetail1=createPartitionDetail();
        PartitionDetail partDetail2=createPartitionDetail();
        pac.addAccumulator(null, partDetail2);
        pac.addAccumulator(partDetail1, null);
    }
    
    
    private PartitionDetail createPartitionDetail()
    {
        PartitionDetail partDetail=new PartitionDetail();
        partDetail.addNumberOfNodesScanned(2332);
        partDetail.addNumberOfRowsScanned(323);
        
        return partDetail;
    }

}
