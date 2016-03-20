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

package com.huawei.unibi.molap.engine.executer;

import java.util.Map;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

/**
 * 
 * Class Description : This class executes the query for slice and return the
 * result map.
 *  
 * Version 1.0
 */
public interface ParallelSliceExecutor
{
    /**
     * Execute the slice based on ranges
     * 
     * @param ranges
     * @return the resultMap
     * @throws Exception
     * 
     */
    Map<ByteArrayWrapper, MeasureAggregator[]> executeSliceInParallel() throws Exception;
    
    /**
     * Execute the slice based on ranges
     * 
     * @param ranges
     * @return the resultMap
     * @throws Exception
     * 
     */
    QueryResult executeSlices() throws Exception;
    
    /**
     * It interrupts the executor.  
     */
    void interruptExecutor();
}
