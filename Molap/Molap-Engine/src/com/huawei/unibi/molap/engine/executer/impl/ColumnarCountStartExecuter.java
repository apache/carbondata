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

package com.huawei.unibi.molap.engine.executer.impl;

import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.impl.CountAggregator;
import com.huawei.unibi.molap.engine.datastorage.CubeDataStore;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.executer.SliceExecuter;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.result.iterator.MemoryBasedResultIterator;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class ColumnarCountStartExecuter implements SliceExecuter
{
    private List<InMemoryCube> slices;
    
    private String tableName;
    
    public ColumnarCountStartExecuter(List<InMemoryCube> slices, String tableName)
    {
        this.slices=slices;
        this.tableName=tableName;
    }
    
    @Override
    public MolapIterator<QueryResult> executeSlices(List<SliceExecutionInfo> infos, int[] sliceIndex) throws QueryExecutionException
    {
        long count=0;
        for(InMemoryCube slice:slices)
        {
            CubeDataStore dataCache = slice.getDataCache(this.tableName);
            if(null!=dataCache)
            {
                count+=dataCache.getData().size();
            }
        }
        MeasureAggregator[] countAgg = new MeasureAggregator[1];
        countAgg[0]=new CountAggregator();
        countAgg[0].setNewValue(count);
        QueryResult result = new QueryResult();
        ByteArrayWrapper wrapper = new ByteArrayWrapper();
        wrapper.setMaskedKey(new byte[0]);
        result.add(wrapper, countAgg);
        return new MemoryBasedResultIterator(result);
    }

}
