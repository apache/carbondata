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

package com.huawei.unibi.molap.engine.processor.row;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.processor.DataProcessorExt;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class RowProcessor implements DataProcessorExt
{

    protected DataProcessorExt dataProcessor;
    
    public RowProcessor(DataProcessorExt dataProcessor)
    {
        this.dataProcessor=dataProcessor;
    }
    
    @Override
    public void initialise(DataProcessorInfo model) throws DataProcessorException
    {
        dataProcessor.initialise(model);
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] value) throws DataProcessorException
    {
        dataProcessor.processRow(key, value);
    }
    
    @Override
    public void finish() throws DataProcessorException
    {
        dataProcessor.finish();
    }

    @Override
    public MolapIterator<QueryResult> getQueryResultIterator()
    {
        return dataProcessor.getQueryResultIterator();
    }

    @Override
    public void processRow(ByteArrayWrapper key, MeasureAggregator[] value) throws DataProcessorException
    {
        dataProcessor.processRow(key, value);
        
    }

}
