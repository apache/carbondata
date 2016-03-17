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

package com.huawei.unibi.molap.engine.result.iterator;

import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.executer.impl.QueryExecuterProperties;
import com.huawei.unibi.molap.engine.executer.impl.QueryResultPreparator;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.result.ChunkResult;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class ChunkBasedResultIterator implements MolapIterator<ChunkResult>
{
    private MolapIterator<QueryResult> queryResultIterator;
    
    private QueryResultPreparator queryResultPreparator;
    
    public ChunkBasedResultIterator(MolapIterator<QueryResult> queryResultIterator, QueryExecuterProperties executerProperties, MolapQueryExecutorModel queryModel)
    {
        this.queryResultIterator=queryResultIterator;
        this.queryResultPreparator=new QueryResultPreparator(executerProperties,queryModel);
        
    }
    @Override
    public boolean hasNext()
    {
        return queryResultIterator.hasNext();
    }

    @Override
    public ChunkResult next()
    {
        return queryResultPreparator.prepareQueryOutputResult(queryResultIterator.next());
    }

}
