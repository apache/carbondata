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

package org.carbondata.query.result.iterator;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.executer.impl.QueryExecuterProperties;
import org.carbondata.query.executer.impl.QueryResultPreparator;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.result.ChunkResult;

public class ChunkBasedResultIterator implements CarbonIterator<ChunkResult> {
    private CarbonIterator<QueryResult> queryResultIterator;

    private QueryResultPreparator queryResultPreparator;

    public ChunkBasedResultIterator(CarbonIterator<QueryResult> queryResultIterator,
            QueryExecuterProperties executerProperties, CarbonQueryExecutorModel queryModel) {
        this.queryResultIterator = queryResultIterator;
        this.queryResultPreparator = new QueryResultPreparator(executerProperties, queryModel);

    }

    @Override
    public boolean hasNext() {
        return queryResultIterator.hasNext();
    }

    @Override
    public ChunkResult next() {
        return queryResultPreparator.prepareQueryOutputResult(queryResultIterator.next());
    }

}
