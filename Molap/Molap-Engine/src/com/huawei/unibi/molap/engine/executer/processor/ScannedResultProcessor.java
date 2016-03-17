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

package com.huawei.unibi.molap.engine.executer.processor;

import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.iterator.MolapIterator;

public interface ScannedResultProcessor
{
    /**
     * Below method will be used to add the scanned result 
     * @param scannedResult
     *          scanned result
     * @param restructureHolder
     *          restructure holder will have the information about the slice 
     * @throws QueryExecutionException
     */
    void addScannedResult(Result scannedResult) throws QueryExecutionException;
    
    /**
     * Below method will be used to process the scanned result 
     * processing type:
     * 1. TopN
     * 2. Dim Column Sorting
     * 3. Measure column Sorting 
     * @throws QueryExecutionException
     */
    MolapIterator<QueryResult> getQueryResultIterator() throws QueryExecutionException;
}
