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

package org.carbondata.query.executer;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.result.RowResult;

public interface QueryExecutor {

    /**
     * Below method will be used to execute the query
     *
     * @param queryModel query model, properties which will be required to execute the query
     * @throws QueryExecutionException will throw query execution exception in case of any abnormal scenario
     */
    CarbonIterator<RowResult> execute(QueryModel queryModel)
            throws QueryExecutionException;

    /**
     * Below method will be used to execute the query of QuickFilter
     *
     * @param queryModel , properties which will be required to execute the query
     * @throws QueryExecutionException will throw query execution exception in case of any abnormal scenario
     */
    CarbonIterator<RowResult> executeDimension(QueryModel queryModel)
            throws QueryExecutionException;

}
