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

package org.carbondata.query.executer.pagination;

import java.io.IOException;
import java.util.Map;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.impl.RestructureHolder;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.result.Result;
import org.carbondata.query.wrappers.ByteArrayWrapper;

/**
 * It aggregates the data received from individual thread. And also it writes to the disk if the limit exceeds
 */
public interface GlobalPaginatedAggregator {
    /**
     * Get the rows for the range specified
     *
     * @param fromRow
     * @param toRow
     * @return
     * @throws IOException
     */
    Map<ByteArrayWrapper, MeasureAggregator[]> getPage(int fromRow, int toRow) throws IOException;

    /**
     * Get the rows for the range specified
     *
     * @param fromRow
     * @param toRow
     * @return
     * @throws IOException
     */
    QueryResult getResult() throws IOException;

    /**
     * Write data to disk if rquired
     *
     * @param data
     * @param restructureHolder
     * @throws Exception
     */
    void writeToDisk(Map<ByteArrayWrapper, MeasureAggregator[]> data,
            RestructureHolder restructureHolder) throws Exception;

    /**
     * Write data to disk if rquired
     *
     * @param data
     * @param restructureHolder
     * @throws Exception
     */
    void writeToDisk(Result data, RestructureHolder restructureHolder) throws Exception;

    /**
     * Final merge of all intermediate files if any
     */
    void mergeDataFile(boolean cacheRequired);

    /**
     * Get the total row count
     *
     * @return
     */
    int getRowCount();

}
