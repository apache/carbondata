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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.query.holders.CarbonResultHolder;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;

/**
 * Interface to get the data out of any persistence medium.
 */
public interface CarbonExecutor {

    /**
     * It is used to get the facts from fact table
     *
     * @param queryModel
     * @throws IOException
     * @throws KeyGenException
     */
    void execute(CarbonQueryExecutorModel queryModel) throws IOException, KeyGenException;

    /**
     * It is used to get dimension members from hierarchies
     *
     * @param hName
     * @param dims
     * @param dimNames
     * @param constraints
     * @param hIterator
     * @throws IOException
     * @throws KeyGenException
     */
    void executeHierarichies(String hName, int[] dims, List<Dimension> dimNames,
            Map<Dimension, CarbonFilterInfo> constraints, CarbonResultHolder hIterator)
            throws IOException, KeyGenException;

    /**
     * It is used to count the dimension members.
     *
     * @param dimension
     * @param hIterator
     * @throws IOException
     */
    void executeDimensionCount(Dimension dimension, CarbonResultHolder hIterator) throws IOException;

    /**
     * It is used to count aggregation table rows.
     *
     * @param table
     * @param hIterator
     * @throws IOException
     */
    void executeAggTableCount(String table, CarbonResultHolder hIterator) throws IOException;

    /**
     * It is used to count aggregation table rows.
     *
     * @param table
     * @param hIterator
     * @throws IOException
     */
    long executeTableCount(String table) throws IOException;

    /**
     * It is used to verify that passed dimension members are existed in db.
     *
     * @param hName
     * @param hName
     * @param dims
     * @param constraints
     * @param hIterator
     * @throws IOException
     */
    void executeDimension(String hName, Dimension dim, int[] dims,
            Map<Dimension, CarbonFilterInfo> constraints, CarbonResultHolder hIterator)
            throws IOException;

    /**
     * Sets the start and end key to scan in the execution.
     *
     * @param startKey
     * @param endKey
     * @param predKeys
     * @param tables
     * @throws IOException
     */
    void setStartAndEndKeys(long[] startKey, long[] endKey, long[][] incldPredKeys,
            long[][] incldOrPredKeys, long[][] excldPredKeys, Dimension[] tables)
            throws IOException;

    /**
     * It interrupts the executor. The task which only can interrupt is through the method
     * execute(CarbonQueryExecutorModel queryModel)
     */
    void interruptExecutor();

}
