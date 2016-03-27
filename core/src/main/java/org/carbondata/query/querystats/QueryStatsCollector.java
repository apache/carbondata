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

package org.carbondata.query.querystats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * This class will collect query stats and other details like cube,schema, and
 * fact table and delegate the request to QueryLogger to log the query
 *
 * @author A00902717
 */
public final class QueryStatsCollector {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(QueryStatsCollector.class.getName());

    private static QueryStatsCollector queryStatsCollector;

    private Map<String, QueryDetail> queryStats;

    private QueryStore queryStore;

    private PartitionAccumulator partitionAccumulator;

    private QueryStatsCollector() {
        queryStore = new BinaryQueryStore();
        queryStats = new ConcurrentHashMap<String, QueryDetail>(1000);
        partitionAccumulator = new PartitionAccumulator();
    }

    public static synchronized QueryStatsCollector getInstance() {
        if (null == queryStatsCollector) {
            queryStatsCollector = new QueryStatsCollector();

        }
        return queryStatsCollector;
    }

    public void addQueryStats(String queryId, QueryDetail queryDetail) {
        queryStats.put(queryId, queryDetail);
    }

    public void removeQueryStats(String queryId) {
        queryStats.remove(queryId);
    }

    public QueryDetail getQueryStats(String queryId) {
        return queryStats.get(queryId);
    }

    public void logQueryStats(QueryDetail queryDetail) {
        try {
            if (null != queryDetail.getDimOrdinals()) {
                queryStore.logQuery(queryDetail);
            }
        } catch (Exception e) {
            LOGGER.info(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Error in logging query:" + e);
        }

    }

    public PartitionDetail getInitialPartitionAccumulatorValue() {
        return null;
    }

    public PartitionAccumulator getPartitionAccumulatorParam() {
        return partitionAccumulator;
    }

}
