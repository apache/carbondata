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

package org.carbondata.query.datastorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * 1) Maintains the information about which MDX query is being executed by which
 * thread. Can use execution id to identify the MDX sequence.
 * 2) Maintains registered delta copies of cubes available for this execution.
 * Even sub query against DB for this MDX can work only on available slices to
 * give consistent results across the MDX query life cycle.
 * 3) Maintains listeners on queries and inform them when query execution is
 * finished.
 */
public final class QueryMapper {

    /**
     * Attribute for Carbon LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(QueryMapper.class.getName());
    /**
     * Map<CubeName, Map<ThreadID, QueryID>>
     */
    private static Map<String, Map<Long, Long>> executionMap =
            new HashMap<String, Map<Long, Long>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * Map<ThreadID, List<SliceIDs>>
     */
    private static Map<Long, List<Long>> executionToSlicesMap =
            new HashMap<Long, List<Long>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * QueryId --> List<SliceListeners>
     */
    private static Map<Long, List<SliceListener>> listeners =
            new HashMap<Long, List<SliceListener>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    private QueryMapper() {

    }

    /**
     * Returns all the queries that are using this given slice.
     *
     * @param slice
     * @return
     */
    public static synchronized List<Long> getQueriesPerSlice(InMemoryTable slice) {
        List<Long> queries = new ArrayList<Long>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        Map<Long, Long> threadQueryMap = executionMap.get(slice.getCubeUniqueName());
        if (threadQueryMap == null) {
            return queries;
        }

        for (Map.Entry<Long, Long> threadQuery : threadQueryMap.entrySet()) {
            // If the thread to slice map contains the the given slice Id, add
            // to list;
            List<Long> slices = executionToSlicesMap.get(threadQuery.getKey());
            if (slices != null && slices.contains(slice.getID())) {
                queries.add(threadQuery.getValue());
            }
        }

        return queries;
    }

    /* private static boolean loadSlice = false; */

    /**
     * Register the query on this thread
     *
     * @param cubeName
     * @param queryID
     * @param threadID
     */
    public static void queryStart(String cubeUniqueName, long queryID) {

        // TODO temporary
        // Load slice from D:\hiers\part2
        // CubeSliceLoader.maxAllowedSlices=1;
        // if(loadSlice)
        // {
        // CubeSliceLoader sliceLoader = new CubeSliceLoader();
        // sliceLoader.loadSliceFromFiles("sur", "D:/hiers/part2/");
        // }
        // loadSlice=!loadSlice;
        //
        // //TODO flush cache for each query for time being
        // InMemoryCubeStore
        // .getInstance()
        // .getActiveSlices(cubeName)
        // .get(0)
        // .getRolapCube()
        // .getSchema()
        // .getInternalConnection()
        // .getCacheControl(null)
        // .flushSchema(
        // InMemoryCubeStore.getInstance().getActiveSlices(cubeName).get(0).getRolapCube().getSchema());

        Long threadId = Thread.currentThread().getId();

        synchronized (QueryMapper.class) {

            if (executionToSlicesMap.containsKey(threadId)) {
                // already registered start of query while validating the query.
                return;
            }

            Map<Long, Long> cubeMap = executionMap.get(cubeUniqueName);
            if (cubeMap == null) {
                cubeMap = new HashMap<Long, Long>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                executionMap.put(cubeUniqueName, cubeMap);
            }

            // Register the thread for query
            cubeMap.put(threadId, queryID);

            LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "QueryMapper :: Thread " + threadId + " is executing query " + queryID
                            + " on cube " + cubeUniqueName);
            //System.out.println("QueryMapper :: Thread " + threadId + " is executing query " + queryID + " on cube "
            //   + cubeName);

            // Register available cube slices for query (Thread)
            if (InMemoryTableStore.getInstance().findCache(cubeUniqueName)) {
                List<Long> slices =
                        InMemoryTableStore.getInstance().getActiveSliceIds(cubeUniqueName);
                executionToSlicesMap.put(threadId, slices);
                LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                        "QueryMapper :: Available slices : " + slices);
                //System.out.println("QueryMapper :: Available slices : " + slices);
            }
        }
    }

    /**
     * Register the query on this thread, and copy the slices applicable from
     * the parent thread to the current thread.
     *
     * @param cubeName
     * @param queryID
     * @param threadID
     */
    public static void queryStart(String cubeUniqueName, long queryID, long parentThreadId) {
        Long threadId = Thread.currentThread().getId();

        synchronized (QueryMapper.class) {
            Map<Long, Long> cubeMap = executionMap.get(cubeUniqueName);
            // no need to do null check and recreate the map as it would have
            // been created by the parent thread

            // Register the thread for query
            cubeMap.put(threadId, queryID);

            // Register available cube slices for query (Thread)
            executionToSlicesMap.put(threadId, executionToSlicesMap.get(parentThreadId));
        }
    }

    /**
     * Unregister the query on this thread
     *
     * @param cubeName
     * @param queryID
     * @param threadID
     */
    public static void queryEnd(String cubeUniqueName, long queryID) {
        queryEnd(cubeUniqueName, queryID, true);
    }

    /**
     * Unregister the query on this thread
     *
     * @param cubeName
     * @param queryID
     * @param threadID
     */
    public static synchronized void queryEnd(String cubeUniqueName, long queryID, boolean publish) {
        Long threadId = Thread.currentThread().getId();
        Map<Long, Long> cubeMap = executionMap.get(cubeUniqueName);
        cubeMap = executionMap.get(cubeUniqueName);
        if (cubeMap != null) {
            // Remove the query entry from thread
            Long queryId = cubeMap.remove(threadId);

            // Remove slices registry for thread/query
            executionToSlicesMap.remove(threadId);

            if (publish) {
                // if multiple threads are using executing single query then we
                // need to only
                // ensure that the listeners are called only for the main thread
                invokeListeners(queryId);
            }
        }
    }

    /**
     * Call the listeners registered for this query
     *
     * @param queryId
     */
    private static void invokeListeners(Long queryId) {
        List<SliceListener> listOnQuery = listeners.get(queryId);

        if (listOnQuery == null || listOnQuery.size() == 0) {
            return;
        }

        List<SliceListener> toRemove =
                new ArrayList<SliceListener>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

        for (SliceListener listener : listOnQuery) {
            // Check and intimate listeners
            listener.fireQueryFinish(queryId);

            if (!listener.stillListening()) {
                // Make listers with zero query references
                toRemove.add(listener);
            }
        }

        // Remove all unwanted listeners
        if (toRemove.size() > 0) {
            listOnQuery.removeAll(toRemove);
        }
    }

    /**
     * How many queries working on this cube?
     *
     * @param cubeName
     * @return
     */
    public static synchronized int getActiveQueriesCount(String cubeUniqueName) {
        Map<Long, Long> cubeMap = executionMap.get(cubeUniqueName);
        if (cubeMap != null) {
            return cubeMap.size();
        }

        return 0;
    }

    /**
     * @param threadId
     * @return
     */
    public static synchronized List<Long> getSlicesForThread(Long threadId) {
        return (executionToSlicesMap.get(threadId) == null ?
                null :
                new ArrayList<Long>(executionToSlicesMap.get(threadId)));
    }

    /**
     * @param listener
     * @param threadID
     */
    public static synchronized void registerSliceListener(SliceListener listener, long queryId) {
        // Long queryId =
        // executionMap.get(listener.getCubeName()).get(threadID);

        List<SliceListener> list = listeners.get(queryId);
        if (list == null) {
            list = new ArrayList<SliceListener>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
            listeners.put(queryId, list);
        }

        list.add(listener);
    }

}
