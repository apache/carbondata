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

import java.util.HashSet;
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.query.util.MolapEngineLogEvent;

/**
 * Maintains a list of queries on which are working on the given slice. Once all
 * the queries execution finished, informs the cube store to clear the cache for
 * the slice.
 */
public class SliceListener {
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(SliceListener.class.getName());
    /**
     * On which slice this is working on
     */
    private InMemoryCube slice;
    /**
     * Queries executing currently on this slice
     */
    private Set<Long> queries = new HashSet<Long>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

    /**
     * @param slice
     */
    public SliceListener(InMemoryCube slice) {
        this.slice = slice;
    }

    /**
     * Add the given query to dependents.
     *
     * @param queryID
     */
    public void registerQuery(Long queryID) {
        queries.add(queryID);
    }

    /**
     * @return
     */
    public String getCubeUniqueName() {
        return slice.getCubeUniqueName();
    }

    public void fireQueryFinish(Long queryId) {
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "SliceListener: query finished " + queryId);
        //System.out.println("SliceListener: query finished " + queryId);
        //Don't remove till some one makes it as in active
        if (!slice.isActive()) {
            queries.remove(queryId);
        }

        if (queries.size() == 0) {
            // to avoid ConcurrentModificationException while sliceListiterating
            //By:Sojer z00218041
            /**
             * This code is commented for checkstyle issue where
             * SLICE_LIST_CONCURRENT = false; where the loop will never run
             *
             * @author C00900810
             */
            // try
            // {
            // while(InMemoryCubeStore.getInstance().isSliceListConcurrent())
            // {
            // Thread.sleep(1);
            // }
            // }
            // catch(InterruptedException e)
            // {
            // LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
            // "InterruptedException");
            // }
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "SliceListener: Unregistering slice " + slice.getID());
            //System.out.println("SliceListener: Unregistering slice " + slice.getID());

            //Yes this slice is ready to clear
            InMemoryCubeStore.getInstance().unRegisterSlice(slice.getCubeUniqueName(), slice);
            slice.clean();

            // By Sojer z00218041 if the query is in waiting and old execution
            // finished, change QUERY_EXECUTE_STATUS and deal with cache
            InMemoryCubeStore.getInstance().afterClearQueriesAndCubes(slice.getCubeUniqueName());
        }
    }

    /**
     * Is there any more queries pending to notify?
     */
    public boolean stillListening() {
        return queries.size() != 0;
    }
}
