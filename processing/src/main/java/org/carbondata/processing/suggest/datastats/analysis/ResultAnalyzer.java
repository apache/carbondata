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

package org.carbondata.processing.suggest.datastats.analysis;

import java.util.*;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.carbondata.query.result.RowResult;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * this class will read iterator and find out distinct dimension data
 *
 * @author A00902717
 */
public class ResultAnalyzer {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(ResultAnalyzer.class.getName());

    private Dimension masterDimension;

    public ResultAnalyzer(Dimension masterDimension) {
        this.masterDimension = masterDimension;
    }

    /**
     * result will be set in distinctOfMasterDim
     *
     * @param rowIterator
     * @param distinctOfMasterDim
     * @param cardinalities
     * @return result: It will have each dimension and its distinct value
     */
    public void analyze(CarbonIterator<RowResult> rowIterator, ArrayList<Level> dimensions,
            Map<Integer, Integer> distinctOfMasterDim) {
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Scanning query Result:" + System.currentTimeMillis());

        //Object:master data
        //HashMap<Integer, HashSet<Object> -> slaveDimension,slavesData for each master data
        HashMap<Object, HashMap<Integer, HashSet<Object>>> datas =
                new HashMap<Object, HashMap<Integer, HashSet<Object>>>(100);

        while (rowIterator.hasNext()) {
            RowResult rowResult = rowIterator.next();
            Object[] results = rowResult.getKey().getKey();
            int columnCounter = 0;
            HashMap<Integer, HashSet<Object>> masterDatas = null;
            for (Level dimension : dimensions) {
                Object resData = results[columnCounter++];

                if (dimension.getOrdinal() == masterDimension.getOrdinal()) {
                    masterDatas = datas.get(resData);
                    if (null == masterDatas) {
                        masterDatas = new HashMap<Integer, HashSet<Object>>(dimensions.size());
                        datas.put(resData, masterDatas);
                    }
                    continue;
                }
                if (masterDatas != null) {
                    HashSet<Object> slaveData = masterDatas.get(dimension.getOrdinal());
                    if (null == slaveData) {
                        slaveData = new HashSet<Object>(dimensions.size());
                        masterDatas.put(dimension.getOrdinal(), slaveData);
                    }
                    slaveData.add(resData);
                }
            }

        }
        calculateAverage(datas, distinctOfMasterDim, dimensions);

    }

    /**
     * for each dimension count no of values and average it and return the result
     *
     * @param datas
     * @param distinctOfMasterDim
     * @param dimensions
     * @return
     */
    private void calculateAverage(HashMap<Object, HashMap<Integer, HashSet<Object>>> datas,
            Map<Integer, Integer> distinctOfMasterDim, ArrayList<Level> allDimensions) {

        //Step 1 merging all slave data
        Set<Entry<Object, HashMap<Integer, HashSet<Object>>>> sampleDatas = datas.entrySet();
        Iterator<Entry<Object, HashMap<Integer, HashSet<Object>>>> sampleDataItr =
                sampleDatas.iterator();
        while (sampleDataItr.hasNext()) {
            Entry<Object, HashMap<Integer, HashSet<Object>>> sampleData = sampleDataItr.next();
            HashMap<Integer, HashSet<Object>> slavesData = sampleData.getValue();
            Set<Entry<Integer, HashSet<Object>>> slaves = slavesData.entrySet();
            Iterator<Entry<Integer, HashSet<Object>>> slaveItr = slaves.iterator();
            while (slaveItr.hasNext()) {
                Entry<Integer, HashSet<Object>> slave = slaveItr.next();
                int slaveOrdinal = slave.getKey();
                HashSet<Object> slaveData = slave.getValue();
                Integer existingVal = distinctOfMasterDim.get(slaveOrdinal);

                if (null == existingVal || 0 == existingVal) {
                    distinctOfMasterDim.put(slaveOrdinal, slaveData.size());

                } else {
                    distinctOfMasterDim.put(slaveOrdinal, (existingVal + slaveData.size()));

                }

            }
        }

        //step 2 calculating average
        Iterator<Level> itr = allDimensions.iterator();
        while (itr.hasNext()) {
            Level level = itr.next();
            if (level.getOrdinal() == masterDimension.getOrdinal()) {
                continue;
            }
            Integer distinctData = distinctOfMasterDim.get(level.getOrdinal());

            Double avg = 0.0;
            if (null == distinctData) {
                distinctData = 0;
            }

            if (sampleDatas.size() > 0) {
                avg = Double.valueOf(Math.round((double) distinctData / sampleDatas.size()));
            }

            distinctOfMasterDim.put(level.getOrdinal(), avg.intValue() == 0 ? 1 : avg.intValue());

        }
        LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                "Finished Scanning query Result:" + System.currentTimeMillis());

    }

}
