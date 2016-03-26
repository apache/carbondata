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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.metadata.MolapMetadata.Dimension;
import org.carbondata.processing.suggest.autoagg.exception.AggSuggestException;
import org.carbondata.processing.suggest.datastats.LoadSampler;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.carbondata.query.querystats.Preference;
import org.carbondata.query.util.MolapEngineLogEvent;

/**
 * This class takes sample data from each load and run it over complete store
 *
 * @author A00902717
 */
public class QueryDistinctData {

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(QueryDistinctData.class.getName());

    private long startTime;

    private long endTime;

    private LoadSampler loadSampler;

    public QueryDistinctData(LoadSampler loadSampler) {
        this.loadSampler = loadSampler;

    }

    public Level[] queryDistinctData(String partitionId) throws AggSuggestException {
        Level[] dimensionDistinctData = handlePartition(partitionId);

        endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Time taken to compute distinct value[millsec]:" + timeTaken);
        return dimensionDistinctData;

    }

    private Level[] handlePartition(String partitionId) throws AggSuggestException {
        //ordinal as key, and dimension cardinality as value
        Map<Integer, Integer> cardinality = loadSampler.getLastLoadCardinality();

        startTime = System.currentTimeMillis();
        List<Dimension> allDimensions = loadSampler.getDimensions();

        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Reading Sample data for dimension:" + allDimensions.size());

        /**
         * For each dimension, what are other dimension distinct value
         */

        Level[] distinctResult = new Level[allDimensions.size()];
        ArrayList<Level> levelToAnalyse = new ArrayList<Level>(allDimensions.size());
        int dimCounter = 0;
        for (Dimension dimension : allDimensions) {
            Level level =
                    new Level(dimension.getOrdinal(), cardinality.get(dimension.getOrdinal()));
            level.setName(dimension.getColName());
            distinctResult[dimCounter] = level;
            if (cardinality.get(dimension.getOrdinal()) > Preference.IGNORE_CARDINALITY) {
                levelToAnalyse.add(distinctResult[dimCounter]);
            }
            dimCounter++;
        }
        //sort based on cardinality
        Collections.sort(levelToAnalyse);

        SampleAnalyzer sampleDataAnalyzer = new SampleAnalyzer(loadSampler, distinctResult);
        sampleDataAnalyzer.execute(levelToAnalyse, partitionId);
        return distinctResult;

    }

    /**
     * This will put result in file
     *
     * @param loadResultData
     */
    /*private void outputResult(Level[] dimensionDistinctData)
	{
		StringBuffer buffer = new StringBuffer();
		List<Dimension> allDimensions = cube.getDimensions(loadSampler
				.getTableName());
		for (int i = 0; i < dimensionDistinctData.length; i++)
		{

			Level dimensionData = dimensionDistinctData[i];
			if(dimensionData.getCardinality()<=Preference.IGNORE_CARDINALITY) 
			{
				continue;
			}
			buffer.append(dimensionData.getName()+'('+dimensionData.getCardinality()+')');
			buffer.append("->");
			int[] otherDimStats = dimensionData.getOtherDimesnionDistinctData();
			if (null == otherDimStats)
			{
				continue;
			}
			for(int j=0;j<otherDimStats.length;j++)
			{
				buffer.append(allDimensions.get(j).getColName() + '['
						+ otherDimStats[j] + ']');
			}
			
			buffer.append('\n');

		}
		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				'\n' + buffer.toString());

	}*/

}
