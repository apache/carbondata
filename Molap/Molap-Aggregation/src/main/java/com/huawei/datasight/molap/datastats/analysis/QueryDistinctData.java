package com.huawei.datasight.molap.datastats.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.datastats.LoadSampler;
import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

/**
 * This class takes sample data from each load and run it over complete store
 * 
 * @author A00902717
 *
 */
public class QueryDistinctData
{

	/**
	 * Attribute for Molap LOGGER
	 */
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(QueryDistinctData.class.getName());

	private long startTime;

	private long endTime;

	private LoadSampler loadSampler;

	public QueryDistinctData(LoadSampler loadSampler)
	{
		this.loadSampler = loadSampler;
		
	}

	public Level[] queryDistinctData(String partitionId) throws AggSuggestException
	{
		Level[] dimensionDistinctData = handlePartition(partitionId);

		endTime = System.currentTimeMillis();
		long timeTaken = endTime - startTime;
		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				"Time taken to compute distinct value[millsec]:" + timeTaken);
		return dimensionDistinctData;

	}

	private Level[] handlePartition(String partitionId) throws AggSuggestException
	{
		//ordinal as key, and dimension cardinality as value
		Map<Integer,Integer> cardinality = loadSampler.getLastLoadCardinality();

		startTime = System.currentTimeMillis();
		List<Dimension> allDimensions = loadSampler.getDimensions();

		LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
				"Reading Sample data for dimension:" + allDimensions.size());

		/**
		 * For each dimension, what are other dimension distinct value
		 */

		Level[] distinctResult = new Level[allDimensions.size()];
		ArrayList<Level> levelToAnalyse = new ArrayList<Level>(allDimensions.size());
		int dimCounter=0;
		for(Dimension dimension:allDimensions)
		{
			Level level = new Level(dimension.getOrdinal(), cardinality.get(dimension.getOrdinal()));
			level.setName(dimension.getColName());
			distinctResult[dimCounter] = level;
			if (cardinality.get(dimension.getOrdinal()) > Preference.IGNORE_CARDINALITY)
			{
				levelToAnalyse.add(distinctResult[dimCounter]);
			}
			dimCounter++;
		}
		//sort based on cardinality
		Collections.sort(levelToAnalyse);

		SampleAnalyzer sampleDataAnalyzer = new SampleAnalyzer(loadSampler,
				distinctResult);
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
