package com.huawei.datasight.molap.querystats;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.huawei.datasight.molap.autoagg.AutoAggSuggestionService;
import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.autoagg.model.AggSuggestion;
import com.huawei.datasight.molap.autoagg.model.Request;
import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.datasight.molap.datastats.util.AggCombinationGeneratorUtil;
import com.huawei.datasight.molap.datastats.util.DataStatsUtil;
import com.huawei.unibi.molap.engine.querystats.BinaryQueryStore;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.engine.querystats.QueryDetail;
import com.huawei.unibi.molap.engine.querystats.QueryNormalizer;
import com.huawei.unibi.molap.engine.querystats.QueryStore;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.Cube;

/**
 * Aggregate suggestion based on query history It reads query history and
 * generate combination
 * 
 * @author A00902717
 *
 */
public class QSAutoAggSuggestionService implements AutoAggSuggestionService
{

	/**
	 * This method gives list of all dimensions can be used in Aggregate table
	 * @param schema
	 * @param cube
	 * @return
	 * @throws AggSuggestException 
	 */
	@Override
	public List<String> getAggregateDimensions(LoadModel loadModel) throws AggSuggestException
	{
		try
		{
			List<AggSuggestion> aggSuggest = generateAggregatesCombination(loadModel);
			return AggCombinationGeneratorUtil.getDimensionsWithMeasures(aggSuggest,loadModel.getCube());
		}
		catch(AggSuggestException e)
		{
			throw e;
		}
		catch(Exception e)
		{
			throw new AggSuggestException("Failed to get aggregate suggestion",e);
		}
		
	}

	/**
	 * this method gives all possible aggregate table script
	 * @param schema
	 * @param cube
	 * @return
	 * @throws AggSuggestException 
	 */
	@Override
	public List<String> getAggregateScripts(LoadModel loadModel) throws AggSuggestException
	{
		try
		{
			List<AggSuggestion> aggSuggest = generateAggregatesCombination(loadModel);
			List<String> aggSuggestsScript = AggCombinationGeneratorUtil
					.createAggregateScript(aggSuggest, loadModel.getCube(),loadModel.getSchemaName(),loadModel.getCubeName());
			return aggSuggestsScript;	
		}
		catch(AggSuggestException e)
		{
			throw e;
		}
		catch(Exception e)
		{
			throw new AggSuggestException("Failed to get aggregate suggestion",e);
		}
		
		
	}

	private List<AggSuggestion> generateAggregatesCombination(LoadModel loadModel) throws AggSuggestException
	{

		//TO-DO need to remove when we find solution to find no of rows scanned by query for filter query
		Level[] distinctData=DataStatsUtil.getDistinctDataFromDataStats(loadModel);
		
		StringBuffer queryStatsPath = new StringBuffer(loadModel.getMetaDataPath());
		queryStatsPath.append(File.separator).append(Preference.AGGREGATE_STORE_DIR).append(File.separator)
				.append(Preference.QUERYSTATS_FILE_NAME);
		
		//read logged query from file
		QueryDetail[] queryDetails=getNormalizedQueries(queryStatsPath.toString(),distinctData);
		Arrays.sort(queryDetails);
		int combinationSize = Preference.AGG_COMBINATION_SIZE;

		if (queryDetails.length < combinationSize)
		{
			combinationSize = queryDetails.length;
		}

		List<AggSuggestion> aggCombinations = new ArrayList<AggSuggestion>(
				queryDetails.length);
		for (int i = 0; i < combinationSize; i++)
		{
			QueryDetail queryDetail = queryDetails[i];
			int[] dimOrdinal = queryDetail.getDimOrdinals();
			Level[] levels = getLevel(dimOrdinal, loadModel.getCube());
			if(null!=levels)
			{
				AggSuggestion combination = new AggSuggestion(levels,Request.QUERY_STATS);
				aggCombinations.add(combination);	
			}
			

		}
		return aggCombinations;

	}

	/**
	 * This method will read querystats file and do some normalization to filter out query
	 * @param queryStatsPath
	 * @param distinctData
	 * @return
	 */
	private QueryDetail[] getNormalizedQueries(String queryStatsPath,Level[] distinctData)
	{
		QueryStore queryStore = new BinaryQueryStore();
		QueryDetail[] queryDetails = queryStore.readQueryDetail(queryStatsPath);
		
		TableSizeCalculator tableSizeCalculator=new TableSizeCalculator(distinctData);
		QueryNormalizer queryNormalizer=new QueryNormalizer();
		for(QueryDetail queryDetail:queryDetails)
		{
			//if it is filter or limit query than noOfRowsScanned will not be there, use datastats to calculate possible rows
			if(queryDetail.isFilterQuery() || queryDetail.isLimitPassed())
			{
				long tableSize=tableSizeCalculator.getApproximateRowSize(queryDetail.getDimOrdinals());
				queryDetail.setRecordSize(tableSize);
				
			}
			queryNormalizer.addQueryDetail(queryDetail);
		}
		List<QueryDetail> normalizedQueries=queryNormalizer.getNormalizedQueries();
		if(queryNormalizer.isReWriteRequired())
		{
			
			queryStore.writeQueryToFile(normalizedQueries, queryStatsPath);
		}
		return normalizedQueries.toArray(new QueryDetail[normalizedQueries.size()]);
	}

	

	private Level[] getLevel(int[] dimOrdinal, Cube cube)
	{
		MolapDef.CubeDimension[] dimensions = cube.dimensions;
		Level[] levels = new Level[dimOrdinal.length];
		int index = 0;
		int prevOrdinal=-1;
		for (int ordinal : dimOrdinal)
		{
			//here dimOrdinal will be sorted
			//this to eliminate duplicate ordinal
			if(ordinal==prevOrdinal)
			{
				continue;
			}
			Level level = new Level(dimensions[ordinal].name,ordinal);
			if(!AggCombinationGeneratorUtil.isVisible(level, cube))
			{
				return null;
			}
			levels[index++]=level;
			prevOrdinal=ordinal;
		}

		return levels;
	}

}
