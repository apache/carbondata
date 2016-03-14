package com.huawei.datasight.molap.autoagg;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;

import org.junit.Before;
import org.junit.Test;

import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.autoagg.model.Request;
import com.huawei.datasight.molap.autoagg.util.CommonUtil;
import com.huawei.datasight.molap.datastats.analysis.QueryDistinctData;
import com.huawei.datasight.molap.datastats.model.DriverDistinctData;
import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.datasight.molap.datastats.util.AggCombinationGeneratorUtil;
import com.huawei.datasight.molap.util.TestUtil;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.util.MolapProperties;

public class DataStatsAggregateServiceTest
{
	static MolapDef.Schema schema;
	static MolapDef.Cube cube;

	static String schemaName;
	static String cubeName;
	static String factTable;

	static String dataPath;
	static String baseMetaPath;
	private LoadModel loadModel;

	@Before
	public void setUpBeforeClass() throws Exception
	{
		try
		{

			File file = new File(
					"../../libraries/testData/Molap-Aggregation/store/");
			String basePath = file.getCanonicalPath() + "/";
			String metaPath = basePath + "schemas/default/carbon/metadata";

			MolapProperties.getInstance().addProperty("molap.storelocation",
					basePath + "store");
			MolapProperties.getInstance().addProperty("molap.number.of.cores",
					"4");
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefitRatio", "10");
			MolapProperties.getInstance().addProperty(
					Preference.AGG_LOAD_COUNT, "2");
			MolapProperties.getInstance().addProperty(
					Preference.AGG_FACT_COUNT, "2");
			MolapProperties.getInstance().addProperty(Preference.AGG_REC_COUNT,
					"5");
			schema = CommonUtil.readMetaData(metaPath).get(0);
			cube = schema.cubes[0];
			schemaName = schema.name;
			cubeName = cube.name;
			factTable = "carbon";
			// dataPath="src/test/resources/store/store";
			// baseMetaPath="src/test/resources/store/schemas";
			dataPath = basePath + "store";
			baseMetaPath = basePath + "schemas/default/carbon";
			loadModel = TestUtil.createLoadModel(schemaName, cubeName, schema,
					cube, dataPath, baseMetaPath);
			CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
			CommonUtil.fillSchemaAndCubeDetail(loadModel);
		}
		catch (Exception e)
		{

		}

	}

	@Test
	public void testDataStats_distinctRelNotSerialized_getDimensions()
	{

		try
		{
			// LoadModel loadModel
			// =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			// delete if data stats serialized file exist
			File dataStatsPath = new File(
					TestUtil.getDistinctDataPath(loadModel));
			if (dataStatsPath.exists())
			{
				dataStatsPath.delete();
			}

			// delete if data stats aggregate combination file exist
			File dataStatsPathAggCombination = new File(
					TestUtil.getDataStatsAggCombination(schemaName, cubeName,
							factTable));
			if (dataStatsPathAggCombination.exists())
			{
				dataStatsPathAggCombination.delete();
			}

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.DATA_STATS);

			List<String> aggCombinations = aggService
					.getAggregateDimensions(loadModel);
			if(aggCombinations.size()>0)
			{
				Assert.assertTrue(true);
			}
			else
			{
				Assert.assertTrue(false);
			}
			
		}
		catch (Exception e)
		{
			Assert.assertTrue(false);
		}

	}

	@Test
	public void testDataStats_distinctRelIsSerialized_getDimensions()
	{
		try
		{
			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.DATA_STATS);
			// LoadModel loadModel
			// =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			List<String> aggCombinations = aggService
					.getAggregateDimensions(loadModel);
			if(aggCombinations.size()>0)
			{
				Assert.assertTrue(true);
			}
			else
			{
				Assert.assertTrue(false);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.assertTrue(false);
		}

	}

	@Test
	public void testDataStats_distinctRelNotSerialized_getScript()
	{

		try
		{
			// LoadModel loadModel
			// =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			// delete if serialized file exist
			File dataStatsPath = new File(
					TestUtil.getDistinctDataPath(loadModel));
			if (dataStatsPath.exists())
			{
				dataStatsPath.delete();
			}

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.DATA_STATS);

			List<String> aggCombinations = aggService
					.getAggregateScripts(loadModel);
			if(aggCombinations.size()>0)
			{
				Assert.assertTrue(true);
			}
			else
			{
				Assert.assertTrue(false);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.assertTrue(false);
		}

	}

	@Test
	public void testDataStats_distinctRelIsSerialized_getScript()
	{
		try
		{

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.DATA_STATS);
			// LoadModel loadModel
			// =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			List<String> aggCombinations = aggService
					.getAggregateScripts(loadModel);
			if(aggCombinations.size()>0)
			{
				Assert.assertTrue(true);
			}
			else
			{
				Assert.assertTrue(false);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.assertTrue(false);

		}

	}

	@Test
	public void testGetAggregateDimensions_throwsException()
	{

		File dataStatsPath = new File(TestUtil.getDistinctDataPath(loadModel));
		if (dataStatsPath.exists())
		{
			dataStatsPath.delete();
		}

		AutoAggSuggestionService aggService = AutoAggSuggestionFactory
				.getAggregateService(Request.DATA_STATS);
		try
		{

			new MockUp<QueryDistinctData>()
			{

				@Mock
				public Level[] queryDistinctData(String partitionId)
						throws AggSuggestException
				{
					throw new AggSuggestException("error",
							new NullPointerException());
				}

			};
			aggService.getAggregateDimensions(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}
		try
		{


			aggService.getAggregateDimensions(null);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}

	@Test
	public void testGetAggregateScript_throwsException()
	{
		File dataStatsPath = new File(
				TestUtil.getDistinctDataPath(loadModel));
		if (dataStatsPath.exists())
		{
			dataStatsPath.delete();
		}

		AutoAggSuggestionService aggService = AutoAggSuggestionFactory
				.getAggregateService(Request.DATA_STATS);


		try
		{
			
			new MockUp<QueryDistinctData>()
			{

				@Mock
				public Level[] queryDistinctData(String partitionId)
						throws AggSuggestException
				{
					throw new AggSuggestException("error",
							new NullPointerException());
				}

			};
			aggService.getAggregateScripts(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}
		try
		{
			

			aggService.getAggregateScripts(null);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}
	
	@Test
	public void testNoVisibleLevel()
	{
		new MockUp<AggCombinationGeneratorUtil>()
		{

			@Mock
			public Level[] getVisibleLevels(Level[] aggLevels,Cube cube)
			{
				return new Level[0];
			}

		};
	}
	
	@Test
	public void testSetCalculatedLoads()
	{
		new MockUp<DriverDistinctData>()
		{

			@Mock
			public List<String> getLoads()
			{
				List<String> loads=new ArrayList<String>();
				return loads;
			}

		};
		File dataStatsPath = new File(
				TestUtil.getDistinctDataPath(loadModel));
		if (dataStatsPath.exists())
		{
			dataStatsPath.delete();
		}

		AutoAggSuggestionService aggService = AutoAggSuggestionFactory
				.getAggregateService(Request.DATA_STATS);

		List<String> aggCombinations;
		try
		{
			aggCombinations = aggService
					.getAggregateScripts(loadModel);
			aggCombinations = aggService
					.getAggregateScripts(loadModel);
			Assert.assertNotNull(aggCombinations);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(false);
		}
	
		
	}

}
