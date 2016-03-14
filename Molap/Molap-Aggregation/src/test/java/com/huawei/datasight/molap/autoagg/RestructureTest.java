package com.huawei.datasight.molap.autoagg;

import java.io.File;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.huawei.datasight.molap.autoagg.model.Request;
import com.huawei.datasight.molap.autoagg.util.CommonUtil;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.datasight.molap.util.TestUtil;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.util.MolapProperties;

public class RestructureTest
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
					"../../libraries/testData/Molap-Aggregation/restructure/");
			String basePath = file.getCanonicalPath() + "/";
			String metaPath = basePath + "schemas/default/rs/metadata";

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
			factTable = "rs";
			// dataPath="src/test/resources/store/store";
			// baseMetaPath="src/test/resources/store/schemas";
			dataPath = basePath + "store";
			baseMetaPath = basePath + "schemas/default/rs";
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
	public void testRestructure()
	{
		try
		{
			 LoadModel loadModel =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			 loadModel.getValidSlices().add("Load_1");
			 loadModel.getValidSlices().add("Load_2");
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
			Assert.assertNotNull(aggCombinations);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.assertTrue(false);
		}
		
	}
	
	@Test
	public void testRestructure_CachedDistinctDataAndLoadisDeleted()
	{
		try
		{
			 LoadModel loadModel =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			 loadModel.getValidSlices().remove(0);
			 loadModel.getValidSlices().add("Load_1");
			 loadModel.getValidSlices().add("Load_2");
			// delete if serialized file exist
			
			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.DATA_STATS);

			List<String> aggCombinations = aggService
					.getAggregateScripts(loadModel);
			Assert.assertEquals(1, aggCombinations.size());
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.assertTrue(true);
		}
		
	}
}
