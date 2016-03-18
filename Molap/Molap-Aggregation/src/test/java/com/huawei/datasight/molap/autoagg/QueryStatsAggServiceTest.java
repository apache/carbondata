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

package com.huawei.datasight.molap.autoagg;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;

import org.junit.BeforeClass;
import org.junit.Test;

import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.autoagg.model.Request;
import com.huawei.datasight.molap.autoagg.util.CommonUtil;
import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.datasight.molap.datastats.util.DataStatsUtil;
import com.huawei.datasight.molap.util.TestUtil;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.util.MolapProperties;

public class QueryStatsAggServiceTest
{
	static MolapDef.Schema schema;
	static MolapDef.Cube cube;
	
	static String schemaName;
	static String cubeName;
	static String factTable;
	static String dataPath;
	static String baseMetaPath;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		try
		{
			File file = new File("../../libraries/testData/Molap-Aggregation/store/");
			String basePath=file.getCanonicalPath()+"/";
			String metaPath=basePath+"schemas/default/carbon/metadata";
		   
		    MolapProperties.getInstance().addProperty("molap.storelocation",
					basePath + "store");
		    MolapProperties.getInstance().addProperty("molap.number.of.cores", "4");
			MolapProperties.getInstance().addProperty(
					"molap.smartJump.avoid.percent", "70");
		    schema = CommonUtil.readMetaData(metaPath).get(0);
			cube = schema.cubes[0];	
			schemaName=schema.name;
			cubeName=cube.name;
			factTable="carbon";
			dataPath=basePath+"store";
			baseMetaPath=basePath+"schemas/default/carbon";
		}
		catch(Exception e)
		{
			
		}
		
		
	}
	
	@Test
	public void testDataStats_configuredBenefitRatio_getDimensions()
	{
		try
		{
			
			MolapProperties.getInstance().addProperty("molap.agg.benefit.ratio", "2");
			
			AutoAggSuggestionService aggService = AutoAggSuggestionFactory.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			List<String> aggCombinations=aggService.getAggregateDimensions(loadModel);
			Assert.assertNotNull(aggCombinations);
		}
		catch(Exception e)
		{
			
		}
	}
	
	@Test
	public void testDataStats_distinctRelIsSerialized_getDimensions()
	{
		
		try
		{
			AutoAggSuggestionService aggService = AutoAggSuggestionFactory.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			List<String> aggCombinations=aggService.getAggregateDimensions(loadModel);
			Assert.assertNotNull(aggCombinations);	
		}
		catch(Exception e)
		{
			
		}
		
	}
	@Test
	public void testDataStats_distinctRelIsNotSerialized_getDimensions()
	{
		
		try
		{
			File file=new File(baseMetaPath+"/aggsuggestion/distinctData");
			if(file.exists())
			{
				file.delete();
			}
			AutoAggSuggestionService aggService = AutoAggSuggestionFactory.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			List<String> aggCombinations=aggService.getAggregateDimensions(loadModel);
			Assert.assertNotNull(aggCombinations);	
		}
		catch(Exception e)
		{
			
		}
		
	}
	@Test
	public void testDataStats_configuredBenefitRatio_getScript()
	{
		
		try
		{
			//delete if serialized file exist
			MolapProperties.getInstance().addProperty("molap.agg.benefit.ratio", "2");
			LoadModel loadModel =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			AutoAggSuggestionService aggService = AutoAggSuggestionFactory.getAggregateService(Request.QUERY_STATS);
			List<String> aggCombinations=aggService.getAggregateScripts(loadModel);
			Assert.assertNotNull(aggCombinations);
		}
		catch(Exception e)
		{
			
		}
		
		
	}
	
	@Test
	public void testDataStats_distinctRelIsSerialized_getScript()
	{
		try
		{
			AutoAggSuggestionService aggService = AutoAggSuggestionFactory.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
			List<String> aggCombinations=aggService.getAggregateScripts(loadModel);
			Assert.assertNotNull(aggCombinations);
		}
		catch(Exception e)
		{
			
		}
	}
	
	@Test
	public void testGetAggregateDimensions_throwsException_Exception()
	{

		try
		{
			new MockUp<DataStatsUtil>()
			{

				@Mock
				public Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
				{
					throw new NullPointerException();
				}

			};
			
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefit.ratio", "2");

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel = TestUtil.createLoadModel(schemaName,
					cubeName, schema, cube, dataPath, baseMetaPath);

			List<String> aggCombinations = aggService
					.getAggregateDimensions(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}
	@Test
	public void testGetAggregateDimensions_throwsException()
	{

		try
		{
			new MockUp<DataStatsUtil>()
			{

				@Mock
				public Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
				{
					throw new NullPointerException();
				}

			};
			
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefit.ratio", "2");

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel = TestUtil.createLoadModel(schemaName,
					cubeName, schema, cube, dataPath, baseMetaPath);

			List<String> aggCombinations = aggService
					.getAggregateDimensions(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}
	
	@Test
	public void testGetAggregateScript_throwsException_WithException()
	{

		try
		{
			new MockUp<DataStatsUtil>()
			{

				@Mock
				public Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
				{
					throw new NullPointerException();
				}

			};
			
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefit.ratio", "2");

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel = TestUtil.createLoadModel(schemaName,
					cubeName, schema, cube, dataPath, baseMetaPath);

			List<String> aggCombinations = aggService.getAggregateScripts(loadModel);
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

		try
		{
			new MockUp<DataStatsUtil>()
			{

				@Mock
				public Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
				{
					throw new NullPointerException();
				}

			};
			
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefit.ratio", "2");

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel = TestUtil.createLoadModel(schemaName,
					cubeName, schema, cube, dataPath, baseMetaPath);

			List<String> aggCombinations = aggService.getAggregateScripts(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}
	
	@Test
	public void testGetAggregateDimensions_throwsAggException_Exception()
	{

		try
		{
			new MockUp<DataStatsUtil>()
			{

				@Mock
				public Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
				{
					throw new NullPointerException();
				}

			};
			
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefit.ratio", "2");

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel = TestUtil.createLoadModel(schemaName,
					cubeName, schema, cube, dataPath, baseMetaPath);

			List<String> aggCombinations = aggService
					.getAggregateDimensions(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}
	@Test
	public void testGetAggregateDimensions_throwsAggException()
	{

		try
		{
			new MockUp<DataStatsUtil>()
			{

				@Mock
				public Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
				{
					throw new AggSuggestException("error");
				}

			};
			
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefit.ratio", "2");

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel = TestUtil.createLoadModel(schemaName,
					cubeName, schema, cube, dataPath, baseMetaPath);

			List<String> aggCombinations = aggService
					.getAggregateDimensions(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}
	
	@Test
	public void testGetAggregateScript_throwsAggException_WithException()
	{

		try
		{
			new MockUp<DataStatsUtil>()
			{

				@Mock
				public Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
				{
					throw new AggSuggestException("error",new NullPointerException());
				}

			};
			
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefit.ratio", "2");

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel = TestUtil.createLoadModel(schemaName,
					cubeName, schema, cube, dataPath, baseMetaPath);

			List<String> aggCombinations = aggService.getAggregateScripts(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}
	@Test
	public void testGetAggregateScript_throwsAggException()
	{

		try
		{
			new MockUp<DataStatsUtil>()
			{

				@Mock
				public Level[] getDistinctDataFromDataStats(LoadModel loadModel) throws AggSuggestException
				{
					throw new AggSuggestException("error");
				}

			};
			
			MolapProperties.getInstance().addProperty(
					"molap.agg.benefit.ratio", "2");

			AutoAggSuggestionService aggService = AutoAggSuggestionFactory
					.getAggregateService(Request.QUERY_STATS);
			LoadModel loadModel = TestUtil.createLoadModel(schemaName,
					cubeName, schema, cube, dataPath, baseMetaPath);

			List<String> aggCombinations = aggService.getAggregateScripts(loadModel);
			Assert.assertTrue(false);
		}
		catch (AggSuggestException e)
		{
			Assert.assertTrue(true);
		}

	}

}
