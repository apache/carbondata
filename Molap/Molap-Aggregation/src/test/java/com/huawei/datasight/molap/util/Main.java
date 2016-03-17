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

package com.huawei.datasight.molap.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.huawei.datasight.molap.autoagg.AutoAggSuggestionFactory;
import com.huawei.datasight.molap.autoagg.AutoAggSuggestionService;
import com.huawei.datasight.molap.autoagg.exception.AggSuggestException;
import com.huawei.datasight.molap.autoagg.model.Request;
import com.huawei.datasight.molap.datastats.LoadSampler;
import com.huawei.datasight.molap.datastats.load.LevelMetaInfo;
import com.huawei.datasight.molap.datastats.model.LoadModel;
import com.huawei.datasight.molap.datastats.util.DataStatsUtil;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.aggregator.dimension.DimensionAggregatorInfo;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.executer.QueryExecutor;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.holders.MolapResultHolder;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.engine.result.RowResult;
import com.huawei.unibi.molap.filter.MolapFilterInfo;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.metadata.MolapMetadata;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.SqlStatement.Type;
import com.huawei.unibi.molap.util.MolapProperties;

public class Main
{

	public static void main(String[] args) throws QueryExecutionException
	{

		/*long a=Long.parseLong("00111111111111111111111111111111111111111111111111101011010000101",2);
		long b=Long.parseLong(  "00011111111111111111111111111111111111111111111111101011010000101",2);
		System.out.print(Long.toBinaryString(a-b));*/
		
		// utility();

		String basePath = null, metaPath = null;

		
		 basePath="hdfs://10.19.92.135:54310/VmallData/VmallStore/";
		 metaPath=basePath+"schemas/default/Vmall_user_prof1/metadata";
		 

		/*basePath = "D:/githuawei/spark_cube/CI/FTScenarios/Small/store/";
		metaPath = basePath + "schemas/default/Small/metadata";*/

		
		  basePath="D:/githuawei/spark_cube/CI/FTScenarios/DynCar/store/";
		  metaPath=basePath+"schemas/default/Carbon_DR_FT/metadata";
		 
		/*
		 * basePath="/opt/ashok/test/DynCar/";
		 * metaPath=basePath+"schemas/default/Carbon_DR_FT/metadata";
		 */

		MolapProperties.getInstance().addProperty("molap.storelocation",
				basePath + "store");
		// MolapProperties.getInstance().addProperty("molap.schemaslocation",
		// basePath+"schemas");
		MolapProperties.getInstance().addProperty("molap.number.of.cores", "4");
		MolapProperties.getInstance().addProperty(
				"molap.smartJump.avoid.percent", "70");
		MolapProperties.getInstance().addProperty(
				Preference.AGG_LOAD_COUNT, "4");
		MolapProperties.getInstance().addProperty(
				Preference.AGG_FACT_COUNT, "2");
		MolapProperties.getInstance().addProperty(
				Preference.AGG_REC_COUNT, "5");
		//MolapProperties.getInstance().addProperty("aggregate.columnar.keyblock","false");

		AutoAggSuggestionService aggServer =AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);
		MolapDef.Schema schema = TestUtil.readMetaData(metaPath).get(0);
		MolapDef.Cube cube = schema.cubes[0];

		
		LoadModel loadModel=TestUtil.createLoadModel(schema.name, cube.name, schema, cube,basePath+"store",basePath+"schemas/default/Carbon_DR_FT/");
		List<String> combs;
		try {
			combs = aggServer.getAggregateDimensions(loadModel);
//			List<String> combs=aggServer.getAggregateScripts(loadModel);
			for(String comb:combs)
			{
				System.out.println(comb);
			}

		} catch (AggSuggestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		// serialize(dimCos);
		//executeQuery(schema,cube);

	}
	
	public static void executeQuery(MolapDef.Schema schema,MolapDef.Cube cube) throws QueryExecutionException
	{
		LoadSampler loadSampler=new LoadSampler();
		LoadModel loadModel = new LoadModel();

		// Schema schema = AggregateUtil.readMetaData(schemaPath).get(0);
		loadModel.setTableName(cube.fact.getAlias());
		loadModel.setSchema(schema);
		loadModel.setCube(cube);

		loadModel.setPartitionId("0");

		
	//	loadSampler.loadCube(loadModel);
		
		MolapQueryExecutorModel model=createQueryExecutorModel(loadSampler);
		QueryExecutor queryExecutor= DataStatsUtil.getQueryExecuter(loadSampler.getMetaCube(),
				cube.fact.getAlias());
		MolapIterator<RowResult> rowIterator =queryExecutor.execute(model);
		System.out.println("hello");
	}

	public static MolapQueryExecutorModel createQueryExecutorModel(LoadSampler loadSampler)
	{
		Cube cube=loadSampler.getMetaCube();
		MolapQueryExecutorModel executorModel = new MolapQueryExecutorModel();
		executorModel.setSparkExecution(true);
		String factTableName = cube.getFactTableName();
		executorModel.setCube(cube);
        executorModel.sethIterator(new MolapResultHolder(new ArrayList<Type>()));
        executorModel.setFactTable(factTableName);
        
        List<Dimension> allDimensions = cube.getDimensions(loadSampler.getTableName());
        Dimension[] dimensions=new Dimension[1];
        dimensions[0]=allDimensions.get(49);
        dimensions[0].setQueryOrder(0);
        executorModel.setDims(dimensions);
        executorModel.setMsrs(new ArrayList<Measure>());
        
      
    		
    		
    		
     					
           
           List<DimensionAggregatorInfo> dimensionAggregatorInfos= new ArrayList<DimensionAggregatorInfo>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
           
           executorModel.setConstraints(new HashMap<MolapMetadata.Dimension, MolapFilterInfo>());
           executorModel.setDimensionAggInfo(dimensionAggregatorInfos);
           executorModel.setActualDimsRows(executorModel.getDims());
           executorModel.setActualDimsCols(new Dimension[0]);
           executorModel.setCalcMeasures(new ArrayList<Measure>());
           executorModel.setAnalyzerDims(executorModel.getDims());
           executorModel.setConstraintsAfterTopN(new HashMap<MolapMetadata.Dimension, MolapFilterInfo>());
           executorModel.setLimit(-1);
           executorModel.setDetailQuery(false);
           executorModel.setQueryId(System.nanoTime() + "");
           executorModel.setOutLocation(MolapProperties.getInstance()
   				.getProperty(MolapCommonConstants.STORE_LOCATION_HDFS));
           return executorModel;
	}


	public static void utility()
	{
		for (int i = 0; i < 5; i++)
		{
			String path = "hdfs://10.19.92.135:54310//VmallData/VmallStore/store/default_0/Vmall_user_prof1_0/RS_0/Vmall_FACT/Load_"
					+ i;
			MolapFile file = FileFactory.getMolapFile(path,
					FileFactory.getFileType(path));
			LevelMetaInfo level = new LevelMetaInfo(file, "Vmall_FACT");
			int[] data = level.getDimCardinality();
			System.out.println("Load_" + i + ":" + Arrays.toString(data));

		}
		System.exit(0);
		/*
		 * Arrays.sort(data); int max=data[data.length-1]; for(int d:data) {
		 * double res=(double)(100*d)/max; System.out.println(d+"->"+res); }
		 */

	}
}
