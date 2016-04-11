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

package org.carbondata.processing.suggest.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.suggest.autoagg.AutoAggSuggestionFactory;
import org.carbondata.processing.suggest.autoagg.AutoAggSuggestionService;
import org.carbondata.processing.suggest.autoagg.exception.AggSuggestException;
import org.carbondata.processing.suggest.autoagg.model.Request;
import org.carbondata.processing.suggest.datastats.LoadSampler;
import org.carbondata.processing.suggest.datastats.load.LevelMetaInfo;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.processing.suggest.datastats.util.DataStatsUtil;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.executer.QueryExecutor;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.holders.CarbonResultHolder;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;
import org.carbondata.query.querystats.Preference;
import org.carbondata.query.result.RowResult;

public class Main {

    public static void main(String[] args) throws QueryExecutionException {

		/*long a=Long.parseLong("00111111111111111111111111111111111111111111111111101011010000101",2);
        long b=Long.parseLong(  "00011111111111111111111111111111111111111111111111101011010000101",2);
		System.out.print(Long.toBinaryString(a-b));*/

        // utility();

        String basePath = null, metaPath = null;

        basePath = "hdfs://10.19.92.135:54310/VmallData/VmallStore/";
        metaPath = basePath + "schemas/default/Vmall_user_prof1/metadata";
		 

		/*basePath = "D:/githuawei/spark_cube/CI/FTScenarios/Small/store/";
		metaPath = basePath + "schemas/default/Small/metadata";*/

        basePath = "D:/githuawei/spark_cube/CI/FTScenarios/DynCar/store/";
        metaPath = basePath + "schemas/default/Carbon_DR_FT/metadata";
		 
		/*
		 * basePath="/opt/ashok/test/DynCar/";
		 * metaPath=basePath+"schemas/default/Carbon_DR_FT/metadata";
		 */

        CarbonProperties.getInstance().addProperty("carbon.storelocation", basePath + "store");
        // CarbonProperties.getInstance().addProperty("carbon.schemaslocation",
        // basePath+"schemas");
        CarbonProperties.getInstance().addProperty("carbon.number.of.cores", "4");
        CarbonProperties.getInstance().addProperty("carbon.smartJump.avoid.percent", "70");
        CarbonProperties.getInstance().addProperty(Preference.AGG_LOAD_COUNT, "4");
        CarbonProperties.getInstance().addProperty(Preference.AGG_FACT_COUNT, "2");
        CarbonProperties.getInstance().addProperty(Preference.AGG_REC_COUNT, "5");
        //CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock","false");

        AutoAggSuggestionService aggServer =
                AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);
        CarbonDef.Schema schema = TestUtil.readMetaData(metaPath).get(0);
        CarbonDef.Cube cube = schema.cubes[0];

        LoadModel loadModel =
                TestUtil.createLoadModel(schema.name, cube.name, schema, cube, basePath + "store",
                        basePath + "schemas/default/Carbon_DR_FT/");
        List<String> combs;
        try {
            combs = aggServer.getAggregateDimensions(loadModel);
            //			List<String> combs=aggServer.getAggregateScripts(loadModel);
            for (String comb : combs) {
                System.out.println(comb);
            }

        } catch (AggSuggestException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // serialize(dimCos);
        //executeQuery(schema,cube);

    }

    public static void executeQuery(CarbonDef.Schema schema, CarbonDef.Cube cube)
            throws QueryExecutionException {
        LoadSampler loadSampler = new LoadSampler();
        LoadModel loadModel = new LoadModel();

        // Schema schema = AggregateUtil.readMetaData(schemaPath).get(0);
        loadModel.setTableName(cube.fact.getAlias());
        loadModel.setSchema(schema);
        loadModel.setCube(cube);

        loadModel.setPartitionId("0");

        //	loadSampler.loadCube(loadModel);

        CarbonQueryExecutorModel model = createQueryExecutorModel(loadSampler);
        QueryExecutor queryExecutor = DataStatsUtil
                .getQueryExecuter(loadSampler.getMetaCube(), cube.fact.getAlias(),
                        loadSampler.getQueryScopeObject());
        CarbonIterator<RowResult> rowIterator = queryExecutor.execute(model);
        System.out.println("hello");
    }

    public static CarbonQueryExecutorModel createQueryExecutorModel(LoadSampler loadSampler) {
        CarbonMetadata.Cube cube = loadSampler.getMetaCube();
        CarbonQueryExecutorModel executorModel = new CarbonQueryExecutorModel();
        executorModel.setSparkExecution(true);
        String factTableName = cube.getFactTableName();
        executorModel.setCube(cube);
        executorModel.sethIterator(new CarbonResultHolder(new ArrayList<SqlStatement.Type>()));
        executorModel.setFactTable(factTableName);

        List<CarbonMetadata.Dimension> allDimensions =
                cube.getDimensions(loadSampler.getTableName());
        CarbonMetadata.Dimension[] dimensions = new CarbonMetadata.Dimension[1];
        dimensions[0] = allDimensions.get(49);
        dimensions[0].setQueryOrder(0);
        executorModel.setDims(dimensions);
        executorModel.setMsrs(new ArrayList<CarbonMetadata.Measure>());

        List<DimensionAggregatorInfo> dimensionAggregatorInfos =
                new ArrayList<DimensionAggregatorInfo>(
                        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        executorModel.setConstraints(new HashMap<CarbonMetadata.Dimension, CarbonFilterInfo>());
        executorModel.setDimensionAggInfo(dimensionAggregatorInfos);
        executorModel.setActualDimsRows(executorModel.getDims());
        executorModel.setActualDimsCols(new CarbonMetadata.Dimension[0]);
        executorModel.setCalcMeasures(new ArrayList<CarbonMetadata.Measure>());
        executorModel.setAnalyzerDims(executorModel.getDims());
        executorModel
                .setConstraintsAfterTopN(new HashMap<CarbonMetadata.Dimension, CarbonFilterInfo>());
        executorModel.setLimit(-1);
        executorModel.setDetailQuery(false);
        executorModel.setQueryId(System.nanoTime() + "");
        executorModel.setOutLocation(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS));
        return executorModel;
    }

    public static void utility() {
        for (int i = 0; i < 5; i++) {
            String path =
                    "hdfs://10.19.92.135:54310//VmallData/VmallStore/store/default_0/Vmall_user_prof1_0/RS_0/Vmall_FACT/Load_"
                            + i;
            CarbonFile file = FileFactory.getCarbonFile(path, FileFactory.getFileType(path));
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
