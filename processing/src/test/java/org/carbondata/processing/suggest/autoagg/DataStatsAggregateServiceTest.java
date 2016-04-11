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

package org.carbondata.processing.suggest.autoagg;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;

import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.store.StoreCreator;
import org.carbondata.processing.suggest.autoagg.exception.AggSuggestException;
import org.carbondata.processing.suggest.autoagg.model.Request;
import org.carbondata.processing.suggest.autoagg.util.CommonUtil;
import org.carbondata.processing.suggest.datastats.analysis.QueryDistinctData;
import org.carbondata.processing.suggest.datastats.model.DriverDistinctData;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.processing.suggest.datastats.util.AggCombinationGeneratorUtil;
import org.carbondata.processing.suggest.util.TestUtil;
import org.carbondata.query.querystats.Preference;
import org.junit.Before;
import org.junit.Test;

public class DataStatsAggregateServiceTest {
    static CarbonDef.Schema schema;
    static CarbonDef.Cube cube;

    static String schemaName;
    static String cubeName;
    static String factTable;

    static String dataPath;
    static String baseMetaPath;
    private LoadModel loadModel;

    @Before
    public void setUpBeforeClass() throws Exception {
        try {

            StoreCreator.createCarbonStore();
            File file = new File("src/test/resources");
            String basePath = file.getCanonicalPath() + "/";
            String metaPath = basePath + "schemas/default/carbon/metadata";

            CarbonProperties.getInstance().addProperty("carbon.storelocation", basePath + "store");
            CarbonProperties.getInstance().addProperty("carbon.number.of.cores", "4");
            CarbonProperties.getInstance().addProperty("carbon.agg.benefitRatio", "10");
            CarbonProperties.getInstance().addProperty(Preference.AGG_LOAD_COUNT, "2");
            CarbonProperties.getInstance().addProperty(Preference.AGG_FACT_COUNT, "2");
            CarbonProperties.getInstance().addProperty(Preference.AGG_REC_COUNT, "5");
            schema = CommonUtil.readMetaData(metaPath).get(0);
            cube = schema.cubes[0];
            schemaName = schema.name;
            cubeName = cube.name;
            factTable = "carbon";
            // dataPath="src/test/resources/store/store";
            // baseMetaPath="src/test/resources/store/schemas";
            dataPath = basePath + "store";
            baseMetaPath = basePath + "schemas/default/carbon";
            loadModel = TestUtil.createLoadModel(schemaName, cubeName, schema, cube, dataPath,
                    baseMetaPath);
            CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
            CommonUtil.fillSchemaAndCubeDetail(loadModel);
        } catch (Exception e) {

        }

    }

    @Test
    public void testDataStats_distinctRelNotSerialized_getDimensions() {

        try {
            // LoadModel loadModel
            // =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
            // delete if data stats serialized file exist
            File dataStatsPath = new File(TestUtil.getDistinctDataPath(loadModel));
            if (dataStatsPath.exists()) {
                dataStatsPath.delete();
            }

            // delete if data stats aggregate combination file exist
            File dataStatsPathAggCombination =
                    new File(TestUtil.getDataStatsAggCombination(schemaName, cubeName, factTable));
            if (dataStatsPathAggCombination.exists()) {
                dataStatsPathAggCombination.delete();
            }

            AutoAggSuggestionService aggService =
                    AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);

            List<String> aggCombinations = aggService.getAggregateDimensions(loadModel);
            if (aggCombinations.size() > 0) {
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
            }

        } catch (Exception e) {
            Assert.assertTrue(false);
        }

    }

    @Test
    public void testDataStats_distinctRelIsSerialized_getDimensions() {
        try {
            AutoAggSuggestionService aggService =
                    AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);
            // LoadModel loadModel
            // =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
            List<String> aggCombinations = aggService.getAggregateDimensions(loadModel);
            if (aggCombinations.size() > 0) {
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

    }

    @Test
    public void testDataStats_distinctRelNotSerialized_getScript() {

        try {
            // LoadModel loadModel
            // =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
            // delete if serialized file exist
            File dataStatsPath = new File(TestUtil.getDistinctDataPath(loadModel));
            if (dataStatsPath.exists()) {
                dataStatsPath.delete();
            }

            AutoAggSuggestionService aggService =
                    AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);

            List<String> aggCombinations = aggService.getAggregateScripts(loadModel);
            if (aggCombinations.size() > 0) {
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

    }

    @Test
    public void testDataStats_distinctRelIsSerialized_getScript() {
        try {

            AutoAggSuggestionService aggService =
                    AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);
            // LoadModel loadModel
            // =TestUtil.createLoadModel(schemaName,cubeName,schema,cube,dataPath,baseMetaPath);
            List<String> aggCombinations = aggService.getAggregateScripts(loadModel);
            if (aggCombinations.size() > 0) {
                Assert.assertTrue(true);
            } else {
                Assert.assertTrue(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);

        }

    }

    @Test
    public void testGetAggregateDimensions_throwsException() {

        File dataStatsPath = new File(TestUtil.getDistinctDataPath(loadModel));
        if (dataStatsPath.exists()) {
            dataStatsPath.delete();
        }

        AutoAggSuggestionService aggService =
                AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);
        try {

            new MockUp<QueryDistinctData>() {

                @Mock
                public Level[] queryDistinctData(String partitionId) throws AggSuggestException {
                    throw new AggSuggestException("error", new NullPointerException());
                }

            };
            aggService.getAggregateDimensions(loadModel);
            Assert.assertTrue(false);
        } catch (AggSuggestException e) {
            Assert.assertTrue(true);
        }
        try {

            aggService.getAggregateDimensions(null);
            Assert.assertTrue(false);
        } catch (AggSuggestException e) {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testGetAggregateScript_throwsException() {
        File dataStatsPath = new File(TestUtil.getDistinctDataPath(loadModel));
        if (dataStatsPath.exists()) {
            dataStatsPath.delete();
        }

        AutoAggSuggestionService aggService =
                AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);

        try {

            new MockUp<QueryDistinctData>() {

                @Mock
                public Level[] queryDistinctData(String partitionId) throws AggSuggestException {
                    throw new AggSuggestException("error", new NullPointerException());
                }

            };
            aggService.getAggregateScripts(loadModel);
            Assert.assertTrue(false);
        } catch (AggSuggestException e) {
            Assert.assertTrue(true);
        }
        try {

            aggService.getAggregateScripts(null);
            Assert.assertTrue(false);
        } catch (AggSuggestException e) {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testNoVisibleLevel() {
        new MockUp<AggCombinationGeneratorUtil>() {

            @Mock
            public Level[] getVisibleLevels(Level[] aggLevels, CarbonDef.Cube cube) {
                return new Level[0];
            }

        };
    }

    @Test
    public void testSetCalculatedLoads() {
        new MockUp<DriverDistinctData>() {

            @Mock
            public List<String> getLoads() {
                List<String> loads = new ArrayList<String>();
                return loads;
            }

        };
        File dataStatsPath = new File(TestUtil.getDistinctDataPath(loadModel));
        if (dataStatsPath.exists()) {
            dataStatsPath.delete();
        }

        AutoAggSuggestionService aggService =
                AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);

        List<String> aggCombinations;
        try {
            aggCombinations = aggService.getAggregateScripts(loadModel);
            aggCombinations = aggService.getAggregateScripts(loadModel);
            Assert.assertNotNull(aggCombinations);
        } catch (AggSuggestException e) {
            Assert.assertTrue(false);
        }

    }

}
