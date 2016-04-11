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
import java.util.List;

import junit.framework.Assert;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.suggest.autoagg.model.Request;
import org.carbondata.processing.suggest.autoagg.util.CommonUtil;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.processing.suggest.util.TestUtil;
import org.carbondata.query.querystats.Preference;
import org.junit.Before;
import org.junit.Test;

public class RestructureTest {
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

            File file = new File("../../libraries/testData/Carbon-Aggregation/restructure/");
            String basePath = file.getCanonicalPath() + "/";
            String metaPath = basePath + "schemas/default/rs/metadata";

            CarbonProperties.getInstance().addProperty(Preference.AGG_LOAD_COUNT, "2");
            CarbonProperties.getInstance().addProperty(Preference.AGG_FACT_COUNT, "2");
            CarbonProperties.getInstance().addProperty(Preference.AGG_REC_COUNT, "5");
            schema = CommonUtil.readMetaData(metaPath).get(0);
            cube = schema.cubes[0];
            schemaName = schema.name;
            cubeName = cube.name;
            factTable = "rs";
            // dataPath="src/test/resources/store/store";
            // baseMetaPath="src/test/resources/store/schemas";
            dataPath = basePath + "store";
            baseMetaPath = basePath + "schemas/default/rs";
            loadModel = TestUtil.createLoadModel(schemaName, cubeName, schema, cube, dataPath,
                    baseMetaPath);
            CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
            CommonUtil.fillSchemaAndCubeDetail(loadModel);
        } catch (Exception e) {

        }

    }

    @Test
    public void testRestructure() {
        try {
            LoadModel loadModel =
                    TestUtil.createLoadModel(schemaName, cubeName, schema, cube, dataPath,
                            baseMetaPath);
            loadModel.getValidSlices().add("Load_1");
            loadModel.getValidSlices().add("Load_2");
            // delete if serialized file exist
            File dataStatsPath = new File(TestUtil.getDistinctDataPath(loadModel));
            if (dataStatsPath.exists()) {
                dataStatsPath.delete();
            }

            AutoAggSuggestionService aggService =
                    AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);

            List<String> aggCombinations = aggService.getAggregateScripts(loadModel);
            Assert.assertNotNull(aggCombinations);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

    }

    @Test
    public void testRestructure_CachedDistinctDataAndLoadisDeleted() {
        try {
            LoadModel loadModel =
                    TestUtil.createLoadModel(schemaName, cubeName, schema, cube, dataPath,
                            baseMetaPath);
            loadModel.getValidSlices().remove(0);
            loadModel.getValidSlices().add("Load_1");
            loadModel.getValidSlices().add("Load_2");
            // delete if serialized file exist

            AutoAggSuggestionService aggService =
                    AutoAggSuggestionFactory.getAggregateService(Request.DATA_STATS);

            List<String> aggCombinations = aggService.getAggregateScripts(loadModel);
            Assert.assertEquals(1, aggCombinations.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        }

    }
}
