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

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.store.StoreCreator;
import org.carbondata.processing.suggest.autoagg.util.CommonUtil;
import org.carbondata.processing.suggest.datastats.load.FactDataHandler;
import org.carbondata.processing.suggest.datastats.load.LevelMetaInfo;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.processing.suggest.util.TestUtil;
import org.carbondata.query.querystats.Preference;
import org.junit.Before;
import org.junit.Test;

public class FactDataHandlerTest {

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
    public void testFactDataHandler() {

        new MockUp<CarbonMetadata.Cube>() {

            @Mock
            public String getMode() {
                return "test";
            }

        };
        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.CARBON_IS_LOAD_FACT_TABLE_IN_MEMORY, "false");
        CarbonMetadata.getInstance().loadCube(schema, schema.name, cube.name, cube);
        CarbonMetadata.Cube metaCube =
                CarbonMetadata.getInstance().getCube(schema.name + "_" + cube.name);
        String levelMetaPath = dataPath + "/default_0/carbon_0/RS_0/carbon/Load_0";
        CarbonFile file =
                FileFactory.getCarbonFile(levelMetaPath, FileFactory.getFileType(levelMetaPath));

        LevelMetaInfo levelMetaInfo = new LevelMetaInfo(file, loadModel.getTableName());
        FactDataHandler factDataHandler =
                new FactDataHandler(metaCube, levelMetaInfo, loadModel.getTableName(), 1, null);
        Assert.assertTrue(true);

    }
}
