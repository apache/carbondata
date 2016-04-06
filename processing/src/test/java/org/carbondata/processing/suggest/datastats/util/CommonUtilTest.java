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

package org.carbondata.processing.suggest.datastats.util;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;
import mockit.Mock;
import mockit.MockUp;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.processing.store.StoreCreator;
import org.carbondata.processing.suggest.autoagg.util.CommonUtil;
import org.carbondata.processing.suggest.datastats.model.LoadModel;
import org.carbondata.processing.suggest.util.TestUtil;
import org.eigenbase.xom.XOMException;
import org.junit.Before;
import org.junit.Test;

public class CommonUtilTest {
    static CarbonDef.Schema schema;
    static CarbonDef.Cube cube;

    static String schemaName;
    static String cubeName;
    static String factTable;

    static String dataPath;
    static String baseMetaPath;
    private LoadModel loadModel;
    private String metaPath;

    @Before
    public void setUpBeforeClass() throws Exception {
        StoreCreator.createCarbonStore();
        File file = new File("src/test/resources");
      
        String basePath = file.getCanonicalPath() + "/";
        metaPath = basePath + "schemas/default/carbon/metadata";

        schema = CommonUtil.readMetaData(metaPath).get(0);
        cube = schema.cubes[0];
        schemaName = schema.name;
        cubeName = cube.name;
        factTable = "carbon";
        // dataPath="src/test/resources/store/store";
        // baseMetaPath="src/test/resources/store/schemas";
        dataPath = basePath + "store";
        baseMetaPath = basePath + "schemas/default/carbon";

        dataPath = basePath + "store";
        baseMetaPath = basePath + "schemas/default/carbon";
        loadModel = TestUtil.createLoadModel(schemaName, cubeName, schema, cube, dataPath,
                baseMetaPath);

    }

    @Test
    public void testsetListOfValidSlices_updateLoad() {
        new MockUp<LoadMetadataDetails>() {

            @Mock
            public String getLoadStatus() {
                return CarbonCommonConstants.MARKED_FOR_UPDATE;
            }

        };

        CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
        Assert.assertTrue(true);
    }

    @Test
    public void testsetListOfValidSlices_partitalSuccess() {
        new MockUp<LoadMetadataDetails>() {

            @Mock
            public String getLoadStatus() {
                return CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS;
            }

        };

        CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
        Assert.assertTrue(true);
    }

    @Test
    public void testsetListOfValidSlices_throwException() {
        new MockUp<java.io.DataInputStream>() {

            @Mock
            public void close() throws IOException {
                throw new IOException();
            }

        };

        CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
        Assert.assertTrue(true);
    }

    @Test
    public void testsetListOfValidSlices_IOException() {
        new MockUp<FileFactory>() {

            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType)
                    throws IOException {
                throw new IOException();
            }

        };

        CommonUtil.setListOfValidSlices(baseMetaPath, loadModel);
        Assert.assertTrue(true);

    }

    @Test
    public void testReadMetaData_FileNotExist() {

        Assert.assertNull(CommonUtil.readMetaData("test/resource"));
    }

    @Test
    public void testReadMetaData_nullSource() {
        Assert.assertNull(CommonUtil.readMetaData(null));
    }

    @Test
    public void testReadMetaData_throwIOException() {
        new MockUp<java.io.DataInputStream>() {

            @Mock
            public void readFully(byte b[]) throws IOException {
                throw new IOException();
            }

        };
        Assert.assertNull(CommonUtil.readMetaData(metaPath));
    }

    @Test
    public void testReadMetaData_throwXOMException() {
        new MockUp<CommonUtil>() {

            @Mock
            public CarbonDef.Schema parseStringToSchema(String schema) throws XOMException {
                throw new XOMException();
            }

        };
        Assert.assertNull(CommonUtil.readMetaData(metaPath));
    }

}
