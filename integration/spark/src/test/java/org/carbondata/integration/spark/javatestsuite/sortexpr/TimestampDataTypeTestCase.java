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

package org.carbondata.integration.spark.javatestsuite.sortexpr;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.integration.spark.common.util.*;
import org.carbondata.integration.spark.load.MolapLoadModel;
import org.carbondata.integration.spark.query.MolapQueryPlan;
import org.carbondata.query.executer.MolapQueryExecutorModel;
import org.carbondata.query.result.RowResult;
import org.junit.*;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TimestampDataTypeTestCase {

    /**
     * Runs before class execution starts
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Configuration hadoopConf = new Configuration();
        hadoopConf.addResource(new Path("../core-default.xml"));
        hadoopConf.addResource(new Path("core-site.xml"));

        String hdfsCarbonPath = hadoopConf.get("fs.defaultFS", "./") + "/opt/carbon/test/";

        MolapProperties.getInstance()
                .addProperty(MolapCommonConstants.STORE_LOCATION_HDFS, hdfsCarbonPath);
        MolapProperties.getInstance()
                .addProperty(MolapCommonConstants.STORE_LOCATION, hdfsCarbonPath);
        MolapProperties.getInstance().addProperty(MolapCommonConstants.MOLAP_BADRECORDS_LOC,
                hdfsCarbonPath + "/badrecords");
        MolapProperties.getInstance().addProperty(MolapCommonConstants.STORE_LOCATION_TEMP_PATH,
                System.getProperty("java.io.tmpdir"));
        MolapProperties.getInstance()
                .addProperty(MolapCommonConstants.MOLAP_TIMESTAMP_FORMAT, "dd-MM-yyyy");
        MolapProperties.getInstance().addProperty("molap.kettle.home",
                "../../Molap/Molap-Data-Processor/molapplugins/molapplugins");
        MolapProperties.getInstance().addProperty("molap.testdata.path", "./TestData/");

        CreateCubeUtils.createCube("default", "timestamptypecube", "empno");
        MolapLoadModel model =
                LoadDataUtils.prepareLoadModel("default", "timestamptypecube", 0 + "");
        LoadDataUtils.loadCube(model, 0);
        QueryPlanUtils.setSchemaCube("default", "timestamptypecube");
    }

    /**
     * Runs After class execution Ends
     *
     * @throws Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        System.out.println("tearing down");
        DropCubeUtils.dropCube("default", "timestamptypecube");
    }

    @Before
    public void setUp() throws Exception {
        // If some per condition to be run before each test method, add here
    }

    @Test
    public void testSelectDimension() throws Exception {

        //select doj from cube

        //Preparation
        MolapQueryPlan plan = QueryPlanUtils.createQueryPlan();

        int queryOrder = 0;

        QueryPlanUtils.addDimension(plan, "doj", queryOrder);
        queryOrder = queryOrder + 1;

        MolapQueryExecutorModel model = QueryUtils.createQueryModel(plan);

        //Execution
        MolapIterator<RowResult> rowIterator = QueryUtils.runQuery(model);

        //Validation
        List<String> expectedOutput = new LinkedList<String>();
        expectedOutput.add("17-01-2007");
        expectedOutput.add("29-05-2008");
        expectedOutput.add("07-07-2009");
        expectedOutput.add("29-12-2010");
        expectedOutput.add("09-11-2011");
        expectedOutput.add("14-10-2012");
        expectedOutput.add("22-09-2013");
        expectedOutput.add("15-08-2014");
        expectedOutput.add("12-05-2015");
        expectedOutput.add("01-12-2015");

        QueryUtils.validateTimestampQueryOutput(rowIterator, expectedOutput);
    }

    @After
    public void tearDown() throws Exception {
        // If some post condition to be run after each test method, add here
    }
}