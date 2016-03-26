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

package org.carbondata.integration.spark.javatestsuite.detailquery;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.core.metadata.MolapMetadata;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.integration.spark.common.util.*;
import org.carbondata.integration.spark.load.MolapLoadModel;
import org.carbondata.integration.spark.query.MolapQueryPlan;
import org.carbondata.integration.spark.query.metadata.MolapMeasure;
import org.carbondata.integration.spark.query.metadata.SortOrderType;
import org.carbondata.query.executer.MolapQueryExecutorModel;
import org.carbondata.query.expression.DataType;
import org.carbondata.query.result.RowResult;
import org.junit.*;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AllDataTypesTestCase {

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

        CreateCubeUtils.createCube("default", "alldatatypescube", "empname");
        MolapLoadModel model =
                LoadDataUtils.prepareLoadModel("default", "alldatatypescube", 0 + "");
        LoadDataUtils.loadCube(model, 0);
        QueryPlanUtils.setSchemaCube("default", "alldatatypescube");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        System.out.println("tearing down");
        DropCubeUtils.dropCube("default", "alldatatypescube");
    }

    @Before
    public void setUp() throws Exception {
        // If some per condition to be run before each test method, add here
    }

    @Test
    public void testFilterAndAggregates() throws Exception {

        //Preparation
        MolapQueryPlan plan = QueryPlanUtils.createQueryPlan();

        int queryOrder = 0;
        QueryPlanUtils.addDimension(plan, "empno", queryOrder);
        queryOrder = queryOrder + 1;

        QueryPlanUtils.addDimensionWithSort(plan, "empname", SortOrderType.DSC, queryOrder);
        queryOrder = queryOrder + 1;

        QueryPlanUtils.addMeasure(plan, "utilization", queryOrder);
        queryOrder = queryOrder + 1;

        QueryPlanUtils
                .addMeasureWithAgg(plan, "salary", MolapMeasure.AggregatorType.COUNT, queryOrder);
        queryOrder = queryOrder + 1;

        QueryPlanUtils.addAggOnDimension(plan, "empno", "sum", queryOrder);
        queryOrder = queryOrder + 1;

        QueryPlanUtils.setQueryLimit(plan, 100);

        QueryPlanUtils.isDetailedQuery(plan, false);

        MolapMetadata.Cube cube = MolapMetadata.getInstance()
                .getCube(plan.getSchemaName() + "_" + plan.getCubeName());
        List<Object> values = new LinkedList<Object>();
        values.add("arvind");
        values.add("ayushi");
        QueryPlanUtils.addInFilter(plan, "empname", values, cube.getDimension("empname"),
                DataType.StringType);

        MolapQueryExecutorModel model = QueryUtils.createQueryModel(plan);

        //Execution
        MolapIterator<RowResult> rowIterator = QueryUtils.runQuery(model);

        //Validation
        List<String> expectedOutput = new LinkedList<String>();
        expectedOutput.add("11,arvind,96.2,1.0,11.0");
        expectedOutput.add("15,ayushi,91.5,1.0,15.0");

        QueryUtils.validateQueryOutput(rowIterator, expectedOutput);
    }

    @Test
    public void testCountOnDimension() throws Exception {
        //select dept,count(dept) from cube group by dept

        //Preparation
        MolapQueryPlan plan = QueryPlanUtils.createQueryPlan();

        int queryOrder = 0;

        QueryPlanUtils.addDimension(plan, "deptname", queryOrder);
        queryOrder = queryOrder + 1;

        QueryPlanUtils.addAggOnDimension(plan, "deptname", "count", queryOrder);
        queryOrder = queryOrder + 1;

        QueryPlanUtils.isDetailedQuery(plan, false);

        MolapQueryExecutorModel model = QueryUtils.createQueryModel(plan);

        //Execution
        MolapIterator<RowResult> rowIterator = QueryUtils.runQuery(model);

        //Validation
        List<String> expectedOutput = new LinkedList<String>();
        expectedOutput.add("network,3.0");
        expectedOutput.add("protocol,2.0");
        expectedOutput.add("configManagement,1.0");
        expectedOutput.add("security,2.0");
        expectedOutput.add("Learning,2.0");

        QueryUtils.validateQueryOutput(rowIterator, expectedOutput);
    }

    @After
    public void tearDown() throws Exception {
        // If some post condition to be run after each test method, add here
    }
}