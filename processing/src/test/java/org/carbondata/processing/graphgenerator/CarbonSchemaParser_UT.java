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

/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 */
package org.carbondata.processing.graphgenerator;

import junit.framework.TestCase;
import org.junit.Test;

/**
 *
 * @author K00900841
 *
 */
public class CarbonSchemaParser_UT extends TestCase {

    @Test
    public void test() {
        assertTrue(true);
    }
    //    private Schema schema;
    //
    //    private Cube mondrianCubes;
    //
    //    @BeforeClass
    //    public void setUp()
    //    {
    //        File file = new File("");
    //        schema = CarbonSchemaParser.loadXML(file.getAbsolutePath() + File.separator + "test" + File.separator
    //                + "resources" + File.separator + "Vishal5SecondsTest1.xml");
    //        mondrianCubes = CarbonSchemaParser.getMondrianCubes(schema);
    //        assertNotNull(mondrianCubes);
    //    }
    //
    //    @AfterClass
    //    public void tearDown()
    //    {
    //        schema = null;
    //        mondrianCubes = null;
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetFactTableName_Output_FactTableNameInSchema()
    //    {
    //        String factTableName = CarbonSchemaParser.getFactTableName(mondrianCubes);
    //        assertEquals("DATA_FACT", factTableName);
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetCubeDimensions_Output_StringArrayOfDimension_WithOutProperty_OrdianlColumn_NameColumn()
    //    {
    //        String[] cubeDimensions = CarbonSchemaParser.getCubeDimensions(mondrianCubes);
    //        assertEquals(Arrays.toString(cubeDimensions),
    //                "[DEVICE_NAME, YEAR_ID, MONTH_NAME, DAY_ID, HOUR_ID, MINUTE_ID, COUNTRY, STATE, CITY, CATEGORY_NAME, PROTOCOL_NAME]");
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetDimensionString_Output_DimensionString_With_ColumnIndex_And_Cardinality()
    //    {
    //        StringBuilder dimString = new StringBuilder();
    //        int count = CarbonSchemaParser.getDimensionString(mondrianCubes.dimensions, dimString, 0);
    //        assertEquals(count, 11);
    //        assertEquals(
    //                "DEVICE_NAME:0:20,YEAR_ID:1:2,MONTH_NAME:2:12,DAY_ID:3:31,HOUR_ID:4:24,MINUTE_ID:5:4,COUNTRY:6:5,STATE:7:10,CITY:8:20,CATEGORY_NAME:9:5,PROTOCOL_NAME:10:20,",
    //                dimString.toString());
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetMeasures_Output_StringArrayOfMeasures()
    //    {
    //        String[] measures = CarbonSchemaParser.getMeasures(mondrianCubes.measures);
    //        assertEquals(measures.length, 10);
    //        String expectedMeasures[] = {"MEASURE1", "MEASURE2", "MEASURE3", "MEASURE4", "MEASURE5", "MEASURE6",
    //                "MEASURE7", "MEASURE8", "MEASURE9", "MEASURE10"};
    //
    //        for(int i = 0;i < measures.length;i++)
    //        {
    //            if(!measures[i].equals(expectedMeasures[i]))
    //            {
    //                assertTrue(false);
    //                break;
    //            }
    //        }
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetPropertyString_Output_PropertyString_With_ColumnIndex_IfPropertyIsPresent()
    //    {
    //        StringBuilder dimString = new StringBuilder();
    //        int count = CarbonSchemaParser.getPropertyString(mondrianCubes.dimensions, dimString, 0);
    //        assertEquals(count, 0);
    //        assertTrue(dimString.length() == 0);
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetMeasureString_Output_MeasureString_With_MeasureOdrinal_And_ColumnIndex()
    //    {
    //        String measure = CarbonSchemaParser.getMeasureString(mondrianCubes.measures, 0);
    //        String[] split = measure.split(",");
    //        assertTrue(split.length == 10);
    //        assertEquals(
    //                "MEASURE1:0,MEASURE2:1,MEASURE3:2,MEASURE4:3,MEASURE5:4,MEASURE6:5,MEASURE7:6,MEASURE8:7,MEASURE9:8,MEASURE10:9",
    //                measure);
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetHeirarchyString_Output_HeirarchyName_With_ColumnIndex_OfLevels()
    //    {
    //        String hierarchyString = CarbonSchemaParser.getHierarchyString(mondrianCubes.dimensions);
    //        String[] split = hierarchyString.split("&");
    //        assertTrue(split.length == 3);
    //        assertEquals("Time:1,2,3,4,5&Location:6,7,8&Protocol:9,10", hierarchyString);
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetMetaHeirarchyString_Output_HeirarchyName_With_LevelName()
    //    {
    //        String metaHierarchyString = CarbonSchemaParser.getMetaHeirString(mondrianCubes.dimensions);
    //        String[] split = metaHierarchyString.split("&");
    //        assertTrue(split.length == 4);
    //        assertEquals(
    //                "Device:DEVICE_NAME&Time:YEAR_ID:MONTH_NAME:DAY_ID:HOUR_ID:MINUTE_ID&Location:COUNTRY:STATE:CITY&Protocol:CATEGORY_NAME:PROTOCOL_NAME",
    //                metaHierarchyString);
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetDimensionCardinalityMap_Output_LevelName_And_CardinalityMap()
    //    {
    //        Map<String, String> cardinalities = CarbonSchemaParser.getCardinalities(mondrianCubes.dimensions);
    //        String[] cubeDimensions = CarbonSchemaParser.getCubeDimensions(mondrianCubes);
    //        String[] dimensionsCardinality = {"20", "2", "12", "31", "24", "4", "5", "10", "20", "5", "20"};
    //        for(int i = 0;i < cubeDimensions.length;i++)
    //        {
    //            if(cardinalities.containsKey(cubeDimensions[i]))
    //            {
    //                if(cardinalities.get(cubeDimensions[i]).equals(dimensionsCardinality[i]))
    //                {
    //                    continue;
    //                }
    //                else
    //                {
    //                    assertTrue(false);
    //                }
    //            }
    //            else
    //            {
    //                assertTrue(false);
    //            }
    //        }
    //        assertTrue(cardinalities.size() == 11);
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetQueryForDimension_Output_DimensionQueryString()
    //    {
    //        String dimensionSQLQueries = CarbonSchemaParser.getDimensionSQLQueries(mondrianCubes.dimensions);
    //        assertEquals(
    //                "Device:SELECT DEVICE_NAME FROM VISHAL.DEVICE_DIM#Time:SELECT YEAR_ID,MONTH_NAME,DAY_ID,HOUR_ID,MINUTE_ID FROM VISHAL.TIME_DIM#Location:SELECT COUNTRY,STATE,CITY FROM VISHAL.LOCATION_DIM#Protocol:SELECT CATEGORY_NAME,PROTOCOL_NAME FROM VISHAL.PROTOCOL_DIM",
    //                dimensionSQLQueries);
    //        String[] split = dimensionSQLQueries.split("#");
    //        assertTrue(split.length == 4);
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetAggregateTables_Output_AggregateTablesArray()
    //    {
    //        AggregateTable[] aggregateTable = CarbonSchemaParser.getAggregateTable(mondrianCubes);
    //        assertTrue(aggregateTable.length == 1);
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetAggregateTableName_Output_AggregateTablesName()
    //    {
    //        AggregateTable[] aggregateTable = CarbonSchemaParser.getAggregateTable(mondrianCubes);
    //        assertTrue(aggregateTable.length == 1);
    //        assertEquals(aggregateTable[0].getAggregateTableName(), "agg_2_Dev_Year_State_Prot_Temp");
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetAggregateLevels_Output_AggregateTablesLevelsArray()
    //    {
    //        AggregateTable[] aggregateTable = CarbonSchemaParser.getAggregateTable(mondrianCubes);
    //        assertTrue(aggregateTable.length == 1);
    //        String[] aggLevels = aggregateTable[0].getAggLevels();
    //        assertTrue(aggLevels.length == 6);
    //        String[] expAggLevels = {"DEVICE_NAME", "YEAR_ID", "COUNTRY", "STATE", "CATEGORY_NAME", "PROTOCOL_NAME"};
    //        for(int i = 0;i < aggLevels.length;i++)
    //        {
    //            if(!aggLevels[i].equals(expAggLevels[i]))
    //            {
    //                assertTrue(false);
    //                break;
    //            }
    //        }
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetDimensionStringForAgg_Output_DimensionString_With_ColumnIndex_And_Cardinality_ForAggTable()
    //    {
    //        AggregateTable[] aggregateTable = CarbonSchemaParser.getAggregateTable(mondrianCubes);
    //        assertTrue(aggregateTable.length == 1);
    //        String[] aggLevels = aggregateTable[0].getAggLevels();
    //        assertTrue(aggLevels.length == 6);
    //        StringBuilder builder = new StringBuilder();
    //        int dimensionStringForAgg = CarbonSchemaParser.getDimensionStringForAgg(aggLevels, builder, 0,
    //                CarbonSchemaParser.getCardinalities(mondrianCubes.dimensions));
    //        assertTrue(dimensionStringForAgg == 6);
    //        assertEquals("DEVICE_NAME:0:20,YEAR_ID:1:2,COUNTRY:2:5,STATE:3:10,CATEGORY_NAME:4:5,PROTOCOL_NAME:5:20",
    //                builder.toString());
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetAggregateMeasures_Output_AggregateTablesMesuaresArray()
    //    {
    //        AggregateTable[] aggregateTable = CarbonSchemaParser.getAggregateTable(mondrianCubes);
    //        assertTrue(aggregateTable.length == 1);
    //        String[] measures = aggregateTable[0].getAggMeasure();
    //        assertTrue(measures.length == 11);
    //        String expectedMeasures[] = {"MEASURE1", "MEASURE2", "MEASURE3", "MEASURE4", "MEASURE5", "MEASURE6",
    //                "MEASURE7", "MEASURE8", "MEASURE9", "MEASURE10", "fact_count"};
    //
    //        for(int i = 0;i < measures.length;i++)
    //        {
    //            if(!measures[i].equals(expectedMeasures[i]))
    //            {
    //                assertTrue(false);
    //                break;
    //            }
    //        }
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetMeasureStringForAgg_Output_MeasureString_With_ColumnIndex_ForAggTable()
    //    {
    //        AggregateTable[] aggregateTable = CarbonSchemaParser.getAggregateTable(mondrianCubes);
    //        assertTrue(aggregateTable.length == 1);
    //        String[] aggMeasures = aggregateTable[0].getAggMeasure();
    //        assertTrue(aggMeasures.length == 11);
    //        String builder = CarbonSchemaParser.getMeasureStringForAgg(aggMeasures, 0);
    //        System.out.println(builder);
    //        assertEquals(
    //                "MEASURE1:0,MEASURE2:1,MEASURE3:2,MEASURE4:3,MEASURE5:4,MEASURE6:5,MEASURE7:6,MEASURE8:7,MEASURE9:8,MEASURE10:9,fact_count:10",
    //                builder.toString());
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetAggregator_Output_AggregateTablesMesuaresAggregatorArray()
    //    {
    //        AggregateTable[] aggregateTable = CarbonSchemaParser.getAggregateTable(mondrianCubes);
    //        assertTrue(aggregateTable.length == 1);
    //        String[] measuresAggregator = aggregateTable[0].getAggregator();
    //        assertTrue(measuresAggregator.length == 11);
    //        String expectedMeasuresAggregator[] = {"count", "sum", "sum", "sum", "count", "sum", "count", "sum", "sum",
    //                "distinct-count", "count"};
    //
    //        for(int i = 0;i < measuresAggregator.length;i++)
    //        {
    //            if(!measuresAggregator[i].equals(expectedMeasuresAggregator[i]))
    //            {
    //                assertTrue(false);
    //                break;
    //            }
    //        }
    //    }
    //
    //    @Test
    //    public void test_CarbonSchemaParser_GetTableInputQueryForAggTable_Output_AggregateTablesInputStepQuery()
    //    {
    //        AggregateTable[] aggregateTable = CarbonSchemaParser.getAggregateTable(mondrianCubes);
    //        assertTrue(aggregateTable.length == 1);
    //        String tableInputSQLQueryForAGG = CarbonSchemaParser.getTableInputSQLQueryForAGG(
    //                aggregateTable[0].getAggLevels(), aggregateTable[0].getAggMeasure(),
    //                aggregateTable[0].getAggregateTableName());
    //        String query = "SELECT " + "\n" + "DEVICE_NAME," + "\n" + "YEAR_ID," + "\n" + "COUNTRY," + "\n" + "STATE,"
    //                + "\n" + "CATEGORY_NAME," + "\n" + "PROTOCOL_NAME," + "\n" + "MEASURE1," + "\n" + "MEASURE2," + "\n"
    //                + "MEASURE3," + "\n" + "MEASURE4," + "\n" + "MEASURE5," + "\n" + "MEASURE6," + "\n" + "MEASURE7,"
    //                + "\n" + "MEASURE8," + "\n" + "MEASURE9," + "\n" + "MEASURE10," + "\n" + "fact_count" + "\n"
    //                + " FROM agg_2_Dev_Year_State_Prot_Temp";
    //        assertEquals(query, tableInputSQLQueryForAGG);
    //    }
}
