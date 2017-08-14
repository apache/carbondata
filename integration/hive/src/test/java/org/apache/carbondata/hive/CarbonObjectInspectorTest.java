/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CarbonObjectInspectorTest {

    private CarbonObjectInspector carbonObjectInspector = null;

    private List<String> columnNames = Arrays
            .asList("id", "name", "salary", "enrollment", "performanceIndex", "areaOfExpertise",
                    "projects", "dept", "dateOfJoining");
    private String listOfColumnTypes =
            "int:string:double:bigint,decimal:struct<area:string,experience:int>:array<string>:smallint:date";

    @Before
    public void setUp() {
        carbonObjectInspector = getCarbonObjectInspector();
    }

    private CarbonObjectInspector getCarbonObjectInspector() {
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(listOfColumnTypes);
        TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        return new CarbonObjectInspector((StructTypeInfo) rowTypeInfo);
    }

    @Test
    public void testGetStructFieldDataNullData() {
        Assert.assertNull(carbonObjectInspector
                .getStructFieldData(null, carbonObjectInspector.getStructFieldRef("salary")));
    }

    @Test
    public void testGetStructFieldDataArrayWritable() {

        Writable[] intWritable =
                new Writable[]{new IntWritable(10000), new IntWritable(20000), new IntWritable(30000)};
        final ArrayWritable writable = new ArrayWritable(IntWritable.class, intWritable);

        CarbonObjectInspector carbonObjectInspector = getCarbonObjectInspector();
        Object result = carbonObjectInspector
                .getStructFieldData(writable, carbonObjectInspector.getStructFieldRef("salary"));
        Assert.assertEquals(30000, ((IntWritable) result).get());
    }

    @Test
    public void testGetStructFieldDataList() {
        CarbonObjectInspector carbonObjectInspector = getCarbonObjectInspector();
        Object result = carbonObjectInspector
                .getStructFieldData(Arrays.asList(1000, "carbondata", 3000, 4000),
                        carbonObjectInspector.getStructFieldRef("name"));
        Assert.assertEquals("carbondata", (String) result);
    }

    @Test
    public void testGetStructFieldsDataListOfNull() {
        CarbonObjectInspector carbonObjectInspector = getCarbonObjectInspector();
        Assert.assertNull(carbonObjectInspector.getStructFieldsDataAsList(null));
    }

    @Test
    public void testGetStructFieldsDataAsListArrayWritable() {
        Writable[] intWritable =
                new Writable[]{new IntWritable(10000), new IntWritable(20000), new IntWritable(30000)};
        final ArrayWritable writable = new ArrayWritable(IntWritable.class, intWritable);

        CarbonObjectInspector carbonObjectInspector = getCarbonObjectInspector();
        ArrayList result = (ArrayList) carbonObjectInspector.getStructFieldsDataAsList(writable);
        Assert.assertEquals(10000, ((IntWritable) result.get(0)).get());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStructFieldsDataAsListForUnsupportedOperation() {
        getCarbonObjectInspector().getStructFieldsDataAsList(Arrays.asList(1000, 2000, 3000, 4000));
    }

    @Test
    public void testCreate() {
        Assert.assertTrue(getCarbonObjectInspector().create() instanceof ArrayList);
    }

    @Test
    public void testSetStructFieldData() {
        ArrayList list = new ArrayList();
        list.addAll(Arrays.asList(10000, 20000, 30000));
        int fieldValue = 60000;
        ArrayList result = (ArrayList) carbonObjectInspector
                .setStructFieldData(list, carbonObjectInspector.getStructFieldRef("salary"), fieldValue);
        Assert.assertEquals(fieldValue, result.get(2));
    }

    @Test
    public void testEqualForNullValue() {
        Assert.assertFalse(carbonObjectInspector.equals(null));
    }

    @Test
    public void testEqualsForDifferentInstance() {
        TypeInfo rowTypeInfo;
        List<String> columnNames = Arrays.asList("id", "name", "salary");
        String listOfColumnTypes = "int:string:double";
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(listOfColumnTypes);
        rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);

        CarbonObjectInspector newCarbonObjectInspector =
                new CarbonObjectInspector((StructTypeInfo) rowTypeInfo);
        Assert.assertFalse(carbonObjectInspector.equals(newCarbonObjectInspector));
    }

    @Test
    public void testEqualsForSameInstance() {
        Assert.assertTrue(carbonObjectInspector.equals(getCarbonObjectInspector()));
    }

    @Test
    public void testEqualsForDifferentObjectInspectors() {
        String testObject = "TestInspector";
        Assert.assertFalse(carbonObjectInspector.equals(testObject));
    }

    @Test
    public void testGetCategoryValue() {
        Assert.assertEquals(ObjectInspector.Category.STRUCT, carbonObjectInspector.getCategory());
    }

    @Test
    public void testGetTypenameFromTypeInfo() {
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(listOfColumnTypes);
        TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        Assert.assertEquals(rowTypeInfo.getTypeName(), carbonObjectInspector.getTypeName());
    }

    @Test
    public void testStructFieldImpl() {
        Assert.assertNotNull(carbonObjectInspector.getStructFieldRef("name").getFieldComment());
        Assert.assertEquals("name", carbonObjectInspector.getStructFieldRef("name").getFieldName());
        Assert.assertTrue(carbonObjectInspector.getStructFieldRef("name")
                .getFieldObjectInspector() instanceof ObjectInspector);
    }
}
