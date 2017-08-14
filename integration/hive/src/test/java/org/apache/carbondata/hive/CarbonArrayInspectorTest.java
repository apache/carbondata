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

import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CarbonArrayInspectorTest {

    private static CarbonObjectInspector carbonObjectInspector;
    private static CarbonArrayInspector carbonArrayInspector;
    private static CarbonArrayInspector carbonArrayInspector1;
    private static ArrayList<TypeInfo> typeInfoArrayList;
    private static ObjectInspector objectInspector;
    private static JobConf jobConf;

    @BeforeClass
    public static void setUp() throws IOException {
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("id");
        fieldNames.add("name");
        jobConf = new JobConf();
        jobConf.set("hive.io.file.readcolumn.ids", "0,1");
        jobConf.set(serdeConstants.LIST_COLUMNS, "id,name");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "int,string");
        final String columnNameProperty = jobConf.get(serdeConstants.LIST_COLUMNS);
        final String columnTypeProperty = jobConf.get(serdeConstants.LIST_COLUMN_TYPES);
        final List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
        final List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        final TypeInfo typeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        objectInspector = new CarbonObjectInspector((StructTypeInfo) typeInfo);

        new MockUp<StructTypeInfo>() {
            @Mock
            public ArrayList<String> getAllStructFieldNames() {
                ArrayList<String> arrayList = new ArrayList<String>();
                arrayList.add("element1");
                return arrayList;
            }

            @Mock
            public ArrayList<TypeInfo> getAllStructFieldTypeInfos() {
                typeInfoArrayList = new ArrayList<>();
                typeInfoArrayList.add(new DecimalTypeInfo());
                return typeInfoArrayList;
            }
        };

        new MockUp<ObjectInspector>() {
            @Mock
            public ObjectInspector.Category getCategory() {
                return ObjectInspector.Category.LIST;
            }
        };

        new MockUp<CarbonObjectInspector>() {
            @Mock
            public String getTypeName() {
                return "some string";
            }

        };

        carbonObjectInspector = new CarbonObjectInspector((StructTypeInfo) typeInfo);
        carbonArrayInspector = new CarbonArrayInspector(objectInspector);
        carbonArrayInspector1 = new CarbonArrayInspector(objectInspector);
    }

    private ArrayWritable getWritables() {
        IntWritable intWritable1 = new IntWritable(1);
        IntWritable intWritable2 = new IntWritable(2);
        Writable[] intWritables = new Writable[]{intWritable1, intWritable2};
        ArrayWritable arrayWritable1 = new ArrayWritable(IntWritable.class, intWritables);
        ArrayWritable arrayWritable2 = new ArrayWritable(IntWritable.class, intWritables);
        Writable[] writableArray = new Writable[]{arrayWritable1, arrayWritable2};
        final ArrayWritable writables = new ArrayWritable(ArrayWritable.class, writableArray);
        return writables;
    }

    @Test
    public void getTypeName() {
        assertEquals(carbonArrayInspector.getTypeName(), "array<some string>");
    }

    @Test
    public void getCategory() {
        assertEquals(carbonArrayInspector.getCategory(), ObjectInspector.Category.LIST);
    }

    @Test
    public void getListElementObjectInspector() {
        assertEquals(carbonObjectInspector, carbonArrayInspector.getListElementObjectInspector());
    }

    @Test
    public void set() {
        ArrayList<Object> arrayList = new ArrayList<Object>();
        arrayList.add(new Object());
        ArrayList<Object> objectArrayList = new ArrayList<Object>();
        objectArrayList.add(new Object());
        assertEquals(arrayList, carbonArrayInspector.set(arrayList, 0, objectArrayList));
    }

    @Test
    public void resize() {
        ArrayList<Object> objectArrayList = new ArrayList<Object>();
        objectArrayList.add(new Object());
        objectArrayList.add(new Object());
        assertEquals(objectArrayList, carbonArrayInspector.resize(objectArrayList, 1));
    }

    @Test
    public void testResizeInDifferentScenario() {
        ArrayList<Object> objectArrayList = new ArrayList<Object>();
        objectArrayList.add(new Object());
        objectArrayList.add(new Object());
        assertEquals(objectArrayList, carbonArrayInspector.resize(objectArrayList, 3));
    }

    @Test
    public void equals() {
        Object object = new Object();
        assertEquals(false, carbonArrayInspector.equals(object));
    }

    @Test
    public void equalsWhenTrue() {
        assertEquals(true, carbonArrayInspector.equals(carbonArrayInspector1));
    }

    @Test
    public void getListElement() {
        IntWritable intWritable1 = new IntWritable(1);
        List<Integer> integerArrayList = new ArrayList<Integer>();
        integerArrayList.add(1);
        integerArrayList.add(2);
        assertEquals(intWritable1, carbonArrayInspector.getListElement(getWritables(), 0));
    }

    @Test
    public void getListElementWhenNull() {
        assertEquals(null, carbonArrayInspector.getListElement(null, 0));
    }

    @Test
    public void getListElementWhenListempty() {
        Writable[] writableArray = new Writable[]{};
        final ArrayWritable writables = new ArrayWritable(ArrayWritable.class, writableArray);
        assertNull(carbonArrayInspector.getListElement(writables, 0));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getListElementExceptionCase() {
        IntWritable intWritable1 = new IntWritable(1);
        carbonArrayInspector.getListElement(intWritable1, 1);
    }

    @Test
    public void getListLength() {
        List<Integer> integerArrayList = new ArrayList<Integer>();
        integerArrayList.add(1);
        integerArrayList.add(2);
        assertEquals(integerArrayList.size(), carbonArrayInspector.getListLength(getWritables()));
    }

    @Test
    public void getListLengthWithNullList() {
        assertEquals(-1, carbonArrayInspector.getListLength(null));
    }

    @Test
    public void getListLengthWhenZeroElementsPresent() {
        Writable[] intWritables = new Writable[]{};
        ArrayWritable arrayWritable1 = new ArrayWritable(IntWritable.class, intWritables);
        ArrayWritable arrayWritable2 = new ArrayWritable(IntWritable.class, intWritables);
        Writable[] writableArray = new Writable[]{arrayWritable1, arrayWritable2};
        final ArrayWritable writables = new ArrayWritable(ArrayWritable.class, writableArray);
        assertEquals(0, carbonArrayInspector.getListLength(writables));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getListLengthExceptionCase() {
        carbonArrayInspector.getListLength(new Object());
    }

    @Test
    public void getList() {
        assertEquals(2, carbonArrayInspector.getList(getWritables()).size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getListExceptionCase() {
        carbonArrayInspector.getList(new Object());
    }

    @Test
    public void getListWhenNull() {
        assertEquals(null, carbonArrayInspector.getList(null));
    }

    @Test
    public void hashcode() {
        assertEquals(carbonArrayInspector.hashCode(), -885793222);
    }
}


