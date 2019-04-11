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

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.*;

import org.apache.log4j.Logger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.carbondata.common.logging.LogServiceFactory;

public class TestCarbonSerDe {

    private static final Logger LOGGER =
            LogServiceFactory.getLogService(TestCarbonSerDe.class.getCanonicalName());

    @Test
    public void testCarbonHiveSerDe() throws Throwable {
        // Create the SerDe
        LOGGER.info("test: testCarbonHiveSerDe");

        final CarbonHiveSerDe carbonHiveSerDe = new CarbonHiveSerDe();
        final Configuration configuration = new Configuration();
        final Properties tblProperties = createProperties();
        SerDeUtils.initializeSerDe(carbonHiveSerDe, configuration, tblProperties, null);

        // Data
        final Writable[] arr = new Writable[7];

        // Primitive types
        arr[0] = new ShortWritable((short) 456);
        arr[1] = new IntWritable(789);
        arr[2] = new LongWritable(1000l);
        arr[3] = new DoubleWritable(5.3);
        arr[4] = new HiveDecimalWritable(HiveDecimal.create(1));
        arr[5] = new Text("CarbonSerDe Binary".getBytes("UTF-8"));

        final Writable[] arrayContainer = new Writable[1];
        final Writable[] array = new Writable[5];
        for (int i = 0; i < 5; ++i) {
            array[i] = new IntWritable(i);
        }
        arrayContainer[0] = new ArrayWritable(Writable.class, array);
        arr[6] = new ArrayWritable(Writable.class, arrayContainer);

        final ArrayWritable arrayWritable = new ArrayWritable(Writable.class, arr);
        // Test
        deserializeAndSerializeLazySimple(carbonHiveSerDe, arrayWritable);

        LOGGER.info("test: testCarbonHiveSerDe - OK");
    }

    private void deserializeAndSerializeLazySimple(final CarbonHiveSerDe serDe,
                                                   final ArrayWritable t) throws SerDeException {

        // Get the row structure
        final StructObjectInspector oi = (StructObjectInspector) serDe.getObjectInspector();

        // Deserialize
        final Object row = serDe.deserialize(t);
        Assert.assertEquals("deserialization gives the wrong object class", row.getClass(),
                ArrayWritable.class);
        Assert.assertEquals("size correct after deserialization",
                serDe.getSerDeStats().getRawDataSize(), t.get().length);
        Assert.assertEquals("deserialization gives the wrong object", t, row);

        // Serialize
        final ArrayWritable serializedArr = (ArrayWritable) serDe.serialize(row, oi);
        Assert.assertEquals("size correct after serialization", serDe.getSerDeStats().getRawDataSize(),
                serializedArr.get().length);
        Assert.assertTrue("serialized object should be equal to starting object",
                arrayWritableEquals(t, serializedArr));
    }

    private Properties createProperties() {
        final Properties tbl = new Properties();

        // Set the configuration parameters
        tbl.setProperty("columns", "aShort,aInt,aLong,aDouble,aDecimal,aString,aList");
        tbl.setProperty("columns.types",
                "smallint:int:bigint:double:decimal:string:array<int>");
        tbl.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");
        return tbl;
    }

    private static boolean arrayWritableEquals(final ArrayWritable a1, final ArrayWritable a2) {
        final Writable[] a1Arr = a1.get();
        final Writable[] a2Arr = a2.get();

        if (a1Arr.length != a2Arr.length) {
            return false;
        }

        for (int i = 0; i < a1Arr.length; ++i) {
            if (a1Arr[i] instanceof ArrayWritable) {
                if (!(a2Arr[i] instanceof ArrayWritable)) {
                    return false;
                }
                if (!arrayWritableEquals((ArrayWritable) a1Arr[i], (ArrayWritable) a2Arr[i])) {
                    return false;
                }
            } else {
                if (!a1Arr[i].equals(a2Arr[i])) {
                    return false;
                }
            }

        }
        return true;
    }
}
