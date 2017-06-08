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
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.BlockIndex;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BTreeNonLeafNode;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.processor.impl.DataBlockIteratorImpl;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverterV3;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.types.Decimal;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestCarbonHiveRecordReader {

    private static CarbonHiveRecordReader carbonHiveRecordReader;
    private static QueryModel queryModel;
    private static CarbonReadSupport carbonReadSupport;
    private static InputSplit inputSplit;
    private static JobConf jobConf;
    private static HiveConf configuration;
    private static CarbonStorePath path;
    private static String[] locations;
    private static ColumnarFormatVersion version;
    private static CarbonTableIdentifier carbonTableIdentifier;
    private static Void avoid;

    @BeforeClass
    public static void setUp() throws IOException {
        carbonTableIdentifier = new CarbonTableIdentifier("Default", "Sample", "t1");
        queryModel = new QueryModel();
        carbonReadSupport = new DictionaryDecodeReadSupport();
        inputSplit = new CarbonHiveInputSplit();
        jobConf = new JobConf();
        configuration = new HiveConf();
        path = new CarbonStorePath("CarbonStorePath");
        locations = new String[]{"location1, location2, location3"};
        version = ColumnarFormatVersion.V3;
        jobConf.set("hive.io.file.readcolumn.ids", "0,1");
        jobConf.set("hive.io.file.readcolumn.names", "id,name");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "int,string");

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "CarbondataFileName";
            }

        };

        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            //new MockUp<QueryModel>() {
            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };
        new MockUp<QueryUtil>() {
            @Mock
            public void resolveQueryModel(QueryModel queryModel) {
            }

        };

        new MockUp<CarbonTable>() {
            @Mock
            public String getStorePath() {
                return "carbonStore";
            }

        };
        new MockUp<CarbonTablePath>() {
            @Mock
            public String getCarbonDataFileName(String carbonDataFilePath) {
                return "carbonfile";
            }
        };
        new MockUp<CarbonUtil>() {
            @Mock
            public long calculateMetaSize(TableBlockInfo tableBlockInfo) throws IOException {
                return 1;
            }
        };

        new MockUp<ThriftReader>() {
            @Mock
            public void open() throws IOException {

            }
        };

        new MockUp<CarbonHeaderReader>() {
            @Mock

            public FileHeader readHeader() throws IOException {
                return new FileHeader();
            }
        };

        new MockUp<ThriftReader>() {
            @Mock
            public void setReadOffset(long bytesToSkip) throws IOException {

            }
        };

        new MockUp<CarbonFooterReaderV3>() {
            @Mock
            public FileFooter3 readFooterVersion3() throws IOException {

                return new FileFooter3();
            }
        };
        new MockUp<DataFileFooterConverterV3>() {
            @Mock
            public DataFileFooter readDataFileFooter(TableBlockInfo tableBlockInfo) {

                return new DataFileFooter();
            }

        };

        new MockUp<BlockIndex>() {
            @Mock
            public void buildIndex(List<DataFileFooter> footerList) {

            }

            @Mock
            public SegmentProperties getSegmentProperties() {
                ColumnSchema columnSchema = new ColumnSchema();
                ArrayList<ColumnSchema> columnSchemaArrayList = new ArrayList<ColumnSchema>();
                columnSchemaArrayList.add(columnSchema);
                int[] columnCard = new int[]{1, 2, 3};
                return new SegmentProperties(columnSchemaArrayList, columnCard);
            }

        };

        new MockUp<SegmentProperties>() {
            @Mock
            public List<CarbonDimension> getDimensions() {
                ArrayList<CarbonDimension> list = new ArrayList<>();
                ColumnSchema columnSchema = new ColumnSchema();
                CarbonDimension carbonDimension = new CarbonDimension(columnSchema, 1, 2, 1, 1);
                list.add(carbonDimension);
                return list;
            }
        };

        new MockUp<SegmentProperties>() {
            @Mock
            public List<CarbonDimension> getDimensions() {
                ArrayList<CarbonDimension> list = new ArrayList<>();
                ColumnSchema columnSchema = new ColumnSchema();
                CarbonDimension carbonDimension = new CarbonDimension(columnSchema, 1, 2, 1, 1);
                list.add(carbonDimension);
                return list;
            }
        };
        new MockUp<CarbonTablePath>() {
            @Mock
            public String getFactDir() {
                return "carbonfactDir";
            }

        };
        new MockUp<CarbonStorePath>() {

            @Mock
            public CarbonTablePath getCarbonTablePath(String storePath,
                                                      CarbonTableIdentifier tableIdentifier) {
                ArrayList<CarbonDimension> list = new ArrayList<>();
                return new CarbonTablePath("value1", "value2", "value3");
            }
        };

        new MockUp<BlockExecutionInfo>() {
            @Mock
            public void setBlockId(String blockId) {

            }
        };

        new MockUp<BTreeDataRefNodeFinder>() {
            @Mock
            public DataRefNode findFirstDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {

                return new BTreeNonLeafNode();
            }

            @Mock
            public DataRefNode findLastDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {

                return new BTreeNonLeafNode();
            }
        };
        new MockUp<BTreeNonLeafNode>() {
            @Mock
            public long nodeNumber() {

                return 1;
            }

        };

        new MockUp<DataBlockIteratorImpl>() {
            @Mock
            public List<Object[]> next() {
                Object[] obj = new Object[5];
                obj[0] = 1;
                obj[1] = 2;
                obj[2] = 3;
                obj[3] = 4;
                obj[4] = 5;
                List<Object[]> list = new ArrayList();
                list.add(obj);
                return list;
            }
        };

    }

    @Test
    public void initailizeTest() throws IOException, IllegalAccessException {

        try {
            carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        } catch (NullPointerException nullPointerException) {
            assert true;
        }


        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        Field field = null;
        try {
            field = CarbonHiveRecordReader.class.getDeclaredField("objInspector");
        } catch (NoSuchFieldException noSuchFieldException) {
            noSuchFieldException.printStackTrace();
        }
        field.setAccessible(true);
        Object result = field.get(carbonHiveRecordReader);
        assertNotNull(result);

    }

    @Test
    public void testNextMethodForStringTypes() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };
        String[] str = new String[]{"value1", "value2"};
        ArrayWritable arrayWritable = new ArrayWritable(str);
        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{"value1", "value2"};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "name,location");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "string,string");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        ;
        assertTrue(carbonHiveRecordReader.next(avoid, arrayWritable));

    }

    @Test
    public void testNextMethodForIntTypes() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };
        String[] str = new String[]{"value1", "value2"};

        IntWritable intWritable1 = new IntWritable(1);
        IntWritable intWritable2 = new IntWritable(2);
        Writable[] writables = new Writable[]{intWritable1, intWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(intWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{1, 2};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "int,int");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        ;
        assertTrue(carbonHiveRecordReader.next(avoid, arrayWritable));

    }

    @Test
    public void testNextMethodForDoubleTypes() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };

        DoubleWritable doubleWritable1 = new DoubleWritable(1.0);
        DoubleWritable doubleWritable2 = new DoubleWritable(2.0);
        Writable[] writables = new Writable[]{doubleWritable1, doubleWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(doubleWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{1.0, 2.0};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "double,double");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        ;
        assertTrue(carbonHiveRecordReader.next(avoid, arrayWritable));

    }

    @Test
    public void testNextMethodForDecimalTypes() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };
        final Object decimalObj1 = new Decimal();
        final Object decimalObj2 = new Decimal();

        ;
        HiveDecimalWritable decimalWritable1 = new HiveDecimalWritable(HiveDecimal.create(
                ((org.apache.spark.sql.types.Decimal) decimalObj1).toJavaBigDecimal()));
        HiveDecimalWritable decimalWritable2 = new HiveDecimalWritable(HiveDecimal.create(
                ((org.apache.spark.sql.types.Decimal) decimalObj2).toJavaBigDecimal()));
        Writable[] writables = new Writable[]{decimalWritable1, decimalWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(decimalWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{decimalObj1, decimalObj2};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "decimal,decimal");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        ;
        assertTrue(carbonHiveRecordReader.next(avoid, arrayWritable));

    }

    @Test
    public void testNextMethodForLongTypes() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };

        LongWritable longWritable1 = new LongWritable(1L);
        LongWritable longWritable2 = new LongWritable(2L);
        Writable[] writables = new Writable[]{longWritable1, longWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(longWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] objects = new Object[]{1L, 2L};
                return objects;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "bigint,bigint");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        ;
        assertTrue(carbonHiveRecordReader.next(avoid, arrayWritable));

    }

    @Test
    public void testNextMethodForShortTypes() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };

        ShortWritable shortWritable1 = new ShortWritable((short) 1);
        ShortWritable shortWritable2 = new ShortWritable((short) 2);
        Writable[] writables = new Writable[]{shortWritable1, shortWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(shortWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{Short.valueOf((short) 1), Short.valueOf((short) 2)};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.ids", "0,1");
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "smallint,smallint");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        ;
        assertTrue(carbonHiveRecordReader.next(avoid, arrayWritable));

    }

    @Test
    public void testNextMethodForDate() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };

        DateWritable dateWritable1 = new DateWritable(new Date(14558909L));
        DateWritable dateWritable2 = new DateWritable(new Date(14558999L));
        Writable[] writables = new Writable[]{dateWritable1, dateWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(dateWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{14558909L, 14558999L};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "date,date");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        ;
        assertTrue(carbonHiveRecordReader.next(avoid, arrayWritable));

    }

    @Test
    public void testNextMethodForTimestamp() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };

        TimestampWritable timestampWritable1 = new TimestampWritable(new Timestamp(12345678L));
        TimestampWritable timestampWritable2 = new TimestampWritable(new Timestamp(67574533L));
        Writable[] writables = new Writable[]{timestampWritable1, timestampWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(timestampWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{12345678L};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "timestamp,timestamp");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        ;
        assertTrue(carbonHiveRecordReader.next(avoid, arrayWritable));

    }

    @Test
    public void testGetPos() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };

        TimestampWritable timestampWritable1 = new TimestampWritable(new Timestamp(12345678L));
        TimestampWritable timestampWritable2 = new TimestampWritable(new Timestamp(67574533L));
        Writable[] writables = new Writable[]{timestampWritable1, timestampWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(timestampWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{12345678L};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "timestamp,timestamp");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        long expected = 0;
        long actual = carbonHiveRecordReader.getPos();
        assertEquals(expected, actual);
    }

    @Test
    public void testGetProgress() throws IOException {
        inputSplit = new CarbonHiveInputSplit("segmentId", path, 1, 5, locations, version);
        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }

            @Mock
            public CarbonTable getTable() {
                return new CarbonTable();
            }

        };

        TimestampWritable timestampWritable1 = new TimestampWritable(new Timestamp(12345678L));
        TimestampWritable timestampWritable2 = new TimestampWritable(new Timestamp(67574533L));
        Writable[] writables = new Writable[]{timestampWritable1, timestampWritable2};
        ArrayWritable arrayWritable = new ArrayWritable(timestampWritable1.getClass(), writables);

        new MockUp<DictionaryDecodeReadSupport>() {
            @Mock
            public Object[] readRow(Object[] data) {
                Object[] obj = new Object[]{12345678L};
                return obj;
            }
        };
        jobConf.set("hive.io.file.readcolumn.names", "id,oldId");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "timestamp,timestamp");
        carbonHiveRecordReader = new CarbonHiveRecordReader(queryModel, carbonReadSupport, inputSplit, jobConf);
        float expected = 0.0F;
        float actual = carbonHiveRecordReader.getProgress();

        assertEquals(expected, actual, 0.0F);
    }
}

