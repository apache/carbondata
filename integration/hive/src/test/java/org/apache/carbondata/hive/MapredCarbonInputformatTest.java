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
import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.impl.DetailQueryExecutor;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputFormat;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MapredCarbonInputformatTest {

    private static final String INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";
    private static JobConf jobConf;
    private static CarbonTable carbonTable;
    private static String[] locations = new String[]{"loc1", "loc2", "loc3"};
    private static int tableBlockSize =
            Integer.parseInt(CarbonCommonConstants.BLOCK_SIZE_DEFAULT_VAL);
    private MapredCarbonInputFormat mapredCarbonInputFormat = new MapredCarbonInputFormat();
    private Path path = new Path("defaultPath");

    @BeforeClass
    public static void setUp() throws IOException {
        jobConf = new JobConf();
        jobConf.set("hive.io.file.readcolumn.ids", "0,1");
        jobConf.set(serdeConstants.LIST_COLUMNS, "id,name");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "int,string");
        jobConf.set("mapreduce.input.carboninputformat.projection", "id,name");
        jobConf.set(INPUT_DIR, "default");
        jobConf.set("hive.io.file.readcolumn.names", "id,name");
        new MockUp<CarbonInputFormat>() {
            @Mock
            public List<InputSplit> getSplits(JobContext job) throws IOException {
                List<InputSplit> list = new ArrayList<InputSplit>();
                list.add(new CarbonInputSplit());
                return list;
            }

            @Mock
            public void setTableInfo(Configuration configuration, TableInfo tableInfo)
                    throws IOException {
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "1";
            }

            @Mock
            String getBucketNo(String carbonFilePath) {
                return "bucket no";
            }
        };

    }

    @Test
    public void testGetSplits() throws Exception {

        new MockUp<Job>() {
            @Mock
            Job getInstance(Configuration conf) throws IOException {
                return new Job(jobConf);
            }
        };

        new MockUp<CarbonInputSplit>() {
            @Mock
            public long getStart() {
                return 1;
            }

            @Mock
            public long getLength() {
                return 1;
            }

            @Mock
            public Path getPath() {
                return path;
            }

            @Mock
            public String[] getLocations() throws IOException {
                return locations;
            }

            @Mock
            public int getNumberOfBlocklets() {
                return 1;
            }

            @Mock
            public ColumnarFormatVersion getVersion() {
                return ColumnarFormatVersion.V3;
            }

            @Mock
            public Map<String, String> getBlockStorageIdMap() {
                Map<String, String> map = new HashMap<String, String>();
                map.put("block1", "value1");
                map.put("block2", "value2");
                map.put("block3", "value3");
                return map;
            }
        };

        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "some path";
            }
        };

        String expected = "[defaultPath:1+1]";
        assertEquals(expected, Arrays.deepToString(mapredCarbonInputFormat.getSplits(jobConf, 2)));
    }

    private ColumnSchema createColumnarDimensionColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnar(true);
        dimColumn.setColumnName("imei");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.STRING);
        dimColumn.setDimensionColumn(true);
        List<Encoding> encodeList =
                new ArrayList<Encoding>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        encodeList.add(Encoding.DICTIONARY);
        dimColumn.setEncodingList(encodeList);
        dimColumn.setNumberOfChild(0);
        return dimColumn;
    }

    private ColumnSchema createColumnarMeasureColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnName("id");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.INT);
        return dimColumn;
    }

    private TableSchema createTableSchema() {
        TableSchema tableSchema = new TableSchema();
        List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
        columnSchemaList.add(createColumnarMeasureColumn());
        columnSchemaList.add(createColumnarDimensionColumn());
        tableSchema.setListOfColumns(columnSchemaList);
        tableSchema.setTableName("table1");
        return tableSchema;
    }

    private TableInfo createTableInfo(long timeStamp) {
        TableInfo info = new TableInfo();
        info.setDatabaseName("schema1");
        info.setLastUpdatedTime(timeStamp);
        info.setTableUniqueName("schema1_tableName");
        info.setFactTable(createTableSchema());
        info.setStorePath("storePath");
        return info;
    }

    private List<CarbonColumn> getCreateOrderColumn(String tableName) {
        List<CarbonColumn> carbonColumnNames = new ArrayList<>();
        CarbonColumn carbonColumn = new CarbonColumn(new ColumnSchema(), 1, 1);
        carbonColumn.getColumnSchema().setColumnName("id");
        carbonColumnNames.add(carbonColumn);
        CarbonColumn carbonColumn1 = new CarbonColumn(new ColumnSchema(), 1, 1);
        carbonColumn1.getColumnSchema().setColumnName("name");
        carbonColumnNames.add(carbonColumn1);
        return carbonColumnNames;
    }

    private TableInfo createCarbonTable() {
        TableInfo tableInfo;
        tableInfo = createTableInfo(1000);
        carbonTable = CarbonTable.buildFromTableInfo(createTableInfo(1000L));
        return tableInfo;
    }

    //private method to createCarbonTable using createTableInfo method

    @Test
    public void getRecordReader() throws IOException {
        CarbonHiveInputSplit carbonHiveInputSplit =
                new CarbonHiveInputSplit("segment", path, 1, 3, locations, 4, ColumnarFormatVersion.V3);
        new MockUp<CarbonTable>() {
            @Mock
            public List<CarbonColumn> getCreateOrderColumn(String tableName) {
                return MapredCarbonInputformatTest.this.getCreateOrderColumn(
                        MapredCarbonInputformatTest.this.createTableInfo(1000).getDatabaseName());
            }
        };

        Reporter reporter = new Reporter() {
            @Override
            public void setStatus(String status) {
            }

            @Override
            public Counters.Counter getCounter(Enum<?> name) {
                return null;
            }

            @Override
            public Counters.Counter getCounter(String group, String name) {
                return null;
            }

            @Override
            public void incrCounter(Enum<?> key, long amount) {

            }

            @Override
            public void incrCounter(String group, String counter, long amount) {

            }

            @Override
            public org.apache.hadoop.mapred.InputSplit getInputSplit()
                    throws UnsupportedOperationException {
                return null;
            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public void progress() {

            }
        };

        new MockUp<SchemaReader>() {
            @Mock
            public CarbonTable readCarbonTableFromStore(AbsoluteTableIdentifier identifier)
                    throws IOException {
                carbonTable = CarbonTable.buildFromTableInfo(createTableInfo(1000L));
                return carbonTable;
            }
        };

        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier =
                        new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }
        };

        new MockUp<AbsoluteTableIdentifier>() {
            @Mock
            public AbsoluteTableIdentifier fromTablePath(String tablePath) {
                CarbonTableIdentifier carbonTableIdentifier =
                        new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("default", carbonTableIdentifier);
            }
        };

        new MockUp<TableInfo>() {
            @Mock
            int getTableBlockSizeInMB() {
                return tableBlockSize;
            }

            @Mock
            public AbsoluteTableIdentifier getOrCreateAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier =
                        new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("default", carbonTableIdentifier);
            }
        };

        new MockUp<DetailQueryExecutor>() {
            @Mock
            public CarbonIterator execute(QueryModel queryModel)
                    throws QueryExecutionException, IOException {
                return new BatchResult();

            }
        };

        new MockUp<CarbonInputFormatUtil>() {
            @Mock
            public CarbonQueryPlan createQueryPlan(CarbonTable carbonTable, String columnString) {
                CarbonQueryPlan plan =
                        new CarbonQueryPlan(carbonTable.getDatabaseName(), carbonTable.getFactTableName());
                return plan;
            }
        };
        assertTrue(mapredCarbonInputFormat.getRecordReader(carbonHiveInputSplit, jobConf,
                reporter) instanceof CarbonHiveRecordReader);
    }

    @Test
    public void testShouldSkipCombine() throws IOException {
        Configuration conf = new HiveConf();
        assertTrue(mapredCarbonInputFormat.shouldSkipCombine(path, conf));
    }
}

