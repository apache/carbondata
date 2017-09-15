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

package org.apache.carbondata.presto.impl;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.gson.Gson;
import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.*;
import org.apache.carbondata.core.datastore.block.*;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.LocalCarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.service.impl.PathFactory;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.TableInfo;
import org.apache.carbondata.hadoop.CacheAccessClient;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.thrift.TBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CarbonTableReaderTest {

    CarbonTableReader carbonTableReader = new CarbonTableReader(new CarbonTableConfig());
    File file = new File("schema");
    File tableStatusFile = new File("tablestatus");
    File dictFile = new File("dictFile");

    @Before
    public void setUp() {
        try {
            file.createNewFile();
            tableStatusFile.createNewFile();
            dictFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        new MockUp<LocalCarbonFile>() {
            @Mock
            public CarbonFile[] listFiles() {
                CarbonFile[] carbonFiles = {new LocalCarbonFile("storePath/schemaName")};
                return carbonFiles;
            }
        };

        new MockUp<ThriftReader>() {
            @Mock
            public TBase read() {
                return new TableInfo();
            }
        };

        new MockUp<ThriftWrapperSchemaConverterImpl>() {
            @Mock
            org.apache.carbondata.core.metadata.schema.table.TableInfo fromExternalToWrapperTableInfo(TableInfo externalTableInfo,
                                                                                                      String dbName, String tableName, String storePath) {
                return getTableInfo(1000L);
            }
        };

        new MockUp<Gson>() {
            @Mock
            public <T> T fromJson(Reader json, Class<LoadMetadataDetails[]> classOfT) {
                LoadMetadataDetails[] loadFolderDetailsArray = new LoadMetadataDetails[2];
                loadFolderDetailsArray[0] = new LoadMetadataDetails();
                loadFolderDetailsArray[0].setLoadStatus("success");
                loadFolderDetailsArray[0].setMergedLoadName("1");
                loadFolderDetailsArray[1] = new LoadMetadataDetails();
                loadFolderDetailsArray[1].setLoadStatus("failure");
                loadFolderDetailsArray[1].setMergedLoadName("1");
                return (T) loadFolderDetailsArray;
            }
        };

        new MockUp<FilterExpressionProcessor>() {
            @Mock
            public List<DataRefNode> getFilterredBlocks(DataRefNode btreeNode,
                                                        FilterResolverIntf filterResolver, AbstractIndex tableSegment,
                                                        AbsoluteTableIdentifier tableIdentifier) {

                List<DataRefNode> dataRefNodes = new ArrayList<>();
                List<DataFileFooter> dataFileFooters = new ArrayList<>();
                DataFileFooter dataFileFooter = new DataFileFooter();
                BlockletIndex blockletIndex = new BlockletIndex(new BlockletBTreeIndex(), new BlockletMinMaxIndex());
                dataFileFooter.setBlockletIndex(blockletIndex);
                dataFileFooters.add(dataFileFooter);
                int[] dimColValueSize = {10};
                BlockBTreeLeafNode blockBTreeLeafNode = new BlockBTreeLeafNode(new BTreeBuilderInfo(dataFileFooters, dimColValueSize), 0, 0L);
                dataRefNodes.add(blockBTreeLeafNode);
                return dataRefNodes;
            }
        };

        new MockUp<SegmentTaskIndexStore>() {
            @Mock
            public SegmentTaskIndexWrapper get(TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier)
                    throws IOException {
                Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> taskIdToTableSegmentMap = new HashMap<>();
                List<ColumnSchema> columnsInTable = new ArrayList<>();
                columnsInTable.add(new ColumnSchema());
                int[] colCardinality = {1};

                taskIdToTableSegmentMap.put(new SegmentTaskIndexStore.TaskBucketHolder("1", "1"), new SegmentTaskIndex(new SegmentProperties(columnsInTable, colCardinality)));
                return new SegmentTaskIndexWrapper(taskIdToTableSegmentMap);
            }
        };

        new MockUp<BlockBTreeLeafNode>() {
            @Mock
            public TableBlockInfo getTableBlockInfo() {
                String[] locations = new String[]{"storePath"};
                String[] deletedFilePath = new String[]{""};

                TableBlockInfo tableBlockInfo = new TableBlockInfo("storePath", 0L, "1", locations, 1024L, ColumnarFormatVersion.V3, deletedFilePath);
                return tableBlockInfo;
            }
        };

        new MockUp<FilterExpressionProcessor>() {
            @Mock
            public BitSet getFilteredPartitions(Expression expressionTree,
                                                PartitionInfo partitionInfo) {
                BitSet bitSet = new BitSet(Integer.parseInt("10000"));
                bitSet.set(0);
                return bitSet;
            }
        };
    }

    @After
    public void tearDown() {
        file.delete();
        tableStatusFile.delete();
        dictFile.delete();
    }

    private ColumnSchema getColumnarDimensionColumn() {
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


    private ColumnSchema getColumnarMeasureColumn() {
        ColumnSchema dimColumn = new ColumnSchema();
        dimColumn.setColumnName("id");
        dimColumn.setColumnUniqueId(UUID.randomUUID().toString());
        dimColumn.setDataType(DataType.INT);
        return dimColumn;
    }

    private TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema();
        List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
        columnSchemaList.add(getColumnarMeasureColumn());
        columnSchemaList.add(getColumnarDimensionColumn());
        tableSchema.setListOfColumns(columnSchemaList);
        tableSchema.setTableName("tableName");
        return tableSchema;
    }

    private org.apache.carbondata.core.metadata.schema.table.TableInfo getTableInfo(long timeStamp) {
        org.apache.carbondata.core.metadata.schema.table.TableInfo info = new org.apache.carbondata.core.metadata.schema.table.TableInfo();
        info.setDatabaseName("schemaName");
        info.setLastUpdatedTime(timeStamp);
        info.setTableUniqueName("schemaName_tableName");
        info.setFactTable(getTableSchema());
        info.setStorePath("storePath");
        PartitionInfo partitionInfo = new PartitionInfo(info.getFactTable().getListOfColumns(), PartitionType.LIST);
        List partitionList = new ArrayList();
        partitionList.add(1);
        partitionInfo.setPartitionIds(partitionList);
        info.getFactTable().setPartitionInfo(partitionInfo);
        return info;
    }

    @Test
    public void getCarbonCacheTest() {

        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.LOCAL;
            }

            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                return new LocalCarbonFile("storePath");
            }

            @Mock
            public DataInputStream getDataInputStream(String path, FileFactory.FileType fileType, int bufferSize)
                    throws IOException {
                return new DataInputStream(new FileInputStream(file));
            }
        };

        new MockUp<LocalCarbonFile>() {
            @Mock
            public CarbonFile[] listFiles() {
                CarbonFile[] carbonFiles = {new LocalCarbonFile("storePath")};
                return carbonFiles;
            }
        };

        new MockUp<PathFactory>() {
            @Mock
            public CarbonTablePath getCarbonTablePath(
                    String storeLocation, CarbonTableIdentifier tableIdentifier, DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {

                return new CarbonTablePath("storePath", "schemaName", "tableName");
            }
        };

        CarbonTableCacheModel carbonTableCacheModel = carbonTableReader.getCarbonCache(new SchemaTableName("schemaName", "tableName"));
        assertEquals(carbonTableCacheModel.tableInfo, getTableInfo(1000L));
        assertEquals(carbonTableCacheModel.carbonTablePath, new CarbonTablePath("storePath", "schemaName", "tableName"));
    }

    @Test(expected = RuntimeException.class)
    public void getCarbonCacheExceptionCase() {
        new MockUp<FileFactory>() {
            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) throws FileNotFoundException {
                throw new FileNotFoundException("");
            }

            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.LOCAL;
            }

        };
        CarbonTableCacheModel carbonTableCacheModel = carbonTableReader.getCarbonCache(new SchemaTableName("schemaName", "tableName"));
    }

    @Test
    public void getSchemaNamesTest() {

        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.LOCAL;
            }

            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                return new LocalCarbonFile("storePath/schemaName");
            }
        };

        new MockUp<LocalCarbonFile>() {
            @Mock
            public CarbonFile[] listFiles() {
                CarbonFile[] carbonFiles = {new LocalCarbonFile("storePath/schemaName")};
                return carbonFiles;
            }
        };

        List<String> schemaNames = carbonTableReader.getSchemaNames();
        assertEquals(schemaNames.get(0), "schemaName");
    }

    @Test(expected = TableNotFoundException.class)
    public void getTableTestExceptionCaseForTableNotFound() {
        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.LOCAL;
            }

            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                return new LocalCarbonFile("storePath/schemaName");
            }
        };
        carbonTableReader.getTable(new SchemaTableName("schemaName", "table"));
    }

    @Test(expected = RuntimeException.class)
    public void getTableExceptionCase() {
        new MockUp<FileFactory>() {
            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) throws FileNotFoundException {
                throw new FileNotFoundException("");
            }
        };
        carbonTableReader.getTable(new SchemaTableName("schemaName", "table"));
    }

    @Test
    public void getTableTest() {
        new MockUp<FileFactory>() {
            @Mock
            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.LOCAL;
            }

            @Mock
            public CarbonFile getCarbonFile(String path, FileFactory.FileType fileType) {
                return new LocalCarbonFile("storePath/schemaname/tablename");
            }
        };

        new MockUp<PathFactory>() {
            @Mock
            public CarbonTablePath getCarbonTablePath(
                    String storeLocation, CarbonTableIdentifier tableIdentifier, DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {

                return new CarbonTablePath("storePath", "schemaName", "tableName");
            }
        };

        new MockUp<CarbonTablePath>() {
            @Mock
            public String getSchemaFilePath() {
                return "schema";
            }
        };

        new MockUp<CarbonTablePath>() {
            @Mock
            public String getFolderContainingFile(String carbonFilePath) {
                return "/schema";
            }
        };

        new MockUp<CarbonMetadata>() {
            @Mock
            public CarbonTable getCarbonTable(String tableUniqueName) {
                return CarbonTable.buildFromTableInfo(getTableInfo(1000L));
            }
        };

        CarbonTable expectedResult = carbonTableReader.getTable(new SchemaTableName("schemaName", "schemaName"));

        assertEquals(expectedResult.getStorePath(), "storePath");
        assertEquals(expectedResult.getCarbonTableIdentifier().getDatabaseName(), "schemaName");
    }

    @Test
    public void getInputSplits2TestNoValidSegmentsCase() throws Exception {
        CarbonTableCacheModel carbonTableCacheModel = new CarbonTableCacheModel();
        Expression inputFilter = new AndExpression(new ColumnExpression("id", DataType.INT), new LiteralExpression(1, DataType.INT));
        carbonTableCacheModel.carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(1000L));

        List<CarbonLocalInputSplit> expectedResult = carbonTableReader.getInputSplits2(carbonTableCacheModel, inputFilter);
        assertEquals(expectedResult.size(), 0);
    }

    @Test
    public void getInputSplits2Test() throws Exception {
        CarbonTableCacheModel carbonTableCacheModel = new CarbonTableCacheModel();
        Expression inputFilter = new AndExpression(new ColumnExpression("id", DataType.INT), new LiteralExpression(1, DataType.INT));
        carbonTableCacheModel.carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(1000L));
        carbonTableCacheModel.carbonTablePath = new CarbonTablePath("/storePath", "schemaName", "tableName");
        new MockUp<FileFactory>() {
            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) {
                return true;
            }
        };

        new MockUp<CarbonTablePath>() {
            @Mock
            public String getTableStatusFilePath() {
                return "tablestatus";
            }

            @Mock
            public String getCarbonDataDirectoryPath(String partitionId, String segmentId) {
                return "dictFile";
            }

            @Mock
            public boolean isCarbonDataFile(String fileNameWithPath) {
                return true;
            }
        };

        new MockUp<CarbonTablePath.DataPathUtil>() {
            @Mock
            public String getSegmentId(String dataFileAbsolutePath) {
                return "1";
            }
        };

        new MockUp<CarbonInputFormatUtil>() {
            @Mock
            public FilterResolverIntf resolveFilter(Expression filterExpression,
                                                    AbsoluteTableIdentifier absoluteTableIdentifier, TableProvider tableProvider) {
                return new ConditionalFilterResolverImpl(inputFilter, true, false, new AbsoluteTableIdentifier("/storePath", new CarbonTableIdentifier("schemaName", "tableName", "tableId")), false);
            }
        };

        new MockUp<FileStatus>() {
            @Mock
            public long getLen() {
                return 20L;
            }
        };

        new MockUp<SegmentTaskIndexStore>() {
            @Mock
            public void clearAccessCount(List<TableSegmentUniqueIdentifier> tableSegmentUniqueIdentifiers) {
            }
        };

        new MockUp<CarbonTableInputFormat>() {
            @Mock public List<InputSplit> getSplits(JobContext job) {

                String[] locations = {"storePath"};
                String[] deleteDeltaFiles = {""};
                InputSplit inputSplit = new CarbonInputSplit("1", new Path("storePath"), 0, 10, locations, 2, ColumnarFormatVersion.V3, deleteDeltaFiles);
                List<InputSplit> inputSplits = new ArrayList<>();
                inputSplits.add(inputSplit);
                return inputSplits;
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "taskNo";
            }
        };

        List<CarbonLocalInputSplit> expectedResult = carbonTableReader.getInputSplits2(carbonTableCacheModel, inputFilter);
        assertEquals(expectedResult.get(0).getSegmentId(), "1");
        assertEquals(expectedResult.get(0).getPath(), "storePath");
    }

    @Test
    public void getInputSplits2TestForUpdatedSegment() throws Exception {
        CarbonTableCacheModel carbonTableCacheModel = new CarbonTableCacheModel();
        Expression inputFilter = new AndExpression(new ColumnExpression("id", DataType.INT), new LiteralExpression(1, DataType.INT));
        carbonTableCacheModel.carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(1000L));
        carbonTableCacheModel.carbonTablePath = new CarbonTablePath("/storePath", "schemaName", "tableName");

        new MockUp<FileFactory>() {
            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) {
                return true;
            }
        };

        new MockUp<CarbonTablePath>() {
            @Mock
            public String getTableStatusFilePath() {
                return "tablestatus";
            }

            @Mock
            public String getCarbonDataDirectoryPath(String partitionId, String segmentId) {
                return "dictFile";
            }

            @Mock
            public boolean isCarbonDataFile(String fileNameWithPath) {
                return true;
            }
        };

        new MockUp<CarbonTablePath.DataPathUtil>() {
            @Mock
            public String getSegmentId(String dataFileAbsolutePath) {
                return "1";
            }
        };

        new MockUp<CarbonInputFormatUtil>() {
            @Mock
            public FilterResolverIntf resolveFilter(Expression filterExpression,
                                                    AbsoluteTableIdentifier absoluteTableIdentifier, TableProvider tableProvider) {
                return new ConditionalFilterResolverImpl(inputFilter, true, false, new AbsoluteTableIdentifier("/storePath", new CarbonTableIdentifier("schemaName", "tableName", "tableId")), false);
            }
        };

        new MockUp<CacheAccessClient<TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper>>() {
            @Mock
            public SegmentTaskIndexWrapper getIfPresent(TableSegmentUniqueIdentifier key) {
                Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> taskIdToTableSegmentMap = new HashMap<>();
                List<ColumnSchema> columnsInTable = new ArrayList<>();
                columnsInTable.add(new ColumnSchema());
                int[] colCardinality = {1};

                taskIdToTableSegmentMap.put(new SegmentTaskIndexStore.TaskBucketHolder("1", "1"), new SegmentTaskIndex(new SegmentProperties(columnsInTable, colCardinality)));
                SegmentTaskIndexWrapper segmentTaskIndexWrapper = new SegmentTaskIndexWrapper(taskIdToTableSegmentMap);
                segmentTaskIndexWrapper.setRefreshedTimeStamp(1000L);
                return segmentTaskIndexWrapper;
            }
        };

        new MockUp<SegmentUpdateStatusManager>() {
            @Mock
            public SegmentUpdateDetails[] getUpdateStatusDetails() {
                SegmentUpdateDetails[] segmentUpdateDets = new SegmentUpdateDetails[1];
                segmentUpdateDets[0] = new SegmentUpdateDetails();
                return segmentUpdateDets;
            }

            @Mock
            public boolean isBlockValid(String segName, String blockName) {
                return true;
            }
        };

        new MockUp<UpdateVO>() {
            @Mock
            public Long getLatestUpdateTimestamp() {
                return 2000L;
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "1";
            }
        };
        new MockUp<Path>() {
            @Mock
            public String getName() {
                return "dictFile.dict";
            }
        };

        new MockUp<CarbonUtil>() {
            @Mock
            public boolean isInvalidTableBlock(String segmentId, String filePath,
                                               UpdateVO invalidBlockVOForSegmentId, SegmentUpdateStatusManager updateStatusMngr) {
                return false;
            }
        };

        new MockUp<SegmentTaskIndexStore>() {
            @Mock
            public void clearAccessCount(List<TableSegmentUniqueIdentifier> tableSegmentUniqueIdentifiers) {
            }
        };

        new MockUp<CarbonTableInputFormat>() {
            @Mock public List<InputSplit> getSplits(JobContext job) {

                String[] locations = {"storePath"};
                String[] deleteDeltaFiles = {""};
                InputSplit inputSplit = new CarbonInputSplit("1", new Path("storePath"), 0, 10, locations, 2, ColumnarFormatVersion.V3, deleteDeltaFiles);
                List<InputSplit> inputSplits = new ArrayList<>();
                inputSplits.add(inputSplit);
                return inputSplits;
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "taskNo";
            }
        };

        List<CarbonLocalInputSplit> expectedResult = carbonTableReader.getInputSplits2(carbonTableCacheModel, inputFilter);
        assertEquals(expectedResult.get(0).getSegmentId(), "1");
        assertEquals(expectedResult.get(0).getPath(), "storePath");
    }

    @Test
    public void getInputSplits2TestWhenNoFilterIsPresent() throws Exception {
        CarbonTableCacheModel carbonTableCacheModel = new CarbonTableCacheModel();
        Expression inputFilter = new AndExpression(new ColumnExpression("id", DataType.INT), new LiteralExpression(1, DataType.INT));
        carbonTableCacheModel.carbonTable = CarbonTable.buildFromTableInfo(getTableInfo(1000L));
        carbonTableCacheModel.carbonTablePath = new CarbonTablePath("/storePath", "schemaName", "tableName");
        new MockUp<FileFactory>() {
            @Mock
            public boolean isFileExist(String filePath, FileFactory.FileType fileType) {
                return true;
            }
        };

        new MockUp<CarbonTablePath>() {
            @Mock
            public String getTableStatusFilePath() {
                return "tablestatus";
            }

            @Mock
            public String getCarbonDataDirectoryPath(String partitionId, String segmentId) {
                return "dictFile";
            }

            @Mock
            public boolean isCarbonDataFile(String fileNameWithPath) {
                return true;
            }
        };

        new MockUp<CarbonTablePath.DataPathUtil>() {
            @Mock
            public String getSegmentId(String dataFileAbsolutePath) {
                return "1";
            }
        };

        new MockUp<CarbonInputFormatUtil>() {
            @Mock
            public FilterResolverIntf resolveFilter(Expression filterExpression,
                                                    AbsoluteTableIdentifier absoluteTableIdentifier, TableProvider tableProvider) {
                return null;
            }
        };

        new MockUp<FileStatus>() {
            @Mock
            public long getLen() {
                return 335544322;
            }
        };

        new MockUp<SegmentTaskIndexStore>() {
            @Mock
            public void clearAccessCount(List<TableSegmentUniqueIdentifier> tableSegmentUniqueIdentifiers) {
            }
        };

        new MockUp<BTreeDataRefNodeFinder>() {
            BlockBTreeLeafNode blockBTreeLeafNode;

            @Mock
            public DataRefNode findFirstDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {
                List<DataFileFooter> dataFileFooters = new ArrayList<>();
                DataFileFooter dataFileFooter = new DataFileFooter();
                BlockletIndex blockletIndex = new BlockletIndex(new BlockletBTreeIndex(), new BlockletMinMaxIndex());
                dataFileFooter.setBlockletIndex(blockletIndex);
                dataFileFooters.add(dataFileFooter);
                int[] dimColValueSize = {10};
                BlockBTreeLeafNode blockBTreeLeafNode = new BlockBTreeLeafNode(new BTreeBuilderInfo(dataFileFooters, dimColValueSize), 0, 0L);
                blockBTreeLeafNode.setNextNode(blockBTreeLeafNode);
                this.blockBTreeLeafNode = blockBTreeLeafNode;
                return blockBTreeLeafNode;
            }

            @Mock
            public DataRefNode findLastDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {
                return this.blockBTreeLeafNode;
            }
        };

        new MockUp<CarbonTableInputFormat>() {
            @Mock public List<InputSplit> getSplits(JobContext job) {

                String[] locations = {"storePath"};
                String[] deleteDeltaFiles = {""};
                InputSplit inputSplit = new CarbonInputSplit("1", new Path("storePath"), 0, 10, locations, 2, ColumnarFormatVersion.V3, deleteDeltaFiles);
                List<InputSplit> inputSplits = new ArrayList<>();
                inputSplits.add(inputSplit);
                return inputSplits;
            }
        };

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "taskNo";
            }
        };

        List<CarbonLocalInputSplit> expectedResult = carbonTableReader.getInputSplits2(carbonTableCacheModel, inputFilter);
        assertEquals(expectedResult.get(0).getSegmentId(), "1");
        assertEquals(expectedResult.get(0).getPath(), "storePath");
    }
}

