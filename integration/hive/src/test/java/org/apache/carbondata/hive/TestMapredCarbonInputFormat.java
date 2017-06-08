package org.apache.carbondata.hive;

import mockit.Mock;
import mockit.MockUp;
import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.*;
import org.apache.carbondata.core.datastore.chunk.reader.CarbonDataReaderFactory;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.datastore.chunk.reader.dimension.v3.CompressedDimensionChunkFileBasedReaderV3;
import org.apache.carbondata.core.datastore.chunk.reader.measure.v3.CompressedMeasureChunkFileBasedReaderV3;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileHolderImpl;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.datastore.impl.btree.BlockletBTreeLeafNode;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.collector.ResultCollectorFactory;
import org.apache.carbondata.core.scan.collector.impl.AbstractScannedResultCollector;
import org.apache.carbondata.core.scan.collector.impl.DictionaryBasedResultCollector;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.impl.DetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.iterator.DetailQueryResultIterator;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.stats.QueryStatisticsRecorderImpl;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputFormat;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestMapredCarbonInputFormat {

    private static MapredCarbonInputFormat mapredCarbonInputFormat;
    private static JobConf jobConf;
    private static String[] locations = new String[]{"location1", "location2", "location3"};
    private static InputSplit inputSplit;
    private static TableBlockInfo tableBlockInfo;
    Path path = new Path("defaultPath");

    @BeforeClass
    public static void setUp() throws IOException {
        mapredCarbonInputFormat = new MapredCarbonInputFormat();
        jobConf = new JobConf();
        jobConf.set("hive.io.file.readcolumn.ids", "0,1");
        jobConf.set("hive.io.file.readcolumn.names", "id,name");
        jobConf.set(serdeConstants.LIST_COLUMN_TYPES, "int,string");
        new MockUp<BTreeBuilderInfo>() {
            @Mock
            public List<DataFileFooter> getFooterList() {
                DataFileFooter fileFooter = new DataFileFooter();
                List<DataFileFooter> footerList = new ArrayList<>();
                footerList.add(fileFooter);
                return footerList;
            }
        };
        new MockUp<DataFileFooter>() {
            @Mock
            public List<BlockletInfo> getBlockletList() {
                BlockletInfo blockletInfo = new BlockletInfo();
                List<BlockletInfo> blockletList = new ArrayList<>();
                blockletList.add(blockletInfo);
                return blockletList;
            }

            @Mock
            public BlockInfo getBlockInfo() {
                tableBlockInfo = new TableBlockInfo("/default/filePath.carbondata", 1L, "segmentID", locations, 2, ColumnarFormatVersion.V3);
                return new BlockInfo(tableBlockInfo);
            }


        };

        new MockUp<BlockletInfo>() {
            @Mock
            public BlockletIndex getBlockletIndex() {
                return new BlockletIndex();
            }
        };

        new MockUp<BlockletIndex>() {
            @Mock
            public BlockletMinMaxIndex getMinMaxIndex() {
                System.out.println("hello2");
                return new BlockletMinMaxIndex();
            }
        };
    }

    @Test
    public void testGetSplits() throws IOException {

        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "1";
            }
        };
        new MockUp<CarbonInputFormat>() {
            @Mock
            public List<InputSplit> getSplits(JobContext job) throws IOException {
                List<InputSplit> list = new ArrayList<InputSplit>();
                list.add(inputSplit);
                return list;
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

                Map<String, String> map = new HashMap<String, String>(3);
                map.put("block1", "value1");
                map.put("block2", "value2");
                map.put("block3", "value3");
                return map;
            }
        };

        inputSplit = new CarbonInputSplit("segment", path, 1, 3, locations, 4, ColumnarFormatVersion.V3);
        String expected = "[defaultPath:1+1]";
        assertEquals(Arrays.deepToString(mapredCarbonInputFormat.getSplits(jobConf, 2)), expected);
    }

    @Test
    public void testGetRecordReader() throws IOException {

        Reporter reporter = new Reporter() {
            @Override
            public void setStatus(String s) {

            }

            @Override
            public Counters.Counter getCounter(Enum<?> anEnum) {
                return null;
            }

            @Override
            public Counters.Counter getCounter(String s, String s1) {
                return null;
            }

            @Override
            public void incrCounter(Enum<?> anEnum, long l) {

            }

            @Override
            public void incrCounter(String s, String s1, long l) {

            }

            @Override
            public org.apache.hadoop.mapred.InputSplit getInputSplit() throws UnsupportedOperationException {
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
        new MockUp<CarbonTablePath.DataFileUtil>() {
            @Mock
            public String getTaskNo(String carbonDataFileName) {
                return "1";
            }
        };

        new MockUp<CarbonInputFormat>() {

            @Mock
            public CarbonTable getCarbonTable(Configuration configuration) throws IOException {
                CarbonTable carbonTable = new CarbonTable();
                return carbonTable;
            }
        };

        new MockUp<CarbonTable>() {

            @Mock
            public String getDatabaseName() {
                return "defaultDatabase";
            }

            @Mock
            public String getFactTableName() {
                return "factTable";
            }
        };

        new MockUp<QueryExecutorFactory>() {
            @Mock
            public QueryExecutor getQueryExecutor(QueryModel queryModel) {
                return new DetailQueryExecutor();
            }
        };

        new MockUp<DetailQueryExecutor>() {
            @Mock
            public CarbonIterator<BatchResult> execute(QueryModel queryModel) {
                queryModel = new QueryModel();
                BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
                List<BlockExecutionInfo> blockExecutionInfoList = new ArrayList<>();
                blockExecutionInfoList.add(blockExecutionInfo);
                ExecutorService executorService = new ExecutorService() {
                    @Override
                    public void shutdown() {

                    }

                    @Override
                    public List<Runnable> shutdownNow() {
                        return null;
                    }

                    @Override
                    public boolean isShutdown() {
                        return false;
                    }

                    @Override
                    public boolean isTerminated() {
                        return false;
                    }

                    @Override
                    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                        return false;
                    }

                    @Override
                    public <T> Future<T> submit(Callable<T> task) {
                        return null;
                    }

                    @Override
                    public <T> Future<T> submit(Runnable task, T result) {
                        return null;
                    }

                    @Override
                    public Future<?> submit(Runnable task) {
                        return null;
                    }

                    @Override
                    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
                        return null;
                    }

                    @Override
                    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
                        return null;
                    }

                    @Override
                    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
                        return null;
                    }

                    @Override
                    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                        return null;
                    }

                    @Override
                    public void execute(Runnable command) {

                    }
                };
                return new DetailQueryResultIterator(blockExecutionInfoList, queryModel, executorService);
            }
        };

        new MockUp<QueryModel>() {
            @Mock
            public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
                CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("db", "mytable", "id");
                return new AbsoluteTableIdentifier("", carbonTableIdentifier);
            }
        };

        new MockUp<AbsoluteTableIdentifier>() {
            @Mock
            public String getStorePath() {
                return "defaultPath";
            }
        };

        new MockUp<FileFactory>() {
            @Mock
            public FileHolder getFileHolder(FileFactory.FileType fileType) {
                return new FileHolderImpl();
            }

            public FileFactory.FileType getFileType(String path) {
                return FileFactory.FileType.LOCAL;
            }

        };

        new MockUp<BlockExecutionInfo>() {
            @Mock
            public void setBlockId(String blockId) {

            }

            @Mock
            public AbstractIndex getDataBlock() {
                ColumnSchema columnSchema = new ColumnSchema();
                List<ColumnSchema> columnsInTable = new ArrayList<>();
                columnsInTable.add(columnSchema);
                int[] columnCardinality = new int[]{1, 2, 3, 4};
                SegmentProperties segmentProperties = new SegmentProperties(columnsInTable, columnCardinality);
                AbstractIndex abstractIndex = new SegmentTaskIndex(segmentProperties);
                return abstractIndex;
            }
        };
        new MockUp<DataFileFooter>() {
            @Mock
            public BlockletIndex getBlockletIndex() {
                return new BlockletIndex();
            }

        };

        new MockUp<BlockletIndex>() {
            @Mock
            public BlockletMinMaxIndex getMinMaxIndex() {
                return new BlockletMinMaxIndex();
            }

        };

        new MockUp<BTreeDataRefNodeFinder>() {
            @Mock
            public DataRefNode findFirstDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {

                DataFileFooter dataFileFooter = new DataFileFooter();
                List<DataFileFooter> dataFileFooters = new ArrayList<>();
                dataFileFooters.add(dataFileFooter);
                int[] dimensionColumnValueSize = new int[]{1, 2, 3, 4, 5};
                BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(dataFileFooters, dimensionColumnValueSize);
                return new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 2L);
            }


            @Mock
            public DataRefNode findLastDataBlock(DataRefNode dataRefBlock, IndexKey searchKey) {
                DataFileFooter dataFileFooter = new DataFileFooter();
                List<DataFileFooter> dataFileFooters = new ArrayList<>();
                dataFileFooters.add(dataFileFooter);
                int[] dimensionColumnValueSize = new int[]{1, 2, 3, 4, 5};
                BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(dataFileFooters, dimensionColumnValueSize);
                BlockBTreeLeafNode blockBTreeLeafNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 0, 1L);

                return blockBTreeLeafNode;
            }
        };

        new MockUp<CarbonDataReaderFactory>() {
            @Mock
            public DimensionColumnChunkReader getDimensionColumnChunkReader(ColumnarFormatVersion version,
                                                                            BlockletInfo blockletInfo, int[] eachColumnValueSize, String filePath) {
                blockletInfo = new BlockletInfo();
                eachColumnValueSize = new int[]{1, 2, 3, 4};
                filePath = "defaultPath";
                DimensionColumnChunkReader dimensionColumnChunkReader = new CompressedDimensionChunkFileBasedReaderV3(blockletInfo, eachColumnValueSize, filePath);
                return dimensionColumnChunkReader;
            }

            @Mock
            public MeasureColumnChunkReader getMeasureColumnChunkReader(ColumnarFormatVersion version,
                                                                        BlockletInfo blockletInfo, String filePath) {
                MeasureColumnChunkReader measureColumnChunkReader = new CompressedMeasureChunkFileBasedReaderV3(blockletInfo, filePath);
                return measureColumnChunkReader;
            }
        };

        new MockUp<BlockInfo>() {

            @Mock
            public TableBlockInfo getTableBlockInfo() {
                tableBlockInfo = new TableBlockInfo("filePath", 1L, "segmentID", locations, 0, ColumnarFormatVersion.V3);
                return tableBlockInfo;
            }
        };

        new MockUp<CarbonDataReaderFactory>() {
            public MeasureColumnChunkReader getMeasureColumnChunkReader(ColumnarFormatVersion version,
                                                                        BlockletInfo blockletInfo, String filePath) {
                MeasureColumnChunkReader measureColumnChunkReader = new CompressedMeasureChunkFileBasedReaderV3(blockletInfo, filePath);
                return measureColumnChunkReader;
            }
        };

        new MockUp<QueryStatisticsModel>() {
            @Mock
            public Map<String, QueryStatistic> getStatisticsTypeAndObjMap() {
                Map<String, QueryStatistic> statisticsTypeAndObjMap = new HashedMap();
                QueryStatistic queryStatistic = new QueryStatistic();
                statisticsTypeAndObjMap.put("key1", queryStatistic);
                return statisticsTypeAndObjMap;
            }

            @Mock
            public QueryStatisticsRecorder getRecorder() {
                return new QueryStatisticsRecorderImpl("queryId");
            }
        };
        new MockUp<QueryStatisticsRecorderImpl>() {
            @Mock
            public synchronized void recordStatistics(QueryStatistic statistic) {

            }
        };

        new MockUp<BlockExecutionInfo>() {
            @Mock
            public QueryDimension[] getQueryDimensions() {
                QueryDimension queryDimension = new QueryDimension("id");
                QueryDimension[] queryDimensions = new QueryDimension[]{queryDimension};
                return queryDimensions;
            }

            @Mock
            public QueryMeasure[] getQueryMeasures() {
                QueryMeasure queryMeasure = new QueryMeasure("name");
                QueryMeasure[] queryMeasures = new QueryMeasure[]{queryMeasure};
                return queryMeasures;
            }

        };

        new MockUp<QueryDimension>() {
            @Mock
            public CarbonDimension getDimension() {
                ColumnSchema columnSchema = new ColumnSchema();
                return new CarbonDimension(columnSchema, 1, 2, 3, 4, 5);
            }
        };

        new MockUp<CarbonDimension>() {
            @Mock
            public boolean hasEncoding(Encoding encoding) {
                return true;
            }
        };

        new MockUp<ResultCollectorFactory>() {
            @Mock
            public AbstractScannedResultCollector getScannedResultCollector(
                    BlockExecutionInfo blockExecutionInfo) {
                return new DictionaryBasedResultCollector(blockExecutionInfo);

            }
        };

        new MockUp<CarbonUtil>() {
            @Mock
            public boolean[] getComplexDataTypeArray(QueryDimension[] queryDimensions) {
                boolean[] dictionaryEncodingArray = new boolean[]{true, true, true};
                return dictionaryEncodingArray;
            }
        };

        new MockUp<DirectDictionaryKeyGeneratorFactory>() {
            @Mock
            public DirectDictionaryGenerator getDirectDictionaryGenerator(DataType dataType) {
                return new DateDirectDictionaryGenerator("DD/MM/YYYY");
            }
        };

        new MockUp<DetailQueryResultIterator>() {
            @Mock
            public BatchResult next() {
                return new BatchResult();
            }
        };

        new MockUp<CarbonInputFormatUtil>() {
            @Mock
            public CarbonQueryPlan createQueryPlan(CarbonTable carbonTable, String columnString) {
                return new CarbonQueryPlan("default", "sample");
            }
        };
        CarbonHiveInputSplit inputSplit = new CarbonHiveInputSplit("segment", path, 1, 3, locations, 4, ColumnarFormatVersion.V3);

        assertNotNull(mapredCarbonInputFormat.getRecordReader(inputSplit, jobConf, reporter));
    }

    @Test
    public void testShouldSkipCombine() throws IOException {
        Configuration conf = new HiveConf();
        assertTrue(mapredCarbonInputFormat.shouldSkipCombine(path,conf));
    }
}
