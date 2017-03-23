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
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.*;
import org.apache.carbondata.core.datastore.block.*;
import org.apache.carbondata.core.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterExpressionProcessor;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.service.impl.PathFactory;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CacheClient;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.TBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import static java.util.Objects.requireNonNull;

public class CarbonTableReader {
    //CarbonTableReader will be a facade of these utils
    //[
    // 1:CarbonMetadata,(logic table)
    // 2:FileFactory, (physic table file)
    // 3:CarbonCommonFactory, (offer some )
    // 4:DictionaryFactory, (parse dictionary util)
    //]

    private CarbonTableConfig config;
    private List<SchemaTableName> tableList;
    private CarbonFile dbStore;
    private FileFactory.FileType fileType;

    private ConcurrentHashMap<SchemaTableName, CarbonTableCacheModel> cc;//as a cache for Carbon reader

    @Inject
    public CarbonTableReader(CarbonTableConfig config){
        this.config = requireNonNull(config, "CarbonTableConfig is null");
        this.cc = new ConcurrentHashMap<>();
    }

    public CarbonTableCacheModel getCarbonCache(SchemaTableName table){
        if(!cc.containsKey(table))//for worker node to initalize carbon metastore
        {
            try(ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileFactory.class.getClassLoader())) {
                if(dbStore == null) {
                    fileType = FileFactory.getFileType(config.getStorePath());
                    try{
                        dbStore = FileFactory.getCarbonFile(config.getStorePath(), fileType);
                    }catch (Exception ex){
                        throw new RuntimeException(ex);
                    }
                }
            }
            updateSchemaTables();
            parseCarbonMetadata(table);
        }

        if(cc.containsKey(table))
            return cc.get(table);
        else
            return null;//need to reload?*/
    }

    public List<String> getSchemaNames() {
        return updateSchemaList();
    }

    //default PathFilter
    private static final PathFilter DefaultFilter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            return CarbonTablePath.isCarbonDataFile(path.getName());
        }
    };

    public boolean updateDbStore(){
        if(dbStore == null) {
            fileType = FileFactory.getFileType(config.getStorePath());
            try{
                dbStore = FileFactory.getCarbonFile(config.getStorePath(), fileType);
            }catch (Exception ex){
                throw new RuntimeException(ex);
            }
        }
        return true;
    }

    public List<String> updateSchemaList() {
        updateDbStore();

        if(dbStore != null){
            List<String> scs = Stream.of(dbStore.listFiles()).map(a -> a.getName()).collect(Collectors.toList());
            return scs;
        }
        else
            return ImmutableList.of();
    }


    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        return updateTableList(schema);
    }

    public Set<String> updateTableList(String dbName){
        List<CarbonFile> schema = Stream.of(dbStore.listFiles()).filter(a -> dbName.equals(a.getName())).collect(Collectors.toList());
        if(schema.size() > 0)
        {
            return Stream.of((schema.get(0)).listFiles()).map(a -> a.getName()).collect(Collectors.toSet());
        }
        else
            return ImmutableSet.of();
    }

    public CarbonTable getTable(SchemaTableName schemaTableName) {
        try {
            updateSchemaTables();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        requireNonNull(schemaTableName, "schemaTableName is null");
        CarbonTable table = loadTableMetadata(schemaTableName);

        return table;
    }


    public void updateSchemaTables()
    {
        //update logic determine later
        if(dbStore == null)
        {
            updateSchemaList();
        }

        tableList = new LinkedList<>();
        for(CarbonFile db: dbStore.listFiles())
        {
            if(!db.getName().endsWith(".mdt")) {
                for (CarbonFile table : db.listFiles()) {
                    tableList.add(new SchemaTableName(db.getName(), table.getName()));
                }
            }
        }
    }

    private CarbonTable loadTableMetadata(SchemaTableName schemaTableName)
    {
        for (SchemaTableName table : tableList) {
            if (!table.equals(schemaTableName))
                continue;

            return parseCarbonMetadata(table);
        }
        return null;
    }

    /**
     * parse carbon metadata into cc(CarbonTableReader cache)
     **/
    public CarbonTable parseCarbonMetadata(SchemaTableName table)
    {
        CarbonTable result = null;
        try {
            //这个应该放在StoreFactory
            CarbonTableCacheModel cache = cc.getOrDefault(table, new CarbonTableCacheModel());
            if(cache.isValid())
                return cache.carbonTable;

            //Step1: get table meta path, load carbon table param
            String storePath = config.getStorePath();
            cache.carbonTableIdentifier = new CarbonTableIdentifier(table.getSchemaName(), table.getTableName(), UUID.randomUUID().toString());
            cache.carbonTablePath = PathFactory.getInstance().getCarbonTablePath(storePath, cache.carbonTableIdentifier);
            cc.put(table, cache);

            //Step2: check file existed? read schema file
            ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
                public TBase create() {
                    return new org.apache.carbondata.format.TableInfo();
                }
            };
            ThriftReader thriftReader =
                    new ThriftReader(cache.carbonTablePath.getSchemaFilePath(), createTBase);
            thriftReader.open();
            org.apache.carbondata.format.TableInfo tableInfo =
                    (org.apache.carbondata.format.TableInfo) thriftReader.read();
            thriftReader.close();

            //Format Level的TableInfo， 需要转换成Code Level的TableInfo
            SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
            TableInfo wrapperTableInfo = schemaConverter
                    .fromExternalToWrapperTableInfo(tableInfo, table.getSchemaName(), table.getTableName(),
                            storePath);
            wrapperTableInfo.setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(cache.carbonTablePath.getSchemaFilePath()));
            //加载到CarbonMetadata仓库
            CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);

            cache.tableInfo = wrapperTableInfo;
            cache.carbonTable = CarbonMetadata.getInstance().getCarbonTable(cache.carbonTableIdentifier.getTableUniqueName());
            result = cache.carbonTable;
        }catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return result;
    }

    public List<CarbonLocalInputSplit> getInputSplits2(CarbonTableCacheModel tableCacheModel, Expression filters) throws Exception {

        //处理filter, 下推filter，将应用在Segment的索引上
        FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();

        AbsoluteTableIdentifier absoluteTableIdentifier = tableCacheModel.carbonTable.getAbsoluteTableIdentifier();
        CacheClient cacheClient = new CacheClient(absoluteTableIdentifier.getStorePath());
        List<String> invalidSegments = new ArrayList<>();
        List<UpdateVO> invalidTimestampsList = new ArrayList<>();

        // get all valid segments and set them into the configuration
        SegmentUpdateStatusManager updateStatusManager = new SegmentUpdateStatusManager(absoluteTableIdentifier);
        SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
        SegmentStatusManager.ValidAndInvalidSegmentsInfo segments = segmentStatusManager.getValidAndInvalidSegments();

        tableCacheModel.segments = segments.getValidSegments().toArray(new String[0]);
        if (segments.getValidSegments().size() == 0) {
            return new ArrayList<>(0);
        }

        // remove entry in the segment index if there are invalid segments
        invalidSegments.addAll(segments.getInvalidSegments());
        for (String invalidSegmentId : invalidSegments) {
            invalidTimestampsList.add(updateStatusManager.getInvalidTimestampRange(invalidSegmentId));
        }
        if (invalidSegments.size() > 0) {
            List<TableSegmentUniqueIdentifier> invalidSegmentsIds = new ArrayList<>(invalidSegments.size());
            for(String segId: invalidSegments) {
                invalidSegmentsIds.add(new TableSegmentUniqueIdentifier(absoluteTableIdentifier, segId));
            }
            cacheClient.getSegmentAccessClient().invalidateAll(invalidSegmentsIds);
        }

        // get filter for segment
        CarbonInputFormatUtil.processFilterExpression(filters, tableCacheModel.carbonTable);
        FilterResolverIntf filterInterface = CarbonInputFormatUtil.resolveFilter(filters, tableCacheModel.carbonTable.getAbsoluteTableIdentifier());

        List<CarbonLocalInputSplit> result = new ArrayList<>();
        //for each segment fetch blocks matching filter in Driver BTree
        for (String segmentNo : tableCacheModel.segments) {
            try{
                List<DataRefNode> dataRefNodes = getDataBlocksOfSegment(filterExpressionProcessor, absoluteTableIdentifier,tableCacheModel.carbonTablePath, filterInterface, segmentNo, cacheClient, updateStatusManager);
                for (DataRefNode dataRefNode : dataRefNodes) {
                    BlockBTreeLeafNode leafNode = (BlockBTreeLeafNode) dataRefNode;
                    TableBlockInfo tableBlockInfo = leafNode.getTableBlockInfo();

                    if (CarbonUtil.isInvalidTableBlock(tableBlockInfo, updateStatusManager.getInvalidTimestampRange(tableBlockInfo.getSegmentId()), updateStatusManager)) {
                        continue;
                    }
                    result.add(new CarbonLocalInputSplit(segmentNo, tableBlockInfo.getFilePath(),
                            tableBlockInfo.getBlockOffset(), tableBlockInfo.getBlockLength(),
                            Arrays.asList(tableBlockInfo.getLocations()), tableBlockInfo.getBlockletInfos().getNoOfBlockLets(),
                            tableBlockInfo.getVersion().number()));
                }
            }catch (Exception ex){
                throw new RuntimeException(ex);
            }
        }
        cacheClient.close();
        return result;
    }

    /**
     * get data blocks of given segment
     */
    private List<DataRefNode> getDataBlocksOfSegment(FilterExpressionProcessor filterExpressionProcessor,
                                                     AbsoluteTableIdentifier absoluteTableIdentifier,
                                                     CarbonTablePath tablePath,
                                                     FilterResolverIntf resolver,
                                                     String segmentId,
                                                     CacheClient cacheClient,
                                                     SegmentUpdateStatusManager updateStatusManager) throws IOException {
        //DriverQueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.getQueryStatisticsRecorderInstance();
        //QueryStatistic statistic = new QueryStatistic();

        //读取Segment 内部的Index
        Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> segmentIndexMap =
                getSegmentAbstractIndexs(absoluteTableIdentifier, tablePath, segmentId, cacheClient,
                        updateStatusManager);

        List<DataRefNode> resultFilterredBlocks = new LinkedList<DataRefNode>();

        if (null != segmentIndexMap) {
            // build result
            for (AbstractIndex abstractIndex : segmentIndexMap.values()) {
                List<DataRefNode> filterredBlocks;
                // if no filter is given get all blocks from Btree Index
                if (null == resolver) {
                    filterredBlocks = getDataBlocksOfIndex(abstractIndex);
                } else {
                    //ignore filter
                    //filterredBlocks = getDataBlocksOfIndex(abstractIndex);

                    // apply filter and get matching blocks
                    filterredBlocks = filterExpressionProcessor
                            .getFilterredBlocks(abstractIndex.getDataRefNode(), resolver, abstractIndex,
                                    absoluteTableIdentifier);
                }
                resultFilterredBlocks.addAll(filterredBlocks);
            }
        }
        //statistic.addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, System.currentTimeMillis());
        //recorder.recordStatisticsForDriver(statistic, "123456"/*job.getConfiguration().get("query.id")*/);
        return resultFilterredBlocks;
    }

    private boolean isSegmentUpdate(SegmentTaskIndexWrapper segmentTaskIndexWrapper,
                                    UpdateVO updateDetails) {
        if (null != updateDetails.getLatestUpdateTimestamp()
                && updateDetails.getLatestUpdateTimestamp() > segmentTaskIndexWrapper
                .getRefreshedTimeStamp()) {
            return true;
        }
        return false;
    }

    private Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> getSegmentAbstractIndexs(/*JobContext job,*/
                                                                                                AbsoluteTableIdentifier absoluteTableIdentifier,
                                                                                                CarbonTablePath tablePath,
                                                                                                String segmentId,
                                                                                                CacheClient cacheClient,
                                                                                                SegmentUpdateStatusManager updateStatusManager) throws IOException {
        Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> segmentIndexMap = null;
        SegmentTaskIndexWrapper segmentTaskIndexWrapper = null;
        boolean isSegmentUpdated = false;
        Set<SegmentTaskIndexStore.TaskBucketHolder> taskKeys = null;
        TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier =
                new TableSegmentUniqueIdentifier(absoluteTableIdentifier, segmentId);
        segmentTaskIndexWrapper =
                cacheClient.getSegmentAccessClient().getIfPresent(tableSegmentUniqueIdentifier);
        UpdateVO updateDetails = updateStatusManager.getInvalidTimestampRange(segmentId);
        if (null != segmentTaskIndexWrapper) {
            segmentIndexMap = segmentTaskIndexWrapper.getTaskIdToTableSegmentMap();
            if (isSegmentUpdate(segmentTaskIndexWrapper, updateDetails)) {
                taskKeys = segmentIndexMap.keySet();
                isSegmentUpdated = true;
            }
        }

        // if segment tree is not loaded, load the segment tree
        if (segmentIndexMap == null || isSegmentUpdated) {

            List<FileStatus> fileStatusList = new LinkedList<FileStatus>();
            List<String> segs = new ArrayList<>();
            segs.add(segmentId);

            FileSystem fs = getFileStatusOfSegments(new String[]{segmentId}, tablePath, fileStatusList);
            List<InputSplit> splits = getSplit(fileStatusList, fs);

            List<FileSplit> carbonSplits = new ArrayList<>();
            for (InputSplit inputSplit : splits) {
                FileSplit fileSplit = (FileSplit) inputSplit;
                String segId = CarbonTablePath.DataPathUtil.getSegmentId(fileSplit.getPath().toString());//这里的seperator应该怎么加？？
                if (segId.equals(CarbonCommonConstants.INVALID_SEGMENT_ID)) {
                    continue;
                }
                carbonSplits.add(fileSplit);
            }

            List<TableBlockInfo> tableBlockInfoList  = new ArrayList<>();
            for (FileSplit inputSplit : carbonSplits) {
                if (isValidBlockBasedOnUpdateDetails(taskKeys, inputSplit, updateDetails, updateStatusManager, segmentId)) {

                    BlockletInfos blockletInfos = new BlockletInfos(0, 0, 0);//this level we do not need blocklet info!!!! Is this a trick?
                    tableBlockInfoList.add(
                            new TableBlockInfo(inputSplit.getPath().toString(),
                                    inputSplit.getStart(),
                                    segmentId,
                                    inputSplit.getLocations(),
                                    inputSplit.getLength(),
                                    blockletInfos,
                                    ColumnarFormatVersion.valueOf(CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION),
                                    null/*new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE)*/));//这里的null是否会异常？
                }
            }

            Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos = new HashMap<>();
            segmentToTableBlocksInfos.put(segmentId, tableBlockInfoList);
            // get Btree blocks for given segment
            tableSegmentUniqueIdentifier.setSegmentToTableBlocksInfos(segmentToTableBlocksInfos);
            tableSegmentUniqueIdentifier.setIsSegmentUpdated(isSegmentUpdated);
            segmentTaskIndexWrapper =
                    cacheClient.getSegmentAccessClient().get(tableSegmentUniqueIdentifier);
            segmentIndexMap = segmentTaskIndexWrapper.getTaskIdToTableSegmentMap();
        }
        return segmentIndexMap;
    }

    private boolean isValidBlockBasedOnUpdateDetails(
            Set<SegmentTaskIndexStore.TaskBucketHolder> taskKeys, FileSplit carbonInputSplit,
            UpdateVO updateDetails, SegmentUpdateStatusManager updateStatusManager, String segmentId) {
        String taskID = null;
        if (null != carbonInputSplit) {
            if (!updateStatusManager.isBlockValid(segmentId, carbonInputSplit.getPath().getName())) {
                return false;
            }

            if (null == taskKeys) {
                return true;
            }

            taskID = CarbonTablePath.DataFileUtil.getTaskNo(carbonInputSplit.getPath().getName());
            String bucketNo =
                    CarbonTablePath.DataFileUtil.getBucketNo(carbonInputSplit.getPath().getName());

            SegmentTaskIndexStore.TaskBucketHolder taskBucketHolder =
                    new SegmentTaskIndexStore.TaskBucketHolder(taskID, bucketNo);

            String blockTimestamp = carbonInputSplit.getPath().getName()
                    .substring(carbonInputSplit.getPath().getName().lastIndexOf('-') + 1,
                            carbonInputSplit.getPath().getName().lastIndexOf('.'));
            if (!(updateDetails.getUpdateDeltaStartTimestamp() != null
                    && Long.parseLong(blockTimestamp) < updateDetails.getUpdateDeltaStartTimestamp())) {
                if (!taskKeys.contains(taskBucketHolder)) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<InputSplit> getSplit(List<FileStatus> fileStatusList, FileSystem targetSystem)  throws IOException  {

        Iterator split = fileStatusList.iterator();

        List<InputSplit> splits = new ArrayList<>();

        while (true)
        {
            while (true)
            {
                while(split.hasNext()) {
                    FileStatus file = (FileStatus) split.next();
                    Path path = file.getPath();
                    long length = file.getLen();
                    if (length != 0L) {
                        BlockLocation[] blkLocations;
                        if (file instanceof LocatedFileStatus) {
                            blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                        } else {
                            blkLocations = targetSystem.getFileBlockLocations(file, 0L, length);
                        }

                        if (this.isSplitable()) {
                            long blockSize1 = file.getBlockSize();
                            long splitSize = this.computeSplitSize(blockSize1, 1, Long.MAX_VALUE);

                            long bytesRemaining;
                            int blkIndex;
                            for (bytesRemaining = length; (double) bytesRemaining / (double) splitSize > 1.1D; bytesRemaining -= splitSize) {
                                blkIndex = this.getBlockIndex(blkLocations, length - bytesRemaining);
                                splits.add(this.makeSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts()));
                            }

                            if (bytesRemaining != 0L) {
                                blkIndex = this.getBlockIndex(blkLocations, length - bytesRemaining);
                                splits.add(this.makeSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[blkIndex].getHosts()));
                            }
                        }
                        else
                        {
                            splits.add(new org.apache.hadoop.mapreduce.lib.input.FileSplit(path, 0L, length, blkLocations[0].getHosts()));
                        }
                    }
                    else {
                        splits.add(new org.apache.hadoop.mapreduce.lib.input.FileSplit(path, 0L, length, new String[0]));
                    }
                }
                return splits;
            }
        }

    }

    private String[] getValidPartitions() {
        //TODO: has to Identify partitions by partition pruning
        return new String[] { "0" };
    }

    private FileSystem getFileStatusOfSegments(String[] segmentsToConsider,
                                               CarbonTablePath tablePath,
                                               List<FileStatus> result) throws IOException {
        String[] partitionsToConsider = getValidPartitions();
        if (partitionsToConsider.length == 0) {
            throw new IOException("No partitions/data found");
        }

        FileSystem fs = null;

        //PathFilter inputFilter = getDataFileFilter(job);

        // get tokens for all the required FileSystem for table path
        /*TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { tablePath },
                job.getConfiguration());*/

        //get all data files of valid partitions and segments
        for (int i = 0; i < partitionsToConsider.length; ++i) {
            String partition = partitionsToConsider[i];

            for (int j = 0; j < segmentsToConsider.length; ++j) {
                String segmentId = segmentsToConsider[j];
                Path segmentPath = new Path(tablePath.getCarbonDataDirectoryPath(partition, segmentId));

                try{
                    Configuration conf = new Configuration();
                    fs = segmentPath.getFileSystem(conf);
                    //fs.initialize(segmentPath.toUri(), conf);

                    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(segmentPath);
                    while (iter.hasNext()) {
                        LocatedFileStatus stat = iter.next();
                        //if(stat.getPath().toString().contains("carbondata"))//参看carbondata的carbonInputFilter的实现
                        if (DefaultFilter.accept(stat.getPath()))
                        {
                            if (stat.isDirectory()) {
                                addInputPathRecursively(result, fs, stat.getPath(), DefaultFilter);
                            } else {
                                result.add(stat);
                            }
                        }
                    }
                }catch (Exception ex){
                    System.out.println(ex.toString());
                }
            }
        }
        return fs;
    }

    protected void addInputPathRecursively(List<FileStatus> result, FileSystem fs, Path path, PathFilter inputFilter) throws IOException {
        RemoteIterator iter = fs.listLocatedStatus(path);

        while(iter.hasNext()) {
            LocatedFileStatus stat = (LocatedFileStatus)iter.next();
            if(inputFilter.accept(stat.getPath())) {
                if(stat.isDirectory()) {
                    this.addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
                } else {
                    result.add(stat);
                }
            }
        }

    }

    /**
     * get data blocks of given btree
     */
    private List<DataRefNode> getDataBlocksOfIndex(AbstractIndex abstractIndex) {
        List<DataRefNode> blocks = new LinkedList<DataRefNode>();
        SegmentProperties segmentProperties = abstractIndex.getSegmentProperties();

        try {
            IndexKey startIndexKey = FilterUtil.prepareDefaultStartIndexKey(segmentProperties);
            IndexKey endIndexKey = FilterUtil.prepareDefaultEndIndexKey(segmentProperties);

            // Add all blocks of btree into result
            DataRefNodeFinder blockFinder =
                    new BTreeDataRefNodeFinder(segmentProperties.getEachDimColumnValueSize());
            DataRefNode startBlock =
                    blockFinder.findFirstDataBlock(abstractIndex.getDataRefNode(), startIndexKey);
            DataRefNode endBlock =
                    blockFinder.findLastDataBlock(abstractIndex.getDataRefNode(), endIndexKey);
            while (startBlock != endBlock) {
                blocks.add(startBlock);
                startBlock = startBlock.getNextDataRefNode();
            }
            blocks.add(endBlock);

        } catch (KeyGenException e) {
            System.out.println("Could not generate start key" + e.getMessage());
        }
        return blocks;
    }

    private boolean isSplitable() {
        try {
            // Don't split the file if it is local file system
            if(this.fileType == FileFactory.FileType.LOCAL)
            {
                return false;
            }
        } catch (Exception e) {
            return true;
        }
        return true;
    }

    private long computeSplitSize(long blockSize, long minSize,
                                  long maxSize) {
        return Math.max(minSize, Math.min(maxSize, blockSize));
    }

    private FileSplit makeSplit(Path file, long start, long length,
                                String[] hosts) {
        return new FileSplit(file, start, length, hosts);
    }

    private int getBlockIndex(BlockLocation[] blkLocations,
                              long offset) {
        for (int i = 0 ; i < blkLocations.length; i++) {
            // is the offset inside this block?
            if ((blkLocations[i].getOffset() <= offset) &&
                    (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length -1];
        long fileLength = last.getOffset() + last.getLength() -1;
        throw new IllegalArgumentException("Offset " + offset +
                " is outside of file (0.." +
                fileLength + ")");
    }


    /**
     * get total number of rows. for count(*)
     *
     * @throws IOException
     * @throws IndexBuilderException
     */
    public long getRowCount() throws IOException, IndexBuilderException {
        long rowCount = 0;
        /*AbsoluteTableIdentifier absoluteTableIdentifier = this.carbonTable.getAbsoluteTableIdentifier();

        // no of core to load the blocks in driver
        //addSegmentsIfEmpty(job, absoluteTableIdentifier);
        int numberOfCores = CarbonCommonConstants.NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT_DEFAULT_VALUE;
        try {
            numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
                    .getProperty(CarbonCommonConstants.NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT));
        } catch (NumberFormatException e) {
            numberOfCores = CarbonCommonConstants.NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT_DEFAULT_VALUE;
        }
        // creating a thread pool
        ExecutorService threadPool = Executors.newFixedThreadPool(numberOfCores);
        List<Future<Map<String, AbstractIndex>>> loadedBlocks =
                new ArrayList<Future<Map<String, AbstractIndex>>>();
        //for each segment fetch blocks matching filter in Driver BTree
        for (String segmentNo : this.segmentList) {
            // submitting the task
            loadedBlocks
                    .add(threadPool.submit(new BlocksLoaderThread(*//*job,*//* absoluteTableIdentifier, segmentNo)));
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw new IndexBuilderException(e);
        }
        try {
            // adding all the rows of the blocks to get the total row
            // count
            for (Future<Map<String, AbstractIndex>> block : loadedBlocks) {
                for (AbstractIndex abstractIndex : block.get().values()) {
                    rowCount += abstractIndex.getTotalNumberOfRows();
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IndexBuilderException(e);
        }*/
        return rowCount;
    }
}
