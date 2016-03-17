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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d5z66zk+gF8JDcCY2Eq95yhP7BMntGwVmedaCwGxR+26NV1z8OMxFmAR4Z5yi48U19rL
dudRRuR88YwxNM7+hU9B3Pj0AHehwGsD8ZzKYo77OO7+Yzv4sUVycfHSZdi+PA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.io.IOException;
import java.util.Map;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.impl.RestructureHolder;
import com.huawei.unibi.molap.engine.executer.pagination.GlobalPaginatedAggregator;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

/**
 * This class will do global merging of all intermediate files in case of pagination.
 * And process the rows like sort,measure filter,and topn etc.
 * @author R00900208
 * 
 */
public class FileBasedGlobalPaginatedAggregatorImpl implements GlobalPaginatedAggregator
{

    @Override
    public Map<ByteArrayWrapper, MeasureAggregator[]> getPage(int fromRow, int toRow) throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryResult getResult() throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void writeToDisk(Map<ByteArrayWrapper, MeasureAggregator[]> data, RestructureHolder restructureHolder)
            throws Exception
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void writeToDisk(Result data, RestructureHolder restructureHolder) throws Exception
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void mergeDataFile(boolean cacheRequired)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int getRowCount()
    {
        // TODO Auto-generated method stub
        return 0;
    }
//    private Map<ByteArrayWrapper, MeasureAggregator[]> gData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>();
//    
//    private Result result;
//
//    private Object writeLock = new Object();
//    
//    private ExecutorService executorService;
//    
//    private ExecutorService executorServiceMerged;
//
//    private String outLocation;
//    
//    private String queryId;
//    
//    private SliceExecutionInfo info;
//    
//    private Map<String, String> currentMergedFiles = new HashMap<String, String>();
//    
//    private LRUCacheKey holder;
//    
//    private boolean paginationRequired;
//    
//    private int rowCount;
//    
//    private QueryRowCounter rowCounter;
//    
//    private PaginationModel model;
//    
//    private Comparator comparator;
//    
//    private String interFileLocation;
//    
//    private static final int rowLimitForFile = Integer.parseInt(MolapProperties.getInstance().getProperty(
//            MolapCommonConstants.PAGINATED_INTERNAL_FILE_ROW_LIMIT,
//            MolapCommonConstants.PAGINATED_INTERNAL_FILE_ROW_LIMIT_DEFAULT));
//    
//    /**
//     * LOGGER
//     */
//    private static final LogService LOGGER = LogServiceFactory.getLogService(FileBasedGlobalPaginatedAggregatorImpl.class.getName());
//    
//    private static long internalMergeLimit = Long.parseLong(MolapProperties.getInstance().getProperty(
//            MolapCommonConstants.PAGINATED_INTERNAL_MERGE_SIZE_LIMIT,
//            MolapCommonConstants.PAGINATED_INTERNAL_MERGE_SIZE_LIMIT_DEFAULT))
//            * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
//    
//    public FileBasedGlobalPaginatedAggregatorImpl(SliceExecutionInfo info,LRUCacheKey holder, QueryRowCounter rowCounter)
//    {
//        executorService = Executors.newFixedThreadPool(2);
//        executorServiceMerged = Executors.newFixedThreadPool(1);
//        this.holder = holder;
//        this.outLocation = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION,
//                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
//                + File.separator + MolapCommonConstants.PAGINATED_CACHE_FOLDER;
//        this.queryId=info.getQueryId();
//        this.info = info;
//        paginationRequired = info.isPaginationRequired();
//        this.rowCounter = rowCounter;
//        this.model=getPaginationModel();
//        this.comparator = getMergerChainComparator(model.getMaskedByteRangeForSorting(), model.getDimensionSortOrder(), model.getDimensionMasks());
//        this.interFileLocation = this.outLocation + File.separator + queryId;
//        if(!info.isDetailQuery())
//        {
//            result= new MapBasedResult();
//        }
//        else
//        {
//            result= new ListBasedResult();
//        }
//    }
//
//    /*
//     * (non-Javadoc)
//     * 
//     * @see
//     * com.huawei.unibi.molap.engine.executer.pagination.GlobalPaginatedAggregator
//     * #getPage(int)
//     */
//    @Override
//    public Map<ByteArrayWrapper, MeasureAggregator[]> getPage(int fromRow, int toRow) throws IOException
//    {
//        if(!paginationRequired)
//        {
//            return gData;
//        }
//        DataFileReader fileReader = new DataFileReader(model.getKeySize(), outLocation, queryId,
//                model.getKeyGenerator(), info.getQueryMsrs(), info.getCalculatedMeasures(), info.getMeasureIndexToRead(),info.getSlice());
//        return fileReader.readData(fromRow, toRow);
//    }
//
//    /**
//     * Write the intermediate aggregate rows to disk.
//     */
//    public void writeToDisk(Map<ByteArrayWrapper, MeasureAggregator[]> data,RestructureHolder restructureHolder) throws Exception
//    {
//        synchronized(this.writeLock)
//        {
//            mergeData(data, restructureHolder);
//            rowCounter.setRowCount(1);
//            if(paginationRequired && (this.gData.size() > rowLimitForFile))
//            {
//                mergeInterFileIfRequired(new File(this.outLocation + File.separator + queryId));
//                if(info.isNormalizedCase())
//                {
//                    gData = NormalizeUtil.convert(gData, info.getNormalizedStartingIndexArray(), info.getActualKeyGenerator(),
//                            info.getKeyGenNormalized(), info.getMaskedBytePositions(), info.getSlices(),
//                            info.getQueryDimensions(), info.getReplacedDims(), info.getListOfMapsOfDimValues(),
//                            info.getByteCountNormalized(), info.getMaskByteRangesNormalized(), info.getMaxKeyNormalized());
//                }
//                this.executorService.submit(new DataFileWriter(this.gData,this.model,MolapCommonConstants.MAP,comparator,interFileLocation,info.isNormalizedCase()));
//                this.gData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>();
//            }
//        }
//    }
//    
//    private void mergeInterFileIfRequired(File path)
//    {
//        File[] files = path.listFiles(new FileFilter()
//        {
//            @Override
//            public boolean accept(File pathname)
//            {
//                String name = pathname.getName();
//                if(name.endsWith(MolapCommonConstants.QUERY_OUT_FILE_EXT))
//                {
//                    if(currentMergedFiles.get(name) == null)
//                    {
//                        return true;
//                    }
//                }
//                return false;
//            }
//        });
//        
//        if(files != null)
//        {
//            long length = 0;
//            for(int i = 0;i < files.length;i++)
//            {
//                length += files[i].length();
//            }
//            
//            if(length > internalMergeLimit)
//            {
//                try
//                {
//                    for(int i = 0;i < files.length;i++)
//                    {
//                        currentMergedFiles.put(files[i].getName(), files[i].getName());
//                    }
//                    InterDataFileMerger fileMerger = new InterDataFileMerger(info.getMaskedKeyByteSize(), outLocation,
//                            queryId, 8192, AggUtil.getAggregators(info.getQueryMsrs(), info.getCalculatedMeasures(),
//                                    false, info.getKeyGenerator(), info.getSlice().getCubeUniqueName()), files, holder,
//                            getMergerChainComparatorForMerger(model.getMaskedByteRangeForSorting(),
//                                    model.getDimensionSortOrder(), model.getDimensionMasks()),info.getQueryMsrs(),info.getCalculatedMeasures(),info.getKeyGenerator(),info.getSlice());
//                    this.executorServiceMerged.submit(fileMerger);
//                }
//                catch(Exception e)
//                {
//                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
//                }
//            }
//            
//        }
//    }
//    
//    /**
//     * @param model
//     * @return
//     */
//    private ComparatorChain getMergerChainComparator(int[][] maskedByteRangeForSorting,byte[] dimensionSortOrder,byte[][] dimensionMasks)
//    {
//        List<Comparator<ByteArrayWrapper>> compratorList = new ArrayList<Comparator<ByteArrayWrapper>>();
//        MaksedByteComparatorBAW keyComparator= null;
//        int length = maskedByteRangeForSorting.length;
//        for(int i = 0;i < length;i++)
//        {
//            keyComparator= new MaksedByteComparatorBAW(maskedByteRangeForSorting[i], dimensionSortOrder[i],dimensionMasks[i]);
//            compratorList.add(keyComparator);
//        }
//        return new ComparatorChain(compratorList);
//    }
//    
//    /**
//     * @param model
//     * @return
//     */
//    private ComparatorChain getMergerChainComparatorForKeyValue(int[][] maskedByteRangeForSorting,byte[] dimensionSortOrder,byte[][] dimensionMasks)
//    {
//        List<Comparator<KeyValueHolder>> compratorList = new ArrayList<Comparator<KeyValueHolder>>();
//        MaksedByteComparatorKeyValue keyComparator= null;
//        for(int i = 0;i < maskedByteRangeForSorting.length;i++)
//        {
//            keyComparator= new MaksedByteComparatorKeyValue(maskedByteRangeForSorting[i], dimensionSortOrder[i],dimensionMasks[i]);
//            compratorList.add(keyComparator);
//        }
//        return new ComparatorChain(compratorList);
//    }
//    
//    /**
//     * @param model
//     * @return
//     */
//    private ComparatorChain getMergerChainComparatorForMerger(int[][] maskedByteRangeForSorting,byte[] dimensionSortOrder,byte[][] dimensionMasks)
//    {
//        List<Comparator<DataFileChunkHolder>> compratorList = new ArrayList<Comparator<DataFileChunkHolder>>();
//        MaksedByteComparatorForDFCH keyComparator= null;
//        for(int i = 0;i < maskedByteRangeForSorting.length;i++)
//        {
//            keyComparator= new MaksedByteComparatorForDFCH(maskedByteRangeForSorting[i], dimensionSortOrder[i],dimensionMasks[i]);
//            compratorList.add(keyComparator);
//        }
//        return new ComparatorChain(compratorList);
//    }
//    
//    private void mergeData(Map<ByteArrayWrapper, MeasureAggregator[]> dataPart,RestructureHolder holder) throws Exception
//    {
//            Map<ByteArrayWrapper, MeasureAggregator[]> value = dataPart;
//            KeyGenerator keyGen = holder.metaData.getKeyGenerator();
//            if(holder.updateRequired)
//            {
//                for(ByteArrayWrapper wrapper : value.keySet())
//                {
//                    long[] data = keyGen.getKeyArray(wrapper.getMaskedKey(),holder.maskedByteRanges);
//                    long[] updData = new long[info.getActualKeyGenerator().getDimCount()];
//                    Arrays.fill(updData, 1);
//                    System.arraycopy(data, 0, updData, 0, data.length);
////                    wrapper.setActualData(keyGenerator.generateKey(updData));
//                    wrapper.setMaskedKey(getMaskedKey(info.getActualKeyGenerator().generateKey(updData), info.getActualMaxKeyBasedOnDimensions(), info.getActalMaskedByteRanges(),
//                            info.getActualMaskedKeyByteSize()));
//                }
//            }
// 
//            mergeByteArrayMapResult(dataPart,gData);
//    }
//    
//  
//
//    /**
//     * Merges the data present in the source map to target map, ensures if the
//     * Aggregator[] is merged if the key (ByteArrayWrapper) is same
//     * 
//     * @param source
//     * @param target
//     * 
//     */
//    private void mergeByteArrayMapResult(Map<ByteArrayWrapper, MeasureAggregator[]> source,
//            Map<ByteArrayWrapper, MeasureAggregator[]> target)
//    {
//
//        Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator = source.entrySet().iterator();
//        ByteArrayWrapper key = null;
//        MeasureAggregator[] value = null;
//        while(iterator.hasNext())
//        {
//            Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
//            key = entry.getKey();
//            value = entry.getValue();
//            MeasureAggregator[] agg = target.get(key);
//            if(agg != null)
//            {
//                // Merge the row
//                for(int j = 0;j < agg.length;j++)
//                {
//                    agg[j].merge(value[j]);
//                }
//            }
//            else
//            {
//                target.put(key, value);
//            }
//        }
//    }
//    
//    private byte[] getMaskedKey(byte[] data,byte[] maxKey, int[] maskByteRanges, int byteCount)
//    {
//        // check masked key is null or not
//        byte[] maskedKey = new byte[byteCount];
//        int counter = 0;
//        int byteRange = 0;
//        for(int i = 0;i < byteCount;i++)
//        {
//            byteRange = maskByteRanges[i];
//            if(byteRange != -1)
//            {
//                maskedKey[counter++] = (byte)(data[byteRange] & maxKey[byteRange]);
//            }
//        }
//        return maskedKey;
//    }
//    
//    
//    /**
//     * Final merge of all intermediate files and do processing like topN , measure filter, sorting and measure soring.
//     */
//    public void mergeDataFile(boolean interrupted)
//    {
//        if(!paginationRequired)
//        {
////            if(!info.isNormalizedCase())
////            {
//                this.gData = processDataForNonPaginationWithSortAndCache(this.gData,interrupted);
////            }
//            return;
//        }
//        if(paginationRequired && interrupted)
//        {
//            return;
//        }
////        synchronized(mergeLock)
////        {
//            try
//            {
//                if(this.gData.size() > 0)
//                {
//                    if(info.isNormalizedCase())
//                    {
//                        gData = NormalizeUtil.convert(gData, info.getNormalizedStartingIndexArray(), info.getActualKeyGenerator(),
//                                info.getKeyGenNormalized(), info.getMaskedBytePositions(), info.getSlices(),
//                                info.getQueryDimensions(), info.getReplacedDims(), info.getListOfMapsOfDimValues(),
//                                info.getByteCountNormalized(), info.getMaskByteRangesNormalized(), info.getMaxKeyNormalized());
//                    }
//                    this.executorService.submit(new DataFileWriter(this.gData,this.model,MolapCommonConstants.MAP,comparator,interFileLocation,info.isNormalizedCase()));
//                    this.gData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(20);
//                }
//  
//                executorService.shutdown();
//                executorServiceMerged.shutdown();
//                executorService.awaitTermination(2, TimeUnit.HOURS);
//                executorServiceMerged.awaitTermination(2, TimeUnit.HOURS);
//                PaginationModel model = getPaginationModel();
//                DataFileMerger dataFileMerger = new DataFileMerger(model, getMergerChainComparatorForMerger(
//                        model.getMaskedByteRangeForSorting(), model.getDimensionSortOrder(), model.getDimensionMasks()),createDataProcessor(model),interFileLocation);
//                dataFileMerger.call();
//                rowCount = model.getRowCount();
//            }
//            catch(Exception e)
//            {
//                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
//            }
//
////        }
//    }
//    
//    
//    private void cacheData(MolapSegmentHeader molapSegmentHeader,KeyValueHolder[] data,PaginationModel model)
//    {
//        if(molapSegmentHeader != null && data.length > 0 && model.getQueryMsrs().length > 0 && info.getRowLimitForDrillDown() <= 0)
//        {
//            //TODO : Smart cache flush would be implemented later.
////            TreeMap<ByteArrayWrapper, MeasureAggregator[]> treeMap = new TreeMap<ByteArrayWrapper, MeasureAggregator[]>(data);
////            byte[] startKeyForMap = ((ByteArrayWrapper)treeMap.firstKey()).getMaskedKey();
////            byte[] lastKeyForMap = ((ByteArrayWrapper)treeMap.lastKey()).getMaskedKey();
////            molapSegmentHeader.setStartKey(model.getKeyGenerator().getKeyArray(startKeyForMap,model.getMaskedByteRange()));
////            molapSegmentHeader.setEndKey(model.getKeyGenerator().getKeyArray(lastKeyForMap,model.getMaskedByteRange()));
////            byte[] updatedSortOrder = MolapCacheManager.getInstance().getUpdatedSortOrder(model.getQueryDims(), info.getDimensionSortOrder(), molapSegmentHeader.getDims());
////            MolapCacheManager.getInstance().storeData(molapSegmentHeader,Arrays.asList(model.getQueryMsrs()),
////                    createMap(data), model.getMaskedByteRange(), model.getMaxKey(),info.getCalculatedMeasures(),info.getActualMaskedKeyByteSize(),updatedSortOrder,info.getActualKeyGenerator());
//        }
//    }
//    
//    private Map<ByteArrayWrapper, MeasureAggregator[]>  createMap(KeyValueHolder[] data)
//    {
//        Map<ByteArrayWrapper, MeasureAggregator[]> map = new LinkedHashMap<ByteArrayWrapper, MeasureAggregator[]>(data.length+1,1.0f);
//        for(KeyValueHolder holder : data)
//        {
//            map.put(holder.key, holder.value);
//        }
//        return map;
//    }
//
////    /**
////     * 
////     */
////    public Map<ByteArrayWrapper, MeasureAggregator[]> processDataForNonPagination(Map<ByteArrayWrapper, MeasureAggregator[]> gData)
////    {
////        try
////        {
////            DataFileWriter fileWriter = new DataFileWriter(gData,this.model,MolapCommonConstants.MAP,comparator,interFileLocation);
////            long st = System.currentTimeMillis();
////            gData = fileWriter.getSortedDataMapSortBasedOnSortIndex();
////            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Time taken to sort with size : "+gData.size() +" is : "+(System.currentTimeMillis()-st));
////            MolapQueryDummyDataWriterProcessor writerProcessor = new MolapQueryDummyDataWriterProcessor();
////            DataProcessor processor = createDataProcessorForNonPagination(model,writerProcessor);
////            for(Entry<ByteArrayWrapper, MeasureAggregator[]> entry : gData.entrySet())
////            {
////                processor.processRow(entry.getKey().getMaskedKey(), entry.getValue());
////            }
////            processor.finish();
////            gData = writerProcessor.getData();
////        }
////        catch(Exception e)
////        {
////            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
////        }
////        return gData;
////  
////    }
//    
//    /**
//     * 
//     */
//    public Map<ByteArrayWrapper, MeasureAggregator[]> processDataForNonPaginationWithSortAndCache(Map<ByteArrayWrapper, MeasureAggregator[]> gData,boolean interrupted)
//    {
//        KeyValueHolder[] data = null;
//        try
//        {
//            long st = System.currentTimeMillis();
//            if(info.isNormalizedCase())
//            {
//                if(interrupted && info.getRowLimitForDrillDown() <= 0)
//                {
////                    throw MondrianResource.instance().MemberFetchLimitExceeded.ex(rowCounter.getRowLimit());
//                }
//                gData = NormalizeUtil.convert(gData, info.getNormalizedStartingIndexArray(), info.getActualKeyGenerator(),
//                        info.getKeyGenNormalized(), info.getMaskedBytePositions(), info.getSlices(),
//                        info.getQueryDimensions(), info.getReplacedDims(), info.getListOfMapsOfDimValues(),
//                        info.getByteCountNormalized(), info.getMaskByteRangesNormalized(), info.getMaxKeyNormalized());
//            }
//            if(interrupted)
//            {
//                return gData;
//            }
//            DataFileWriter fileWriter = new DataFileWriter(gData,this.model,MolapCommonConstants.MAP,getMergerChainComparatorForKeyValue(model.getMaskedByteRangeForSorting(), model.getDimensionSortOrder(), model.getDimensionMasks()),interFileLocation,info.isNormalizedCase());
//            data = fileWriter.getSortedDataMapSortBasedOnSortIndexCustom(!info.isNormalizedCase());
//
//            if(!info.isNormalizedCase())
//            {
//                cacheData(info.getSegmentHeader(),data, this.model);
//            }
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Time taken to sort with size : "+data.length +" is : "+(System.currentTimeMillis()-st));
//            MolapQueryDummyDataWriterProcessor writerProcessor = new MolapQueryDummyDataWriterProcessor();
//            DataProcessor processor = createDataProcessorForNonPagination(model,writerProcessor);
//            for(KeyValueHolder holder : data)
//            {
//                processor.processRow(holder.key.getMaskedKey(), holder.value);
//            }
//            processor.finish();
//            gData = writerProcessor.getData();
//        }
////        catch (ResourceLimitExceededException e) 
////        {
////            throw e;
////        }
//        catch(Exception e)
//        {
//            if(data != null)
//            {
//                gData = createMap(data);
//            }
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
//        }
//        return gData;
//    }
//    
//    /**
//     * 
//     */
//    private QueryResult processDataForNonPaginationWithSortAndCache()
//    {
//        KeyValueHolder[] data = null;
//        QueryResult queryResult = new QueryResult();
//        try
//        {
//            long st = System.currentTimeMillis();
//            DataFileWriter fileWriter = new DataFileWriter(result,this.model,MolapCommonConstants.MAP,getMergerChainComparatorForKeyValue(model.getMaskedByteRangeForSorting(), model.getDimensionSortOrder(), model.getDimensionMasks()),interFileLocation,info.isNormalizedCase());
//            data = fileWriter.getSortedDataMapSortBasedOnSortIndexCustom(!info.isNormalizedCase());
//
//            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Time taken to sort with size : "+data.length +" is : "+(System.currentTimeMillis()-st));
//            MolapQueryDummyProcessor writerProcessor = new MolapQueryDummyProcessor();
//            DataProcessor processor = createDataProcessorForNonPagination(model,writerProcessor);
//            for(KeyValueHolder holder : data)
//            {
//                processor.processRow(holder.key.getMaskedKey(), holder.value);
//            }
//            processor.finish();
//            queryResult = writerProcessor.getData();
//        }
//        catch(Exception e)
//        {
//            if(data != null)
//            {
//                queryResult.prepareResult(data);
//            }
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
//        }
//        return queryResult;
//    }
//    
//    
//    
//    /**
//     * Process the data for non pagination. It would do measure filter,sort and topN and finally store to map.
//     */
//    public Map<ByteArrayWrapper, MeasureAggregator[]> processDataForNonPaginationWithSort(Map<ByteArrayWrapper, MeasureAggregator[]> gData,boolean sortRequired)
//    {
//        try
//        {
//            MolapQueryDummyDataWriterProcessor writerProcessor = new MolapQueryDummyDataWriterProcessor();
//            DataProcessor processor = createDataProcessorForNonPagination(model,writerProcessor);
//            if(sortRequired)
//            {
//                DataFileWriter fileWriter = new DataFileWriter(gData, this.model, MolapCommonConstants.MAP,
//                        getMergerChainComparatorForKeyValue(model.getMaskedByteRangeForSorting(),
//                                model.getDimensionSortOrder(), model.getDimensionMasks()), interFileLocation,
//                        info.isNormalizedCase());
//                long st = System.currentTimeMillis();
//                KeyValueHolder[] data = fileWriter.getSortedDataMapSortBasedOnSortIndexCustom(false);
//                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Time taken to sort with size : "+data.length +" is : "+(System.currentTimeMillis()-st));
//                for(KeyValueHolder holder : data)
//                {
//                    processor.processRow(holder.key.getMaskedKey(), holder.value);
//                }
//            }
//            else
//            {
//                for(Entry<ByteArrayWrapper, MeasureAggregator[]> entry : gData.entrySet())
//                {
//                    processor.processRow(entry.getKey().getMaskedKey(), entry.getValue());
//                }
//            }
//            processor.finish();
//            gData = writerProcessor.getData();
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
//        }
//        return gData;
//  
//    }
//    
//    /**
//     * These processors would be executed from down to up. That means the first processor created here will be 
//     * executed at last.
//     * @param model
//     * @return
//     */
//    private DataProcessor createDataProcessor(PaginationModel model)
//    {
//        DataProcessor processor = null;
//        try
//        {
//            processor = new MolapQueryDataWriter();
//            if(model.getMsrSortModel() != null)
//            {
//                processor = new MeasureSortProcessor(processor);
//            }
//            
//            boolean topNPresent = false;
//            if(model.getTopNType() != null)
//            {
//                topNPresent = true;
//            }
//
//            boolean msrFilterPresent = isMeasureFilterPresent(model.getMsrConstraints(),model.getQueryMsrs());
//            boolean msrFilterPresentAfterTopN = isMeasureFilterPresent(model.getMsrConstraintsAfterTopN(),model.getQueryMsrs());
//            
//            if(msrFilterPresentAfterTopN)
//            {
//                processor = new MeasureFilterProcessor(processor,true);
//            }
//            
//            if(model.getFilterModelAfterTopN() != null && model.getConstraintsAfterTopN().size() > 0)
//            {
//                processor = new DimensionFilterProcessor(processor);
//            }
//            
//            if(topNPresent)
//            {
//                // processor = new TopNProcessorMerger(processor,comparator);
//                processor = new TopNProcessorBytes(processor);
//                processor = new GroupByProcessor(processor);
//            }
//            
//            if(msrFilterPresent)
//            {
//                processor = new MeasureFilterProcessor(processor,false);
//                processor = new GroupByProcessor(processor,info.getMsrFilterProcessorModel());
//            }
//
//            processor.initModel(model);
//        }
//        catch (Exception e) 
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
//        }
//        return processor;
//    }
//
//    /**
//     * @param model
//     * @param msrFilterPresent
//     * @return
//     */
//    private boolean isMeasureFilterPresent(GroupMeasureFilterModel[] msrConstraints,Measure[] queryMsrs)
//    {
//        if(msrConstraints != null)
//        {
//            MeasureFilter[] filters = MeasureFilterFactory.getFilterMeasures(msrConstraints,Arrays.asList(queryMsrs));
//            if(MeasureFilterUtil.isMsrFilterEnabled(filters))
//            {
//                return true;
//            }
//        }
//        return false;
//    }
//    
//    /**
//     * These processors would be executed from down to up. That means the first processor created here will be 
//     * executed at last.
//     * @param model
//     * @return
//     */
//    private DataProcessor createDataProcessorForNonPagination(PaginationModel model,MolapQueryDummyProcessor writerProcessor)
//    {
//        DataProcessor processor = writerProcessor;
//        try
//        {
//            if(model.getMsrSortModel() != null)
//            {
//                processor = new MeasureSortProcessor(processor);
//            }
//
//            boolean topNPresent = false;
//            if(model.getTopNType() != null)
//            {
//                topNPresent = true;
//            }
//
//            boolean msrFilterPresent = isMeasureFilterPresent(model.getMsrConstraints(), model.getQueryMsrs());
//            boolean msrFilterPresentAfterTopN = isMeasureFilterPresent(model.getMsrConstraintsAfterTopN(),
//                    model.getQueryMsrs());
//
//            if(msrFilterPresentAfterTopN)
//            {
//                processor = new MeasureFilterProcessor(processor, true);
//            }
//            if(topNPresent && model.getFilterModelAfterTopN() != null && model.getConstraintsAfterTopN().size() > 0)
//            {
//                processor = new DimensionFilterProcessor(processor);
//            }
//            if(topNPresent)
//            {
//                // processor = new TopNProcessorMerger(processor,comparator);
//                processor = new TopNProcessorBytes(processor);
//                processor = new GroupByProcessor(processor);
//            }
//
//            if(msrFilterPresent)
//            {
//                processor = new MeasureFilterProcessor(processor, false);
//                processor = new GroupByProcessor(processor, info.getMsrFilterProcessorModel());
//            }
//
//            processor.initModel(model);
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//        }
//        return processor;
//    }
//    
//    /**
//     * These processors would be executed from down to up. That means the first processor created here will be 
//     * executed at last.
//     * @param model
//     * @return
//     */
//    private DataProcessor createDataProcessorForNonPagination(PaginationModel model,MolapQueryDummyDataWriterProcessor writerProcessor)
//    {
//        DataProcessor processor = writerProcessor;
//        try
//        {
//            if(model.getMsrSortModel() != null)
//            {
//                processor = new MeasureSortProcessor(processor);
//            }
//
//            boolean topNPresent = false;
//            if(model.getTopNType() != null)
//            {
//                topNPresent = true;
//            }
//
//            boolean msrFilterPresent = isMeasureFilterPresent(model.getMsrConstraints(), model.getQueryMsrs());
//            boolean msrFilterPresentAfterTopN = isMeasureFilterPresent(model.getMsrConstraintsAfterTopN(),
//                    model.getQueryMsrs());
//
//            if(msrFilterPresentAfterTopN)
//            {
//                processor = new MeasureFilterProcessor(processor, true);
//            }
//            if(topNPresent && model.getFilterModelAfterTopN() != null && model.getConstraintsAfterTopN().size() > 0)
//            {
//                processor = new DimensionFilterProcessor(processor);
//            }
//            if(topNPresent)
//            {
//                // processor = new TopNProcessorMerger(processor,comparator);
//                processor = new TopNProcessorBytes(processor);
//                processor = new GroupByProcessor(processor);
//            }
//
//            if(msrFilterPresent)
//            {
//                processor = new MeasureFilterProcessor(processor, false);
//                processor = new GroupByProcessor(processor, info.getMsrFilterProcessorModel());
//            }
//
//            processor.initModel(model);
//        }
//        catch(Exception e)
//        {
//            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
//        }
//        return processor;
//    }
//
//    /**
//     * @return
//     */
//    private PaginationModel getPaginationModel()
//    {
//        PaginationModel model = new PaginationModel();
//        boolean normalizedCase = info.isNormalizedCase();
//        model.setKeySize(normalizedCase?info.getByteCountNormalized():info.getActualMaskedKeyByteSize());
//        model.setOutLocation(outLocation);
//        model.setQueryId(queryId);
//        model.setFileBufferSize(8192);
//        model.setMeasureAggregators(AggUtil.getAggregators(info.getQueryMsrs(),info.getCalculatedMeasures(), false, info.getKeyGenerator(), info.getSlice().getCubeUniqueName()));
//        model.setBlockSize(8192);
//        model.setDimensionSortOrder(info.getDimensionSortOrder());
//        model.setDimensionMasks(info.getDimensionMaskKeys());
//        model.setMaskedByteRangeForSorting(info.getMaskedByteRangeForSorting());
//        model.setMaxKey(normalizedCase?info.getMaxKeyNormalized():info.getActualMaxKeyBasedOnDimensions());
//        model.setMaskedByteRange(normalizedCase?info.getMaskedBytesNormalized():info.getMaskedBytePositions());
//        model.setHolder(holder);
//        model.setHolderSize(rowLimitForFile);
//        model.setQueryDims(normalizedCase?info.getReplacedDims():info.getQueryDimensions());
//        model.setSlices(info.getSlices());
//        model.setKeyGenerator(normalizedCase?info.getKeyGenNormalized():info.getActualKeyGenerator());
//        model.setActualMaskByteRanges(normalizedCase?info.getMaskByteRangesNormalized():info.getActalMaskedByteRanges());
//        model.setQueryMsrs(info.getQueryMsrs());
//        model.setCalculatedMeasures(info.getCalculatedMeasures());
//        model.setPaginationEnabled(paginationRequired);
//        model.setMeasureIndexToRead(info.getMeasureIndexToRead());
//        model.setFilterModelAfterTopN(info.getFilterModelAfterTopN());
//        model.setConstraintsAfterTopN(info.getConstraintsAfterTopN());
//        model.setLimit(info.getLimit());
//        if(info.getMsrFilterProcessorModel() != null)
//        {
//            model.setMaskedBytesForMeasureFilter(info.getMsrFilterProcessorModel().getMaskedBytes());
//            model.setMsrFilterMaskedBytesPos(info.getMsrFilterProcessorModel().getMaskedBytesPos());
//        }
//        TopNModel topNModel = info.getTopNModel();
//        if(topNModel != null )
//        {
//            model.setGroupMaskedBytes(topNModel.getTopNGroupMaskedBytes());
//            model.setMaskedBytes(topNModel.getTopNMaskedBytes());
//            model.setTopMeasureIndex(topNModel.getMsrIndex());
//            model.setTopNCount(topNModel.getCount());
//            model.setTopNType(topNModel.getTopNType());
//            model.setAvgMsrIndex(topNModel.getAvgMsrIndex());
//            model.setCountMsrIndex(topNModel.getCountMsrIndex());
//            model.setAggName(topNModel.getMeasure().getAggName());
//            model.setTopCountOnCalcMeasure(topNModel.getMsrIndex()>=info.getQueryMsrs().length ? true :false);
//            model.setTopNGroupMaskedBytesPos(topNModel.getTopNGroupMaskedBytesPos());
//            model.setTopNMaskedBytesPos(topNModel.getTopNMaskedBytesPos());
//            if(topNModel.getAxisType() != null && topNModel.getAxisType().equals(AxisType.COLUMN))
//            {
//                model.setTopNOnColumn(true);
//            }
//        }
//        if(null!=info.getMsrSortModel())
//        {
//            model.setMsrSortModel(info.getMsrSortModel());
//        }
//        if(null!=info.getMsrConstraints())
//        {
//            model.setMsrConstraints(info.getMsrConstraints() != null ?info.getMsrConstraints().toArray(new GroupMeasureFilterModel[info.getMsrConstraints().size()]):null);
//        }
//        if(null!=info.getMsrConstraintsAfterTopN())
//        {
//            model.setMsrConstraintsAfterTopN(info.getMsrConstraintsAfterTopN() != null ?info.getMsrConstraintsAfterTopN().toArray(new GroupMeasureFilterModel[info.getMsrConstraintsAfterTopN().size()]):null);
//        }
//        return model;
//    }
//    
//    /**
//     * Get the total row count
//     * @return
//     */
//    public int getRowCount()
//    {
//        return rowCount;
//    }
//
//    @Override
//    public QueryResult getResult() throws IOException
//    {
//        return processDataForNonPaginationWithSortAndCache();
//    }
//
//    @Override
//    public void writeToDisk(Result data, RestructureHolder restructureHolder) throws Exception
//    {
//        result.merge(data);
//    }

}
