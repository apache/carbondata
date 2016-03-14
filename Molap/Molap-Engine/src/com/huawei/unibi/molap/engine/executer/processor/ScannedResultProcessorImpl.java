package com.huawei.unibi.molap.engine.executer.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.columnar.aggregator.impl.MapBasedResultAggregatorImpl;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileWriter.KeyValueHolder;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.merger.MergerExecutor;
import com.huawei.unibi.molap.engine.processor.DataProcessor;
import com.huawei.unibi.molap.engine.processor.DataProcessorExt;
import com.huawei.unibi.molap.engine.processor.FileBasedLimitProcessor;
import com.huawei.unibi.molap.engine.processor.MemoryBasedLimitProcessor;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.engine.processor.row.AggreagtedRowProcessor;
import com.huawei.unibi.molap.engine.processor.row.RowProcessor;
import com.huawei.unibi.molap.engine.processor.writer.BlockWriterProcessor;
//import com.huawei.unibi.molap.engine.processor.writer.RowWriterProcessor;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.result.impl.ListBasedResult;
import com.huawei.unibi.molap.engine.result.impl.MapBasedResult;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.util.ScannedResultProcessorUtil;
import com.huawei.unibi.molap.engine.writer.WriterExecutor;
import com.huawei.unibi.molap.engine.writer.exception.ResultWriterException;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : ScannedResultProcessorImpl.java
 * Description   : 
 * Class Version  : 1.0
 */
public class ScannedResultProcessorImpl implements ScannedResultProcessor
{
    private Result mergedScannedResult;
    
    private SliceExecutionInfo info;
    
    private boolean isFileBased;
    
    private WriterExecutor writerExecutor;
    
    private MergerExecutor mergerExecutor;
    
    private String outLocation;
    
    private final Map<String,String> processedFileMap; 
    
    private String interMediateLocation;
    
    private DataProcessorInfo dataProcessorInfo;
    
    private ExecutorService execService;
    
    private List<Result> scannedResultList;
    
    private long recordCounter;
    
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(ScannedResultProcessorImpl.class.getName());
    
    private static long internalMergeLimit = Long.parseLong(MolapProperties.getInstance().getProperty(
            MolapCommonConstants.PAGINATED_INTERNAL_MERGE_SIZE_LIMIT,
            MolapCommonConstants.PAGINATED_INTERNAL_MERGE_SIZE_LIMIT_DEFAULT))
            * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    
    public ScannedResultProcessorImpl(SliceExecutionInfo info)
    {
        this.info=info;
        writerExecutor= new WriterExecutor();
        
        this.outLocation = info.getOutLocation() + '/' + MolapCommonConstants.SPILL_OVER_DISK_PATH + info.getSchemaName() + '/'
                + info.getCubeName()+'/'+System.nanoTime();
        this.interMediateLocation=outLocation+'/'+info.getQueryId();
       
        this.mergerExecutor = new MergerExecutor();
        this.processedFileMap = new HashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        dataProcessorInfo=ScannedResultProcessorUtil.getDataProcessorInfo(info,  ScannedResultProcessorUtil.getResultTempFileReaderComprator(info.getMaskedByteRangeForSorting(),
                        info.getDimensionSortOrder(), info.getDimensionMaskKeys()), ScannedResultProcessorUtil.getMergerChainComparator(info.getMaskedByteRangeForSorting(),
                        info.getDimensionSortOrder(), info.getDimensionMaskKeys()));
        
        initialiseResult();
        execService =Executors.newFixedThreadPool(1);
        scannedResultList = new ArrayList<Result>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    
    /**
     *  for initializing the map based or list based result.
     */
    private void initialiseResult()
    {
        if(!info.isDetailQuery())
        {
//            mergedScannedResult = new TrieBasedResult(info.getColumnarSplitter().getKeySizeByBlock(
//                    info.getQueryDimOrdinal()));
            mergedScannedResult = new MapBasedResult();
        }
        else
        {
            mergedScannedResult = new ListBasedResult();
        }
    }

    @Override
    public void addScannedResult(Result scannedResult)
            throws QueryExecutionException
    {
        synchronized(processedFileMap)
        {
            scannedResultList.add(scannedResult);
            recordCounter+=scannedResult.size();
            if((scannedResultList.size() > 3)
                    || (this.info.isDetailQuery() && recordCounter >= this.info.getNumberOfRecordsInMemory()))
            {
                List<Result> localResult = scannedResultList;
                scannedResultList = new ArrayList<Result>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
                execService.submit(new MergerThread(localResult));
            }
        }
    }
    
    private final class MergerThread implements Callable<Void>
    {
        private List<Result> scannedResult;
        
        private MergerThread(List<Result> scannedResult)
        {
            this.scannedResult=scannedResult;
        }
        @Override
        public Void call() throws Exception
        {
            mergeScannedResultsAndWriteToFile(scannedResult);
            return null;
        }
        
    }
    
    private void mergeScannedResultsAndWriteToFile(List<Result> scannedResult) throws QueryExecutionException
    {
        long start = System.currentTimeMillis();
        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Started a slice result merging");
        
        for(int i = 0;i < scannedResult.size();i++)
        {
            mergedScannedResult.merge(scannedResult.get(i));
        }
        if(info.isDetailQuery() && info.isFileBasedQuery()
                && (isFileBased || info.getNumberOfRecordsInMemory() < mergedScannedResult.size()))
        {
            if(!isFileBased)
            {
                createSpillOverDirectory();
            }
//            mergerIntermediateFiles(interMediateLocation,new String[]{MolapCommonConstants.QUERY_OUT_FILE_EXT});
            writerExecutor.writeResult(
                    mergedScannedResult,
                    ScannedResultProcessorUtil.getMergerChainComparator(info.getMaskedByteRangeForSorting(),
                            info.getDimensionSortOrder(), info.getDimensionMaskKeys()),
                            dataProcessorInfo, interMediateLocation);
            initialiseResult();
            isFileBased=true;
        }
        
        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Finished current slice result merging in time (MS) " + (System.currentTimeMillis()-start));
    }
    
    private void createSpillOverDirectory() throws QueryExecutionException
    {
        try
        {
            if(!FileFactory.isFileExist(interMediateLocation, FileFactory.getFileType(interMediateLocation)))
            {
                if(!FileFactory.mkdirs(interMediateLocation, FileFactory.getFileType(interMediateLocation)))
                {
                    throw new QueryExecutionException("Problem while creating Spill Over Directory");
                }
            }
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "********************************File based query execution");
        }
        catch(IOException e)
        {
            throw new QueryExecutionException(e);
        }
    }

    @Override
    public MolapIterator<QueryResult> getQueryResultIterator() throws QueryExecutionException
    {
        execService.shutdown();
        try
        {
            execService.awaitTermination(1, TimeUnit.DAYS);
        }
        catch(InterruptedException e1)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem in thread termination"+e1.getMessage());
        }
        
        if(scannedResultList.size()>0)
        {
            mergeScannedResultsAndWriteToFile(scannedResultList);
            scannedResultList=null;
        }
        
        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Finished result merging from all slices");
        
        DataProcessorExt processor = getProcessor();
        if(!isFileBased)
        {
            KeyValueHolder[] sortedResult = ScannedResultProcessorUtil.getSortedResult(
                    dataProcessorInfo,
                    mergedScannedResult,
                    ScannedResultProcessorUtil.getMergerChainComparator(info.getMaskedByteRangeForSorting(),
                            info.getDimensionSortOrder(), info.getDimensionMaskKeys()));
            try
            {
                processor.initialise(dataProcessorInfo);
                for(int i = 0;i < sortedResult.length;i++)
                {
                    if(sortedResult[i].key.getCompleteComplexTypeData() == null)
                    {
                        processor.processRow(sortedResult[i].key.getMaskedKey(), sortedResult[i].value);
                    }
                    else
                    {
                        processor.processRow(sortedResult[i].key, sortedResult[i].value);
                    }
                }
            }
            catch(DataProcessorException e)
            {
                throw new QueryExecutionException(e);
            }
        }
        else
        {
            if(mergedScannedResult.size() > 0)
            {
                writerExecutor.writeResult(
                        mergedScannedResult,
                        ScannedResultProcessorUtil.getMergerChainComparator(info.getMaskedByteRangeForSorting(),
                                info.getDimensionSortOrder(), info.getDimensionMaskKeys()), dataProcessorInfo,
                        interMediateLocation);
            }
            closeExecuters();
            
            try
            {
                mergerExecutor.mergeFinalResult(
                        processor,
                        dataProcessorInfo,
                        ScannedResultProcessorUtil.getFiles(interMediateLocation, new String[]{
                                MolapCommonConstants.QUERY_OUT_FILE_EXT, MolapCommonConstants.QUERY_MERGED_FILE_EXT}));
            }
            catch(Exception e)
            {
                throw new QueryExecutionException(e);
            }
        }
        return processor.getQueryResultIterator();
    }

    /**
     * check whether its file based or memory based processing 
     * and return appropriate DataProcessor
     * @return DataProcessor
     */
    private DataProcessorExt getProcessor()
    {
        if(!isFileBased)
        {
            return new MemoryBasedLimitProcessor();
        }
        else if(info.getLimit()!=-1 && info.getLimit()<info.getNumberOfRecordsInMemory())
        {
            if(info.isDetailQuery())
            {
                return new RowProcessor(new MemoryBasedLimitProcessor());
            }
            else
            {
                return new AggreagtedRowProcessor(new MemoryBasedLimitProcessor());
            }
        }
        else 
        {
            if(info.isDetailQuery())
            {
                return new RowProcessor(new FileBasedLimitProcessor(new BlockWriterProcessor(outLocation)));
            }
            else
            {
                return new AggreagtedRowProcessor(new FileBasedLimitProcessor(new BlockWriterProcessor(outLocation)));
            }
        }
        
    }
    
    /**
     *  For closing of the writerExecutor , mergerExecutor threads 
     */
    private void closeExecuters()
    {
        try
        {
            writerExecutor.closeWriter();
        }
        catch(ResultWriterException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while closing stream"+e.getMessage());
        }
        try
        {
            mergerExecutor.closeMerger();
        }
        catch(ResultWriterException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while closing stream"+e.getMessage());
        }
    }

}
