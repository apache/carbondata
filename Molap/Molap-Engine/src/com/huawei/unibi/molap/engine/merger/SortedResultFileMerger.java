package com.huawei.unibi.molap.engine.merger;

import java.util.AbstractQueue;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.merger.exception.ResultMergerException;
import com.huawei.unibi.molap.engine.processor.DataProcessor;
import com.huawei.unibi.molap.engine.processor.exception.DataProcessorException;
import com.huawei.unibi.molap.engine.reader.ResultTempFileReader;
import com.huawei.unibi.molap.engine.reader.exception.ResultReaderException;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : SortedResultFileMerger.java
 * Description   : This class is responsible for handling the merge of the result data
 *         in a sorted order. This will use the heap to get the data sorted.
 * Class Version  : 1.0
 */
public class SortedResultFileMerger implements Callable<Void>
{
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(SortedResultFileMerger.class.getName());

    /**
     * dataProcessorInfo holds all the metadata related to query execution.
     */
    private DataProcessorInfo dataProcessorInfo;

    /**
     * recordHolderHeap
     */
    private AbstractQueue<ResultTempFileReader> recordHolderHeap;

    /**
     * fileCounter
     */
    private int fileCounter;

    /**
     * dataProcessor This is the data processor object which will process the data.
     */
    private DataProcessor dataProcessor;
    
    /**
     * files all the intermediate files.
     */
    private MolapFile[] files;

    public SortedResultFileMerger(final DataProcessor dataProcessor, final DataProcessorInfo dataProcessorInfo,final MolapFile[] files)
    {
        this.dataProcessorInfo = dataProcessorInfo;
        this.files=files;
        fileCounter=files.length;
        this.recordHolderHeap = new PriorityQueue<ResultTempFileReader>(this.fileCounter, dataProcessorInfo.getHeapComparator());
        this.dataProcessor=dataProcessor;
    }

    /**
     * 
     * @see java.util.concurrent.Callable#call()
     * 
     */
    @Override
    public Void call() throws Exception
    {
        try
        {
            dataProcessor.initialise(dataProcessorInfo);
			// For each intermediate result file.
            for(MolapFile file : this.files)
            {
                // reads the temp files and creates ResultTempFileReader object.
                ResultTempFileReader molapSortTempFileChunkHolder = new ResultTempFileReader(file.getAbsolutePath(),
                        dataProcessorInfo.getKeySize(), AggUtil.getAggregators(dataProcessorInfo.getAggType(), false,
                                dataProcessorInfo.getKeyGenerator(), dataProcessorInfo.getCubeUniqueName(),
                                dataProcessorInfo.getMsrMinValue(),null),
                                dataProcessorInfo.getFileBufferSize());
                // initialize
                molapSortTempFileChunkHolder.initialize();
                molapSortTempFileChunkHolder.readRow();
                // add ResultTempFileReader object to heap
                this.recordHolderHeap.add(molapSortTempFileChunkHolder);
            }
            while(hasNext())
            {
                writeSortedRecordToFile();
            }
        }
        catch(ResultReaderException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw e;
        }
        finally
        {
            dataProcessor.finish();
			 //delete the temp files.
            MolapUtil.deleteFoldersAndFiles(this.files);
        }
        return null;
    }

    /**
     * This method will be used to check whether any more files are present or
     * not.
     * 
     * @return more element is present
     * 
     */
    private boolean hasNext()
    {
        return this.fileCounter > 0;
    }

    /**
     * This method will be used to write the sorted record from file
     * 
     * @return sorted record sorted record
     * @throws MolapSortKeyAndGroupByException
     * 
     */
    private void writeSortedRecordToFile() throws ResultMergerException
    {
        // poll the top object from heap
        // heap maintains binary tree which is based on heap condition that will
        // be based on comparator we are passing the heap
        // when will call poll it will always delete root of the tree and then
        // it does trickle down operation. complexity is log(n)
        ResultTempFileReader dataFile = this.recordHolderHeap.poll();
        // check if there no entry present.
        if(!dataFile.hasNext())
        {
            // if chunk is empty then close the stream
            dataFile.closeStream();
            // change the file counter
            --this.fileCounter;
            return;
        }
        try
        {
            // process the row based on the dataprocessor type.
            dataProcessor.processRow(dataFile.getKey(), dataFile.getMeasures());
        }
        catch(DataProcessorException e)
        {
            throw new ResultMergerException(e);
        }

        dataFile.setMeasureAggs(AggUtil.getAggregators(dataProcessorInfo.getAggType(), false, dataProcessorInfo.getKeyGenerator(), dataProcessorInfo
                .getCubeUniqueName(),dataProcessorInfo.getMsrMinValue(),null));
        try
        {
            // read the next row to process and add to the heap.
            dataFile.readRow();
        }
        catch(ResultReaderException e)
        {
            throw new ResultMergerException(e);
        }
        // add to heap
        this.recordHolderHeap.add(dataFile);
    }
}
