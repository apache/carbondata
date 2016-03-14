/**
 * 
 */
package com.huawei.unibi.molap.engine.writer;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.unibi.molap.engine.executer.Tuple;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.writer.exception.ResultWriterException;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : WriterExecutor.java
 * Description   : This is the executor class which is responsible for creating the
 * ExecutorService and submitting the Result Writer callable object.
 * Class Version  : 1.0
 */
public class WriterExecutor
{
    private ExecutorService execService;
    
    /**
     * Constructor.
     */
    public WriterExecutor()
    {
        execService = Executors.newFixedThreadPool(2);
    }

    /**
     * This method is used to invoke the writer based on the scanned result.
     * 
     * @param scannedResult
     * @param comparator
     * @param writerVo
     * @param outLocation
     */
    public void writeResult(Result scannedResult, Comparator comparator, DataProcessorInfo dataProcessorInfo, String outLocation)
    {
        execService.submit(new ScannedResultDataFileWriterThread(scannedResult, dataProcessorInfo, comparator, outLocation));
    }

    /**
     * This method is used to invoke the writer based on the data heap.
     * 
     * @param dataHeap
     * @param writerVo
     * @param outLocation
     */
    public void writeResult(AbstractQueue<Tuple> dataHeap, DataProcessorInfo writerVo, String outLocation)
    {
        execService.submit(new HeapBasedDataFileWriterThread(dataHeap, writerVo, outLocation));
    }

    /**
     * This method will wait till the writer threads are finished.
     * 
     * @throws ResultWriterException
     */
    public void closeWriter() throws ResultWriterException
    {
        execService.shutdown();
        try
        {
            execService.awaitTermination(2, TimeUnit.DAYS);
        }
        catch(InterruptedException e)
        {
           throw new ResultWriterException(e);
        }
    }
    
}
