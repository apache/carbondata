/**
 * 
 */
package com.huawei.unibi.molap.engine.merger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.engine.processor.DataProcessor;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.writer.exception.ResultWriterException;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : MergerExecutor.java
 * Description   : This is the executor class for merge. This is responsible for
 *         creating executor service for merge and running the callable object.
 * Class Version  : 1.0
 */
public class MergerExecutor
{
    /**
     *  execService Service for maintaining thread pool. 
     */
    private ExecutorService execService;

    public MergerExecutor()
    {
        execService = Executors.newFixedThreadPool(2);
    }

    public void mergeResult(DataProcessor processor,DataProcessorInfo info, MolapFile[] files)
    {
        if(!info.isSortedData())
        {
            execService.submit(new UnSortedResultMerger(processor,info,files));
        }
        else
        {
            execService.submit(new SortedResultFileMerger(processor,info,files));
        }
    }
    
    public void mergeFinalResult(DataProcessor processor,DataProcessorInfo info, MolapFile[] files) throws Exception
    {
        if(!info.isSortedData())
        {
            new UnSortedResultMerger(processor,info,files).call();
        }
        else
        {
            new SortedResultFileMerger(processor,info,files).call();
        }
    }

    public void closeMerger() throws ResultWriterException
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
