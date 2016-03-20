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

package com.huawei.unibi.molap.engine.writer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileWriter.KeyValueHolder;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.util.ScannedResultProcessorUtil;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * 
 * Description   : This class is responsible for writing query output to file.
 * Class Version  : 1.0
 */
public class ScannedResultDataFileWriterThread extends ResultWriter
{

    /**
     * ScannedResult
     */
    private Result scannedResult;

    /**
     * outLocation
     */
    private String outLocation;

    /***
     * comparator
     */
    private Comparator comparator;

    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(ScannedResultDataFileWriterThread.class
            .getName());

    /**
     * DataFileWriter Constructor
     * 
     * @param dataMap
     * @param model
     */
    public ScannedResultDataFileWriterThread(Result scannedResult, DataProcessorInfo dataProcessorInfo, Comparator comparator,
            String outLocation)
    {
        this.outLocation = outLocation;
        this.dataProcessorInfo = dataProcessorInfo;
        this.scannedResult = scannedResult;
        this.comparator = comparator;
//        updateDuplicateDimensions();
    }

    /**
     * 
     * @see java.util.concurrent.Callable#call()
     * 
     */
    @Override
    public Void call() throws Exception
    {
        DataOutputStream dataOutput = null;
        MolapFile molapFile = null;
        String destPath = null;
        try
        {
            String path = this.outLocation + '/' + System.nanoTime()
                    + ".tmp";
            dataOutput = FileFactory.getDataOutputStream(path,
                    FileFactory.getFileType(path),
                    (short)1);
            molapFile = FileFactory.getMolapFile(path, FileFactory.getFileType(path));
            dataOutput.writeInt(scannedResult.size());
            /*int writenDataSize = */writeScannedResult(dataOutput);
            destPath=this.outLocation + '/' + System.nanoTime()
                    + MolapCommonConstants.QUERY_OUT_FILE_EXT;
        }
        catch(IOException e)
        {
            throw new QueryExecutionException(e);
        }
        finally
        {
            MolapUtil.closeStreams(dataOutput);
            try
            {
                if(null!=molapFile && !molapFile.renameTo(destPath))
                {
                    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while renaming the file");
                }
            }
            catch(Exception e)
            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, "Problem while renaming the file");
            }
        }
        return null;
    }

    /**
     * This will write the scannedResult into the data stream.
     * 
     * @param dataOutput
     * @return
     * @throws KeyGenException
     * @throws IOException
     */
    private void writeScannedResult(DataOutputStream dataOutput) throws QueryExecutionException
    {
        KeyValueHolder[] holderArray;
        try
        {
            holderArray = ScannedResultProcessorUtil.getSortedResult(dataProcessorInfo, scannedResult, comparator);
        }
        catch(QueryExecutionException e)
        {
           throw e;
        }
        //int counter = 0;
        try
        {
        for(KeyValueHolder holder : holderArray)
        {
            dataOutput.write(holder.key.getMaskedKey());
            MeasureAggregator[] value = holder.value;
            for(int i = 0;i < value.length;i++)
            {
                value[i].writeData(dataOutput);
            }
           // counter++;
        }
        }
        catch(IOException e)
        {
            throw new QueryExecutionException(e);
        }
        //return counter;
    }
}
