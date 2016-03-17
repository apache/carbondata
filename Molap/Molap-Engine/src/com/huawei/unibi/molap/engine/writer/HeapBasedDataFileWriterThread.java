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
 TTt3d/+OaeO30CuXiPeMQ1b+bGpDqN2k91rrrg5Qo616Byv5rBJNqzufTz7T840oTWQTl92w
 xjhM1Q5+9Rooj6mjTLDiC3s4yPFyntfnBfxf9EnzO/onXMlrcTl4kfUVlrYzeg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.writer;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.AbstractQueue;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.Tuple;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : HeapBasedDataFileWriterThread.java
 * Description   : This class is responsible for writing query output to file
 * Class Version  : 1.0
 */
public class HeapBasedDataFileWriterThread extends ResultWriter
{

    /**
     * dataHeap
     */
    private AbstractQueue<Tuple> dataHeap;

    /**
     * outLocation
     */
    private String outLocation;

    /***
     * comparator
     */
   // private Comparator comparator;

    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(HeapBasedDataFileWriterThread.class.getName());

    /**
     * DataFileWriter {refer Constructor}
     * 
     * @param dataMap
     *            data
     * @param outLocation
     *            out location
     * @param queryId
     *            query id
     * 
     */
    public HeapBasedDataFileWriterThread(AbstractQueue<Tuple> dataHeap, DataProcessorInfo writerVo,String outLocation)
    {
        this.outLocation = outLocation;

        this.dataProcessorInfo = writerVo;

        this.dataHeap = dataHeap;

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
        try
        {
            if(!new File(this.outLocation).mkdirs())
            {
                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                        "Problem while creating the pagination directory");
            }

            File tempFile = new File(this.outLocation + File.separator + System.nanoTime() + ".tmp");

            dataOutput = FileFactory.getDataOutputStream(tempFile.getAbsolutePath(),
                    FileFactory.getFileType(tempFile.getAbsolutePath()),
                    MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                            * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR);

            int size = this.dataHeap.size();
           
            dataOutput.writeInt(size);
            
            writeDataFromHeap(dataOutput);

            File dest = new File(this.outLocation + File.separator + System.nanoTime()
                    + MolapCommonConstants.QUERY_OUT_FILE_EXT);
            if(!tempFile.renameTo(dest))
            {
                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while renaming the file");
            }
        }
        finally
        {
            MolapUtil.closeStreams(dataOutput);
        }
        this.dataHeap = null;
        return null;
    }

    /**
     * Below method will be used to write data from heap to file
     * 
     * @param dataOutput
     * @throws IOException
     * @throws KeyGenException
     */
    private void writeDataFromHeap(DataOutputStream dataOutput) throws IOException, KeyGenException
    {
        int size = dataHeap.size();
        for(int i = 0;i < size;i++)
        {
            Tuple poll = dataHeap.poll();
            byte[] key = poll.getKey();
            dataOutput.write(key);
            MeasureAggregator[] value = poll.getMeasures();
            for(int j = 0;j < value.length;j++)
            {
                value[j].writeData(dataOutput);
            }
        }
    }

}
