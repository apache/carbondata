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

package com.huawei.unibi.molap.engine.reader;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
//import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;
import com.huawei.unibi.molap.engine.reader.exception.ResultReaderException;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : QueryDataFileReader.java
 * Description   : This class is responsible for reading the query result from the result file.
 * Class Version  : 1.0
 */
public class QueryDataFileReader
{
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(QueryDataFileReader.class.getName());
    /**
     * FileHolder fileHolder
     */
    private FileHolder fileHolder;

    /**
     * String filePath
     */
    private String filePath;

    /**
     * DataProcessorInfo info
     */
    private DataProcessorInfo info;

    /**
     * 
     * @param filePath
     * @param info
     */
    public QueryDataFileReader(String filePath, DataProcessorInfo info)
    {
        fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filePath));
        this.filePath = filePath;
        this.info = info;
    }

    /**
     * Reading the query result from result file and also doing the snappy uncompression.
     * @param leafNodeInfo
     * @return
     * @throws ResultReaderException
     */
    public QueryResult prepareResultFromFile(LeafNodeInfo leafNodeInfo) throws ResultReaderException
    {
        QueryResult queryResult = new QueryResult();
        byte[] keyArray = fileHolder.readByteArray(this.filePath, leafNodeInfo.getKeyOffset(),
                leafNodeInfo.getKeyLength());
        MeasureAggregator[] measureAggregators = AggUtil.getAggregators(info.getAggType(), false, info.getKeyGenerator(), info
                        .getCubeUniqueName(),info.getMsrMinValue(),info.getHighCardinalityTypes());

        DataInputStream[] msrStreams = new DataInputStream[leafNodeInfo.getMeasureLength().length];

        for(int j = 0;j < msrStreams.length;j++)
        {
            msrStreams[j] = new DataInputStream(new ByteArrayInputStream(
                    SnappyByteCompression.INSTANCE.unCompress(fileHolder.readByteArray(filePath,
                            leafNodeInfo.getMeasureOffset()[j], leafNodeInfo.getMeasureLength()[j]))));
        }

        DataInputStream keyStream = new DataInputStream(new ByteArrayInputStream(
                SnappyByteCompression.INSTANCE.unCompress(keyArray)));
        try
        {
            for(int j = 0;j < leafNodeInfo.getNumberOfKeys();j++)
            {
                byte[] key = new byte[info.getKeySize()];
                keyStream.readFully(key);
                for(int k = 0;k < measureAggregators.length;k++)
                {
                    measureAggregators[k].readData(msrStreams[k]);
                }
                ByteArrayWrapper wrapper = new ByteArrayWrapper();
                wrapper.setMaskedKey(key);
                queryResult.add(wrapper, measureAggregators);
                measureAggregators = AggUtil.getAggregators(info.getAggType(), false, info.getKeyGenerator(), info
                        .getCubeUniqueName(),info.getMsrMinValue(),info.getHighCardinalityTypes());
            }
        }
        catch(IOException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e,"Problem while reading the query out file");
            throw new ResultReaderException(e);
        }
        finally
        {
           MolapUtil.closeStreams(keyStream);
           MolapUtil.closeStreams(msrStreams);
        }
        return queryResult;
    }
    
    /**
     * for deleting file and closing streams.
     */
    public void close()
    {
        if(null != fileHolder)
        {
            fileHolder.finish();
            if(!(FileFactory.getMolapFile(filePath, FileFactory.getFileType(filePath)).delete()))
            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                        "Problem while deleting the pagination temp file" + filePath);
            }
        }
    }
}
