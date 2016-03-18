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
TTt3d2fR1mKJrpuJi4zf6Ohi1r1Vw8Ab5QUfWPWH3PrRT+pNSl3okA1beKcE3rRUCneBekqd
9bs2/YCl8QGx15rzrJTFIsTTAwxvMxbZNVMSTynBlcoQ/2FfIGus8nPbsKuQ6A==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.groupby.GroupByHolder;
import com.huawei.unibi.molap.engine.executer.pagination.DataProcessor;
import com.huawei.unibi.molap.engine.executer.pagination.PaginationModel;
import com.huawei.unibi.molap.engine.executer.pagination.exception.MolapPaginationException;
import com.huawei.unibi.molap.engine.executer.pagination.lru.LRUCacheKey;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.writer.MolapDataWriter;
import com.huawei.unibi.molap.writer.exception.MolapDataWriterException;

/**
 * It writes the data to file in blocks manner
 * @author R00900208
 *
 */
public class MolapQueryDataWriter implements DataProcessor
{
    /**
     * outLocation
     */
    private String outLocation;
    
    /**
     * queryId
     */
    private String queryId;
    

    /**
     * keySize
     */
    private int keySize;
    
    /**
     * blockSize
     */
    private int blockSize;
    
    /**
     *blockDataArray 
     */
    private ByteArrayOutputStream[]blockDataArray;
    
    /**
     *blockDataArray 
     */
    private DataOutputStream[] msrDataOutStreams;
    
    /**
     * blockKeyArray
     */
    private byte[] blockKeyArray;
    
    
    /**
     * entryCount
     */
    private int entryCount;
    
    /**
     * startKey
     */
    private byte[] startKey;
    
    /**
     * endKey
     */
    private byte[] endKey;
    
    /**
     * dataWriter
     */
    private MolapDataWriter dataWriter;
    
    /**
     * measureAggregators
     */
    private MeasureAggregator[]measureAggregators;
    
    /**
     * rowCount
     */
    private int rowCount;
    
    /**
     * holder
     */
    private LRUCacheKey holder;
    
    /**
     * model
     */
    private PaginationModel model;
    
    
    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapQueryDataWriter.class.getName());
    

    /**
     * Intialize with model
     */
    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException
    {
        this.model = model;
        this.blockSize = model.getBlockSize();
        this.outLocation = model.getOutLocation();
        this.queryId = model.getQueryId();
        this.measureAggregators = model.getMeasureAggregators();
        this.keySize = model.getKeySize();
        this.blockKeyArray=new byte[blockSize*keySize];
        createMsrDataOutStrms();
        this.dataWriter = new MolapDataWriter(outLocation, this.measureAggregators.length, this.keySize, queryId,
                MolapCommonConstants.QUERY_MERGED_FILE_EXT, false, false);
        holder = model.getHolder();
        try
        {
            this.dataWriter.initChannel();
        }
        catch(MolapDataWriterException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
            throw new MolapPaginationException(e);
        }
        
    }
    

    /**
     * finish
     */
    public void finish() throws MolapPaginationException
    {
        try
        {
            if(this.entryCount>0)
            {
                
                this.dataWriter.writeDataToFile(SnappyByteCompression.INSTANCE.compress(blockKeyArray), convertToByteArray(), entryCount, startKey, endKey);
                this.dataWriter.writeleafMetaDataToFile();
            }
            else
            {
                this.dataWriter.writeleafMetaDataToFile();
            }
            close();
            MolapFile file = this.dataWriter.closeChannle();
            holder.setIncrementalSize(file.getSize());
            holder.setPath(file.getAbsolutePath());
            model.setRowCount(rowCount);
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Total row count for query "+queryId +" : "+ rowCount);
        }
        catch (Exception e) 
        {
            throw new MolapPaginationException(e);
        }
    }
    
    private void close()
    {
        try
        {
        for(int i = 0;i < blockDataArray.length;i++)
        {
            blockDataArray[i].close();
            msrDataOutStreams[i].close();
        }
        }
        catch (Exception e) 
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e);
        }
    }
    
    
    private byte[][] convertToByteArray()
    {
        byte[][] b = new byte[blockDataArray.length][];
        for(int i = 0;i < b.length;i++)
        {
            b[i] = SnappyByteCompression.INSTANCE.compress(blockDataArray[i].toByteArray());
        }
        return b;
    }
    
    private void createMsrDataOutStrms()
    {
        this.blockDataArray= new ByteArrayOutputStream[this.measureAggregators.length];
        msrDataOutStreams = new DataOutputStream[this.measureAggregators.length];
        for(int i = 0;i < this.measureAggregators.length;i++)
        {
            this.blockDataArray[i]= new ByteArrayOutputStream(blockSize*MolapCommonConstants.INT_SIZE_IN_BYTE);
            msrDataOutStreams[i] = new DataOutputStream(this.blockDataArray[i]);
        }
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures) throws MolapPaginationException
    {
        try
        {
            if(this.entryCount==this.blockSize)
            {
                try
                {
                    this.dataWriter.writeDataToFile(SnappyByteCompression.INSTANCE.compress(blockKeyArray),  convertToByteArray(), this.entryCount, this.startKey, this.endKey);
                    close();
                    createMsrDataOutStrms();
                }
                catch(MolapDataWriterException e)
                {
                    throw new MolapPaginationException("Problem while writing the data to file: ", e);
                }
                this.entryCount = 0;
            }
            
            if(this.entryCount==0)
            {
                this.startKey=key;
            }
            this.endKey=key;
        
            System.arraycopy(key, 0,blockKeyArray , keySize*entryCount, key.length);
   
            for(int i = 0;i < measures.length;i++)
            {
                measures[i].writeData(msrDataOutStreams[i]);
            }
        }
        catch (Exception e) 
        {
            throw new MolapPaginationException(e);
        }
        rowCount++;
        entryCount++;
        
    }


    /**
     * processGroup
     */
    @Override
    public void processGroup(GroupByHolder groupByHolder)
    {
        // No need to implement any thing
        
    }



}
