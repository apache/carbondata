/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d0DebYPOTY5EEEbPew1WgO40egWK6f0mL8apDujTbAdIJaFxmwMbIXXKWkCJl61wFgMl
xUphcjumnqiX4p6rdg4Jh6sECPH//yeB0vWgW+2VHgTwIhKwbO44RUeCnJrG0g==*/
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
package com.huawei.unibi.molap.engine.reader;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.reader.exception.ResultReaderException;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :DataFileChunkHolder.java
 * Class Description : DataFileChunkHolder  class  
 * Version 1.0
 */
public class ResultTempFileReader
{
    
    /**
     *  temp file 
     */
    private String inFilePath;

    /**
     * read stream
     */
    private DataInputStream stream;

    /**
     * entry count 
     */
    private int entryCount;

    /**
     * number record read 
     */
    private int numberOfRecordRead;

    /**
     * fileBufferSize
     */
    private int fileBufferSize;
    
    /**
     * keySize
     */
    private int keySize;
    
    /**
     * MeasureAggregator
     */
    private  MeasureAggregator[] measureAggregator;
    
    /**
     * byteArrayWrapper
     */
    private ByteArrayWrapper byteArrayWrapper;
    
    /**
     * MolapSortTempFileChunkHolder Constructor
     * 
     * @param inFile
     *          temp file 
     * @param recordSize
     *          measure count 
     * @param keySize
     *          mdkey length 
     *
     */
    public ResultTempFileReader(String inFilePath, int keySize, final MeasureAggregator[] measureAggregator, int fileBufferSize)
    {
        // set temp file 
        this.inFilePath = inFilePath;
        // key size
        this.keySize=keySize;
        // measure aggregator
        this.measureAggregator = measureAggregator;
        //fileBufferSize
        this.fileBufferSize=fileBufferSize;
        // byte array wrapper
        this.byteArrayWrapper= new ByteArrayWrapper(); 
        // out stream
    }
        

    /**
     * This method will be used to initialize 
     * 
     * @throws MolapSortKeyAndGroupByException
     *          problem while initializing 
     */
    public void initialize() throws ResultReaderException
    {
        FileHolder fileHolder = null;
        try
        {
            FileType fileType = FileFactory.getFileType(inFilePath);
            fileHolder =FileFactory.getFileHolder(fileType);
            if(fileType.equals(FileType.HDFS))
            {
                this.entryCount= fileHolder.readInt(inFilePath, 0);
            }
            else
            {
                fileHolder =FileFactory.getFileHolder(fileType);
                this.entryCount= fileHolder.readInt(inFilePath, 0);
            }
            this.stream = FileFactory.getDataInputStream(inFilePath, fileType, this.fileBufferSize);
            this.stream.readInt();
        }
        catch(FileNotFoundException e)
        {
            throw new ResultReaderException(e);
        }
        catch(IOException e)
        {
            throw new ResultReaderException(e);
        }
        finally
        {
            if(null!=fileHolder)
            {
                fileHolder.finish();
            }
        }
    }
    
    /**
     * This method will be used to read new row from file 
     * 
     * @throws MolapSortKeyAndGroupByException
     *          problem while reading 
     *
     */
    public void readRow() throws ResultReaderException
    {
        try
        {
            byte[] mdKey = new byte[this.keySize];
            this.stream.readFully(mdKey);
            
//                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while reading mdkey from pagination temp file");
            
            this.byteArrayWrapper.setMaskedKey(mdKey);
            for(int i = 0;i <  this.measureAggregator.length;i++)
            {
                this.measureAggregator[i].readData(this.stream);
            }
            numberOfRecordRead++;
        }
        catch(FileNotFoundException e)
        {
            throw new ResultReaderException(this.inFilePath + " No Found ", e);
        }
        catch(IOException e)
        {
            throw new ResultReaderException(" Problem while reading" + this.inFilePath, e);
        }
    }

    /**
     * below method will be used to get the row 
     * 
     * @return row
     *
     */
    public byte[] getRow()
    {
        return this.byteArrayWrapper.getMaskedKey();
    }

    /**
     * Below method will be used to close stream
     */
    public void closeStream()
    {
        MolapUtil.closeStreams(this.stream);
    }
    
    /**
     * below method will be used to check whether any more records are present in file or not
     * 
     * @return more row present in file 
     *
     */
    public boolean hasNext()
    {
        return this.numberOfRecordRead < this.entryCount;
    }

    /**
     * This method will return the data array
     * 
     * @param index
     * @return
     *
     */
    public MeasureAggregator[] getMeasures() 
    {
        return this.measureAggregator;
    }
    
    /**
     * This method will return the key array
     * 
     * @return
     *
     */
    public byte[] getKey()
    {
        return this.byteArrayWrapper.getMaskedKey();
    }
    
    public void setMeasureAggs(final MeasureAggregator[] aggs)
    {
        this.measureAggregator = aggs;
    }
    
}
