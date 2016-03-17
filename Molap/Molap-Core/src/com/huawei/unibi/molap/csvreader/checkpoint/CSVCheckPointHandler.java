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

package com.huawei.unibi.molap.csvreader.checkpoint;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.csvreader.checkpoint.exception.CheckPointException;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :CSVCheckPointHandler.java 
 * Class Description : CheckPointInterface class 
 * Version 1.0
 */
public class CSVCheckPointHandler implements CheckPointInterface
{

    
    /**
     * CSV_CHECKPOINT_FIELD_INFO_COUNT
     */
    private static final int CSV_CHECKPOINT_FIELD_INFO_COUNT=2;
    
    /**
     * fileLocation
     */
    private String fileLocation;
    
    /**
     * String
     */
    private String fileName;
    
    /**
     * CSVCheckPointHandler
     * @param fileLocation
     */
    public CSVCheckPointHandler(String fileLocation,String fileName)
    {
        this.fileLocation=fileLocation;
        this.fileName=fileName;
    }
    /**
     * Below method will be used to get the checkpoint cache
     * @return check point cache
     * @throws CheckPointException
     *          will throw exception in case of any error while getting the cache
     */
    @Override
    public Map<String, Long> getCheckPointCache() throws CheckPointException
    {
        // create cache
        Map<String,Long> checkPointCache = new HashMap<String, Long>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        File checkPointFile = new File(this.fileLocation + File.separator
                + MolapCommonConstants.CHECKPOINT_FILE_NAME + fileName
                + MolapCommonConstants.CHECKPOINT_EXT);
        // if file not present then last execution was successful return empty map
        if(!checkPointFile.exists())
        {
            return checkPointCache;
        }
        FileInputStream stream = null;
        FileChannel fileChannel= null;
        try
        {
            // create reading stream
            stream = new FileInputStream(checkPointFile);
            // get file channel 
            fileChannel=stream.getChannel();
            // get file size 
            long fileSize = fileChannel.size();
            // current offset 
            long currentOffSet=0;
            // if current offset is less than length then run
//            Record format: <TotalLenght><FileLength><FileName><Offset>
//                            <Int><Int><Int><Long>
            while(currentOffSet < fileSize)
            {
                ByteBuffer buffer = ByteBuffer
                        .allocate(MolapCommonConstants.INT_SIZE_IN_BYTE);
                fileChannel.read(buffer);
                buffer.rewind();
                int totalSize = buffer.getInt();
                currentOffSet += MolapCommonConstants.INT_SIZE_IN_BYTE;
                buffer = ByteBuffer.allocate(totalSize);
                fileChannel.read(buffer);
                buffer.rewind();
                byte[] fileNamebytes = new byte[buffer.getInt()];
                buffer.get(fileNamebytes);
                checkPointCache
                        .put(new String(fileNamebytes,Charset.defaultCharset()), buffer.getLong());
                currentOffSet += totalSize;
            }
        }
        catch(FileNotFoundException e)
        {
           throw new CheckPointException(e);
        }
        catch (IOException e) 
        {
            throw new CheckPointException(e);
        }
        finally
        {
            MolapUtil.closeStreams(fileChannel,stream);
        }
        return checkPointCache;
    }

    /**
     * Below method will be used to store the check point cache 
     * 
     * @param checkPointCache
     *          check point cache 
     * 
     * @throws CheckPointException
     *          problem while storing the checkpoint cache
     */
    @Override
    public void saveCheckPointCache(Map<String, Long> checkPointCache)
            throws CheckPointException
    {
        byte[] nameBytes=null;
        DataOutputStream stream=null;
        String actualFileName=this.fileLocation + File.separator
                + MolapCommonConstants.CHECKPOINT_FILE_NAME + fileName
                + MolapCommonConstants.CHECKPOINT_EXT;
        File newFile = new File(actualFileName
                + MolapCommonConstants.FILE_INPROGRESS_STATUS);
        try
        {
            stream = new DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(newFile), 1024));
            
            for(Entry<String, Long> entrySet:checkPointCache.entrySet())
            {
                nameBytes=entrySet.getKey().getBytes(Charset.defaultCharset());
                stream.writeInt(MolapCommonConstants.INT_SIZE_IN_BYTE + nameBytes.length + MolapCommonConstants.LONG_SIZE_IN_BYTE);
                stream.writeInt(nameBytes.length);
                stream.write(nameBytes);
                long offset=entrySet.getValue();
                stream.writeLong(offset);
            }
        }
        catch (FileNotFoundException e) 
        {
            throw new CheckPointException("Problem while writing the check point file",e);
        }
        catch (IOException e) 
        {
            throw new CheckPointException("Problem while writing the check point file",e);
        }
        finally
        {
            MolapUtil.closeStreams(stream);
            File olderFile=new File(actualFileName);
            if(olderFile.exists())
            {
                File oldFile = new File(actualFileName+MolapCommonConstants.BAK_EXT);
                if(oldFile.exists())
                {
                    if(olderFile.delete())
                    {
                        throw new CheckPointException("Problem while deleting the older .bak extension check point file.");
                    }
                }
                if(!olderFile.renameTo(new File(actualFileName+MolapCommonConstants.BAK_EXT)))
                {
                    throw new CheckPointException("Problem while renaming the older check point file extension to .bak");
                }
                if(!newFile.renameTo(new File(actualFileName)))
                {
                    throw new CheckPointException("Problem while renaming the new check point file extension to: " + actualFileName);
                }
                olderFile=new File(actualFileName+".bak");
                if(!olderFile.delete())
                {
                    throw new CheckPointException("Problem while deleting the older check point file: " + olderFile.getAbsolutePath());
                }
            }
            else
            {
                if(!newFile.renameTo(new File(actualFileName)))
                {
                    throw new CheckPointException("Problem while renaming the new check point file extension to: " + actualFileName);
                }
            }
        }
    }
        
    /**
     * Below method will be used to get the check point info field count
     * @return
     */
    @Override
    public int getCheckPointInfoFieldCount()
    {
        return CSV_CHECKPOINT_FIELD_INFO_COUNT;
    }
    
    @Override
    public void updateInfoFields(Object[] inputRow, Object[] outputRow)
    {
        outputRow[outputRow.length -2] = inputRow[inputRow.length-2];
        outputRow[outputRow.length -1] = inputRow[inputRow.length-1];
    }
    
//    public static void main(String[] args) throws Exception
//    {
//        CSVCheckPointHandler ch = new CSVCheckPointHandler("C:\\", "fact200NE_ap_state");
//        System.out.println(ch.getCheckPointCache());
//    }
}
