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
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.writer;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :24-May-2013 12:16:14 PM
 * FileName : HierarchyValueWriter.java
 * Class Description :
 * Version 1.0
 */
public class HierarchyValueWriterForCSV
{

	/**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(HierarchyValueWriterForCSV.class.getName());
    /**
     * hierarchyName
     */
    private String hierarchyName;
    
    /**
     * bufferedOutStream
     */
    private FileChannel outPutFileChannel;
    
    /**
     * storeFolderLocation
     */
    private String storeFolderLocation;
    
    /**
     * intialized
     */
    private boolean intialized;
    
    /**
     * counter the number of files.
     */
    private int counter;
    
    /**
     * byteArrayList
     */
    private List<ByteArrayHolder> byteArrayholder= new ArrayList<ByteArrayHolder>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    
    /**
     * toflush
     */
    private int toflush;
    
    
    /**
     * 
     * @return Returns the byteArrayList.
     * 
     */
    public List<ByteArrayHolder> getByteArrayList() throws KettleException
    {
        // Write the hierarchy file
//        if(CheckPointHanlder.IS_CHECK_POINT_NEEDED)
//        {
//            if(toflush == byteArrayholder.size())
//            {
//                performRequiredOperation();
//            }
//        }
        
        return byteArrayholder;
    }

    /**
     * 
     * @return Returns the bufferedOutStream.
     * 
     */
    public FileChannel getBufferedOutStream()
    {
        return outPutFileChannel;
    }

    /**
     * 
     * @param storeFolderLocation 
     * @param list 
     * @param trim
     * 
     */
    public HierarchyValueWriterForCSV(String hierarchy, String storeFolderLocation)
    {
        this.hierarchyName = hierarchy;
        this.storeFolderLocation = storeFolderLocation;
        
        MolapProperties instance = MolapProperties.getInstance();
        
        this.toflush = Integer.parseInt(instance.getProperty(
                MolapCommonConstants.SORT_SIZE,
                MolapCommonConstants.SORT_SIZE_DEFAULT_VAL));
        
        int rowSetSize = Integer.parseInt(instance.getProperty(
                MolapCommonConstants.GRAPH_ROWSET_SIZE,
                MolapCommonConstants.GRAPH_ROWSET_SIZE_DEFAULT));
        
        if(this.toflush > rowSetSize)
        {
            this.toflush = rowSetSize;
        }
        
        updateCounter(hierarchy, storeFolderLocation);
    }
    
    
    private void updateCounter(final String meString, String storeFolderLocation)
    {
        File storeFolder = new File(storeFolderLocation);
        
        File[] listFiles = storeFolder.listFiles(new FileFilter()
        {
            
            @Override
            public boolean accept(File file)
            {
                if(file.getName().indexOf(meString) > -1)
                    
                {
                    return true;
                }
                return false;
            }
        });
        
        
        if(listFiles.length == 0)
        {
            counter = 0;
            return;
        }
        
        for(File hierFile : listFiles)
        {
            String hierFileName = hierFile.getName();
            
            if(hierFileName.endsWith(MolapCommonConstants.FILE_INPROGRESS_STATUS))
            {
                hierFileName = hierFileName.substring(0, hierFileName.lastIndexOf('.'));
              //CHECKSTYLE:OFF    Approval No:Approval-262
                try
                {
                    counter = Integer.parseInt(hierFileName.substring(hierFileName.length()-1));
                }
                catch(NumberFormatException nfe)
                {

                    if(new File(hierFileName + '0'+ MolapCommonConstants.LEVEL_FILE_EXTENSION).exists())
                    {
                        // Need to skip because the case can come in which server went down while files were merging and the other hieracrhy 
                        // files were not deleted, and the current file status is inrogress. so again we will merge the files and 
                        // rename to normal file
                        LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Need to skip as this can be case in which hierarchy file already renamed.");
                        if(hierFile.delete())
                        {
                            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Deleted the Inprogress hierarchy Files.");
                        }
                    }
                    else
                    {
                        // levelfileName0.level file not exist that means files is merged and other files got deleted. 
                        // while renaming this file from inprogress to normal file, server got restarted/killed.
                        // so we need to rename the file to normal.
                        
                        File inprogressFile = new File(storeFolder + File.separator + hierFile.getName());
                        File changetoName = new File(storeFolder + File.separator + hierFileName);
                        
                        if(inprogressFile.renameTo(changetoName))
                        {
                            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Renaming the level Files while creating the new instance on server startup.");
                        }
                        
                    }
                    
                }
              //CHECKSTYLE:ON
            }
            
            String val = hierFileName.substring(hierFileName.length()-1);
            
            int parsedVal = getIntValue(val);
            
            if(counter < parsedVal)
            {
                counter = parsedVal;
            }
        }
        counter++;
    }

    /**
     * 
     * @param val
     * @param parsedVal
     * @return
     * 
     */
    private int getIntValue(String val)
    {
        int parsedVal = 0;
        try
        {
            parsedVal = Integer.parseInt(val);
        }
        catch(NumberFormatException nfe)
        {
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Hierarchy File is already renamed so there will not be"
                    + "any need to keep the counter");
        }
        return parsedVal;
    }
    /**
     * @param storeFolderLocation
     * @throws KettleException
     */
    private void intialize() throws KettleException 
    {
        intialized = true;
        
        File f = new File(storeFolderLocation + File.separator + hierarchyName + counter + MolapCommonConstants.FILE_INPROGRESS_STATUS);

        counter++;
        
        FileOutputStream fos = null;
       
        boolean isFileCreated = false;
        if(!f.exists())
        {
            try
            {
               isFileCreated = f.createNewFile();

            }
            catch(IOException e)
            {
                // TODO : logging
                //not required: findbugs fix
                //closeStreamAndDeleteFile(f,bufferedOutputStream ,out);
                throw new KettleException("unable to create member mapping file", e);
            }
            if(!isFileCreated)
            {
                 throw new KettleException("unable to create file" + f.getAbsolutePath());
            }
        }

        try
        {
            fos = new FileOutputStream(f);
            
            outPutFileChannel = fos.getChannel();
        }
        catch(FileNotFoundException e)
        {
            // TODO : logging 
            closeStreamAndDeleteFile(f,outPutFileChannel ,fos);
            throw new KettleException("member Mapping File not found to write mapping info", e);
        }
    }
    
    
    /**
     * @param bytes
     * @throws KettleException
     */
    public void writeIntoHierarchyFile(byte[] bytes,int primaryKey) throws KettleException
    {
        if(!intialized)
        {
            intialize();
        }
        
        ByteBuffer byteBuffer = storeValueInCache(bytes, primaryKey);

        try
        {
            byteBuffer.flip();
            outPutFileChannel.write(byteBuffer);
        }
        catch(IOException e)
        {
            throw new KettleException("Error while writting in the hierarchy mapping file", e);
        }
    }
    
    /**
     * 
     * @param value
     * @param key
     * @param properties
     * @return
     * 
     */
    private ByteBuffer storeValueInCache(byte[] bytes, int primaryKey)
    {
        
        // adding 4 to store the total length of the row at the beginning
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 4);
        
        buffer.put(bytes);
        buffer.putInt(primaryKey);
        
        return buffer;
    }
    
    
    public void performRequiredOperation()  throws KettleException
    {
        if(byteArrayholder.size() == 0)
        {
//            MolapUtil.closeStreams(outPutFileChannel);
//            String filePath = this.storeFolderLocation + File.separator + hierarchyName + (counter-1) + MolapCommonConstants.FILE_INPROGRESS_STATUS;
//            File inProgressFile = new File(filePath);
//            inProgressFile.delete();
//            counter--;
//            //create the new outputStream 
//            try
//            {
//                intialize();
//            }
//            catch(KettleException e)
//            {
//                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Not able to create the output stream for File :" + hierarchyName + (counter-1));
//            }
            return;
        }
        //write to the file and close the stream.
        Collections.sort(byteArrayholder);
        
        for(ByteArrayHolder byteArray : byteArrayholder)
        {
            writeIntoHierarchyFile(byteArray.getMdKey(), byteArray.getPrimaryKey());
        }
        
        MolapUtil.closeStreams(outPutFileChannel);
        
        //rename the inprogress file to normal .level file
        String filePath = this.storeFolderLocation + File.separator + hierarchyName + (counter-1) + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        File inProgressFile = new File(filePath);
        String inprogressFileName = inProgressFile.getName();
        
        String changedFileName = inprogressFileName.substring(0, inprogressFileName.lastIndexOf('.'));
       
        File orgFinalName = new File(this.storeFolderLocation + File.separator
                + changedFileName);

        if(!inProgressFile.renameTo(orgFinalName))
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Not able to rename file : " + inprogressFileName);
        }
                
       //create the new outputStream 
        try
        {
            intialize();
        }
        catch(KettleException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Not able to create the output stream for File :" + hierarchyName + (counter-1));
        }
        
        //clear the byte array holder also.
        byteArrayholder.clear();
    }
    
    private void closeStreamAndDeleteFile(File f, Closeable... streams) throws KettleException
    {
    	boolean isDeleted = false;
        for(Closeable stream : streams)
        {
            if(null != stream)
            {
                try
                {
                    stream.close();
                }
                catch(IOException e)
                {
                    // throw new KettleException("unable to close the stream",
                    // e);
                    LOGGER.error(
                            MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                            e, "unable to close the stream ");
                }

            }
        }
        
        // delete the file
        isDeleted = f.delete();
        if(!isDeleted)
        {
            LOGGER.error(
                    MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Unable to delete the file " + f.getAbsolutePath());
        }
        
    }

    /**
     * 
     * @return Returns the hierarchyName.
     * 
     */
    public String getHierarchyName()
    {
        return hierarchyName;
    }

    public int getCounter()
    {
        return counter;
    }

}

