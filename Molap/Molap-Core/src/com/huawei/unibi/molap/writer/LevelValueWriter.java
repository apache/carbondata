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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
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
 * Created Date :24-May-2013 12:15:55 PM
 * FileName : LevelValueWriter.java
 * Class Description :
 * Version 1.0
 */
public class LevelValueWriter
{
    /**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(LevelValueWriter.class.getName());
    /**
     * memberFileName
     */
    private String memberFileName;

    /**
     * bufferedOutputStream <code> FileChannel </code>
     */
    private DataOutputStream dataOutputStream;

    /**
     * storeFolderLocation
     */
    private String storeFolderLocation;
    
    
    private boolean intialized;
    
    /**
     * toBuffer
     */
    private int bufferedRecords;
    
    /**
     * toflush
     */
    private int toflush;
    
    /**
     * counter the number of files.
     */
    private int counter;
    
    /**
     * 
     * First time flag for surrogate key gen step
     * 
     */
    private boolean isFirstTimeSurrogateWrite;
    
    private int maxSurrogate;
    
    //TODO SIMIAN
    /**
     * 
     * @param meString
     * @param storeFolderLocation
     * @throws KettleException 
     */
    
    public LevelValueWriter(String meString, String storeFolderLocation) throws KettleException
    {
        MolapProperties instance = MolapProperties.getInstance();

        this.memberFileName = meString;
        this.storeFolderLocation = storeFolderLocation;
        
        
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
        
        updateCounter(meString, storeFolderLocation);
        
        isFirstTimeSurrogateWrite = true;
        maxSurrogate=Integer.MIN_VALUE;
    }

    //TODO SIMIAN
    private void updateCounter(final String meString, String storeFolderLocation)
    {
        File storeFolder = new File(storeFolderLocation); 
        
        File[] filesList = storeFolder.listFiles(new FileFilter()
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
        
        if(filesList.length == 0)
        {
            counter = 0;
            return;
        }
        
        for(File levelFile : filesList)
        {
            String levelFileName = levelFile.getName();
            
            if(levelFileName.endsWith(MolapCommonConstants.FILE_INPROGRESS_STATUS))
            {
                levelFileName = levelFileName.substring(0, levelFileName.lastIndexOf('.'));
              //CHECKSTYLE:OFF    Approval No:Approval-262
                try
                {
                    counter = Integer.parseInt(levelFileName.substring(meString.length()));
                    if(levelFile.length() == 0)
                    {
                        counter--;
                    }
                    else // Rename level file to normal File.
                    {
                        File inprogressFile = new File(storeFolder + File.separator + levelFile.getName());
                        File changetoName = new File(storeFolder + File.separator + levelFileName);
                        
                        if(inprogressFile.renameTo(changetoName))
                        {
                            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Renaming the level Files while creating the new instance on server startup.");
                        }
                    }
                }
                catch(NumberFormatException nfe)
                {
                    if(new File(levelFileName + '0'+ MolapCommonConstants.LEVEL_FILE_EXTENSION).exists())
                    {
                        // Need to skip because the case can come in which server went down while files were merging and the other level 
                        // files were not deleted, and the current file status is inrogress. so again we will merge the files and 
                        // rename to normal file
                        LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Need to skip as this can be case in which level file already renamed.");
                        if(levelFile.delete())
                        {
                            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Deleted the Inprogress level Files.");
                        }
                    }
                    else
                    {
                        // levelfileName0.level file not exist that means files is merged and other files got deleted. 
                        // while renaming this file from inprogress to normal file, server got restarted/killed.
                        // so we need to rename the file to normal.
                        
                        File inprogressFile = new File(storeFolder + File.separator + levelFile.getName());
                        File changetoName = new File(storeFolder + File.separator + levelFileName);
                        
                        if(inprogressFile.renameTo(changetoName))
                        {
                            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Renaming the level Files while creating the new instance on server startup.");
                        }
                        
                    }
                }
                
              //CHECKSTYLE:ON
                
            }
            else
            {
                String val = levelFileName.substring(meString.length());
                
                int parsedVal = getIntValue(val);
                
                if(counter < parsedVal)
                {
                    counter = parsedVal;
                }
            }
        }
        counter++;
    }

    /**
     * Return the Int Value
     * @param val
     * @return
     * 
     */
    private int getIntValue(String val)
    {
        int parsedVal = 0;
        // In case of normalized schema load the files will be loaded at the earliest
        // so not required to maintain the counter.
        try
        {
            parsedVal = Integer.parseInt(val);
        }
        catch(NumberFormatException nfe)
        {
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Level File is already renamed so there will not be" +
                    "any need to keep the counter");
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

        File f = new File(storeFolderLocation + File.separator + memberFileName + counter + MolapCommonConstants.FILE_INPROGRESS_STATUS);
        
        counter++;
        
        FileOutputStream out = null;
       
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
            out = new FileOutputStream(f);
//            int bufferSize = Integer.parseInt(MolapProperties.getInstance().getProperty("molap.level.write.bufferinkb", "64"));
            dataOutputStream = new DataOutputStream(new BufferedOutputStream(out,MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR*MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR));
        }
        catch(FileNotFoundException e)
        {
            // TODO : logging
            closeStreamAndDeleteFile(f,dataOutputStream ,out);
            throw new KettleException("member Mapping File not found to write mapping info", e);
        }
    }

    /**
     * This method will write the surrogate to level files.
     * 
     * @param value
     * @param key
     * @param properties
     * @throws KettleException
     */
    public void writeIntoLevelFile(String value, int key, Object[] properties)
            throws KettleException 
    {
        if(!intialized)
        {
            intialize();
        }
//      if(CheckPointHanlder.IS_CHECK_POINT_NEEDED)
//      {
//          if(toflush == bufferedRecords)
//          {
//              // If bufferredrecords is equal to sort size then we need to do 
//              // below operations.
//              //1. close the current File Stream.
//              //2. Renamed the file from inprogress to normal.
//              //3. and create new stream for new data.
//              
//              performRequiredOperation();
//              bufferedRecords =0;
//          }
//      }
        try 
        {
            if(isFirstTimeSurrogateWrite)
            {
                writeMinValue(key);
                isFirstTimeSurrogateWrite = false;
            }
            storeValueInCache(value, key, properties);
            if(maxSurrogate<key)
            {
                maxSurrogate=key;
            }
            bufferedRecords++;
        }
        catch (IOException e) 
        {
            throw new KettleException("Unable to write in Member mapping file",e);
        }

    }
    
    //TODO SIMIAN  
    /**
     * Perform Operation required for creating new level file.
     * 
     * 
     *
     */
    public void performRequiredOperation()
    {
        if(bufferedRecords == 0)
        {
//            MolapUtil.closeStreams(dataOutputStream);
//            String filePath = this.storeFolderLocation + File.separator + memberFileName + (counter-1) + MolapCommonConstants.FILE_INPROGRESS_STATUS;
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
//                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Not able to create the output stream for File :" + memberFileName + (counter-1));
//            }
            return;
        }
        //Close the stream
        MolapUtil.closeStreams(dataOutputStream);
        
        //rename the inprogress file to normal .level file
        String filePath = this.storeFolderLocation + File.separator + memberFileName + (counter-1) + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        File inProgressFile = new File(filePath);
        String inprogressFileName = inProgressFile.getName();
        
        String changedFileName = inprogressFileName.substring(0, inprogressFileName.lastIndexOf('.'));
       
        File existFinalName = new File(this.storeFolderLocation + File.separator
                + changedFileName);

        if(!inProgressFile.renameTo(existFinalName)) 
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
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Not able to create the output stream for File :" + memberFileName + (counter-1));
        }
        
        bufferedRecords =0;
        
    }
    
    /**
     * 
     * @param value
     * @param key
     * @param properties
     * @return
     * @throws IOException
     * 
     */
    private void writeMinValue(int minMaxValue) throws IOException
    {
        dataOutputStream.writeInt(minMaxValue);
    }
    
    /**
     * 
     * @param value
     * @param key
     * @param properties
     * @return
     * @throws IOException
     * 
     */
    public void writeMaxValue() throws IOException
    {
        dataOutputStream.writeInt(maxSurrogate);
    }

    /**
     * 
     * @param value
     * @param key
     * @param properties
     * @return
     * @throws IOException 
     * 
     */
    private void storeValueInCache(String value, int key,
            Object[] properties) throws IOException
    {
        // holds the total length of the row. initializing with 4 since it takes 4 bytes to store the length of the row.
     //   int rowLength = 0;
        
        //Add member value length. as for storing member length we take 4 bytes.
//        rowLength = rowLength + 4;
        
        // to store surrogate key it requires 4 bytes.
//        rowLength = rowLength + 4;
        
        // Holds the size of the properties 
        int propertySize = properties.length;
        
        // Holds 4 bytes for each of the property name length.
     //   rowLength = rowLength + propertySize * 4;
        
        // Holds the length of the individual properties name length 
        int []propertyLength = new int[propertySize];
        
        // holds the byte array of of the Properties.
        byte[][] propertiesByte = new byte[propertySize][];
      //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_005
        boolean enableEncoding = Boolean.valueOf(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.ENABLE_BASE64_ENCODING,
                MolapCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
      //CHECKSTYLE:ON
        int i = 0;
        for(Object obj: properties)
        {
            String prop = obj.toString();
            byte[] propBytes = null;
          //CHECKSTYLE:OFF    Approval No: Approval-193
            
            if(enableEncoding)
            {
                propBytes = Base64.encodeBase64(prop.getBytes(Charset.defaultCharset()));
            }
            else
            {
                propBytes = prop.getBytes(Charset.defaultCharset());
            }
                
            
            //CHECKSTYLE:ON    Approval No: Approval-193
            propertyLength[i] = propBytes.length;
            propertiesByte[i] = propBytes;
         //   rowLength = rowLength + propBytes.length;
            i++;
        }
        
        
        byte[] memberBytes;
        
        if(enableEncoding)
        {
            memberBytes = Base64.encodeBase64(value.getBytes(Charset.defaultCharset()));
        }
        else
        {
            memberBytes = value.getBytes(Charset.defaultCharset());
        }
        
     //   rowLength = rowLength + memberBytes.length;
        
//        ByteBuffer buffer = ByteBuffer.allocate(rowLength+4);
       // buffer.clear();
        // put total length in the buffer to get the row end
//        dataOutputStream.writeInt(rowLength);
        dataOutputStream.writeInt(memberBytes.length);
        dataOutputStream.write(memberBytes);
//        dataOutputStream.writeInt(key);
        for(int j=0; j < propertySize; j++)
        {
            dataOutputStream.writeInt(propertyLength[j]);
            dataOutputStream.write(propertiesByte[j]);
        }
//        return buffer;
    }
    
    /**
     * 
     * @return Returns the bufferedOutputStream.
     * 
     */
    public OutputStream getBufferedOutputStream()
    {
        return dataOutputStream;
    }
    
    public void clearOutputStream()
    {
        dataOutputStream= null;
    }
    
    /**
     * Close the stream and delete the level file.
     * 
     * @param f
     * @param streams
     * @throws KettleException
     *
     */
    private void closeStreamAndDeleteFile(File f, Closeable... streams) throws KettleException
    {
        boolean isDeleted = false;
        for(Closeable stream : streams)
        {
            if(null != stream)
            {
              //CHECKSTYLE:OFF    Approval No: Approval-298
                try
                {
                    stream.close();
                }
                catch(IOException e)
                {
                    // throw new KettleException("Unable to close Steam", e);
                    LOGGER.error(
                            MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                            e, "Unable to close Steam ");
                }
              //CHECKSTYLE:ON    Approval No: Approval-298
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
     * @return Returns the memberFileName.
     * 
     */
    public String getMemberFileName()
    {
        return memberFileName;
    }

    /**
     * return the counter of the current level file.    
     * 
     * @return
     *
     */
    public int getCounter()
    {
        return counter;
    }
}

