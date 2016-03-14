/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/nSn2OCpRdJIFiH39ZwlNN3x//pST74bYfAiivEpfjbXE1rdmnnIlS7jSfAvLsDRYpy7
hddLgXWQIfJpwqwfOIeiyEdrJHGPkERfmPbnyDmn5modi0Ly5rqm9LbmQmC3kg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.surrogatekeysgenerator.dbbased;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import org.pentaho.di.core.exception.KettleException;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :24-May-2013 12:16:14 PM
 * FileName : HierarchyValueWriter.java
 * Class Description :
 * Version 1.0
 */
public class HierarchyValueWriter
{

	/**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(HierarchyValueWriter.class.getName());
    /**
     * hierarchyName
     */
    private String hierarchyName;
    
    /**
     * bufferedOutStream
     */
    private BufferedOutputStream bufferedOutStream;
    
    /**
     * BUFFFER_SIZE
     */
    private static final int BUFFFER_SIZE = 32768;
    
    /**
     * storeFolderLocation
     */
    private String storeFolderLocation;
    
    /**
     * byteArrayList
     */
    private List<byte[]> byteArrayList= new ArrayList<byte[]>();
    
    
    /**
     * 
     * @return Returns the byteArrayList.
     * 
     */
    public List<byte[]> getByteArrayList()
    {
        return byteArrayList;
    }

    /**
     * 
     * @param byteArrayList The byteArrayList to set.
     * 
     */
    public void setByteArrayList(List<byte[]> byteArrayList)
    {
        this.byteArrayList = byteArrayList;
    }

    /**
     * 
     * @return Returns the bufferedOutStream.
     * 
     */
    public BufferedOutputStream getBufferedOutStream()
    {
        return bufferedOutStream;
    }

    /**
     * 
     * @param storeFolderLocation 
     * @param list 
     * @param trim
     * 
     */
    public HierarchyValueWriter(String hierarchy, String storeFolderLocation)
    {
        this.hierarchyName = hierarchy;
        this.storeFolderLocation = storeFolderLocation;
    }
    
    
    /**
     * @param bytes
     * @throws KettleException
     */
    public void writeIntoHierarchyFile(byte[] bytes) throws KettleException
    {
            File f = new File(storeFolderLocation + File.separator + hierarchyName);

            FileOutputStream fos = null;

            // ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
            // buffer.put(bytes);
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
                    //closeStreamAndDeleteFile(f, bufferedOutStream,fos);
                	// delete the file
                                        
                    throw new KettleException("unable to create file", e);
                }
                if(!isFileCreated)
                {
                	 throw new KettleException("unable to create file" + f.getAbsolutePath());
                }
            }
            try
            {
                if(null == bufferedOutStream)
                {
                    fos = new FileOutputStream(f);
                    bufferedOutStream = new BufferedOutputStream(fos,BUFFFER_SIZE);

                }
                    bufferedOutStream.write(bytes);

            }
            catch(FileNotFoundException e)
            {
                // TODO : logging 
                closeStreamAndDeleteFile(f, bufferedOutStream,fos);
                throw new KettleException("hierarchy mapping file not found", e);
            }
            catch(IOException e)
            {
                // TODO : logging 
                closeStreamAndDeleteFile(f, bufferedOutStream,fos);
                throw new KettleException("Error while writting in the hierarchy mapping file", e);
            }
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
                            MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            e, "unable to close the stream ");
                }

            }
        }
        
        // delete the file
        isDeleted = f.delete();
        if(!isDeleted)
        {
            LOGGER.error(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
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

}

