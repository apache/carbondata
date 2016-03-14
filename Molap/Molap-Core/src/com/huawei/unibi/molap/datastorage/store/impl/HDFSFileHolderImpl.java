/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
//import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;

/**
 * @author R00900208
 *
 */
public class HDFSFileHolderImpl implements FileHolder 
{
    
    /**
     * cache to hold filename and its stream
     */
    private Map<String, FSDataInputStream> fileNameAndStreamCache;
    private static final LogService LOGGER = LogServiceFactory.getLogService(HDFSFileHolderImpl.class.getName());
    
    public HDFSFileHolderImpl()
    {
        this.fileNameAndStreamCache = new HashMap<String, FSDataInputStream>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    @Override
    public byte[] readByteArray(String filePath, long offset, int length)
    {
        
        FSDataInputStream fileChannel = updateCache(filePath);
        byte[] byteBffer = read(fileChannel, length, offset);
        return byteBffer;
    }
    
    /**
     * This method will be used to check whether stream is already present in
     * cache or not for filepath if not present then create it and then add to
     * cache, other wise get from cache
     * 
     * @param filePath
     *          fully qualified file path
     * @return channel
     * 
     */
    private FSDataInputStream updateCache(String filePath)
    {
        FSDataInputStream fileChannel = fileNameAndStreamCache.get(filePath);
        try
        {
            if(null == fileChannel)
            {
                Path pt=new Path(filePath);
                FileSystem fs = pt.getFileSystem(new Configuration());
                fileChannel = fs.open(pt);
                fileNameAndStreamCache.put(filePath, fileChannel);
            }
        }
        catch(IOException e)
        {           
//            e.printStackTrace();
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,e, e.getMessage());  
        }
        return fileChannel;
    }
    
    /**
     * This method will be used to read from file based on number of bytes to be read and positon
     * 
     * @param channel
     *          file channel
     * @param size
     *          number of bytes
     * @param offset
     *          position
     * @return  byte buffer
     *
     */
    private byte[] read(FSDataInputStream channel, int size, long offset)
    {
        byte[] byteBffer = new byte[size];
        try
        {
            channel.seek(offset);
            channel.readFully(byteBffer);
        }
        catch(Exception e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());   
        }
        return byteBffer;
    }
    
    /**
     * This method will be used to read from file based on number of bytes to be read and positon
     * 
     * @param channel
     *          file channel
     * @param size
     *          number of bytes
     * @param offset
     *          position
     * @return  byte buffer
     *
     */
    private byte[] read(FSDataInputStream channel, int size)
    {
        byte[] byteBffer = new byte[size];
        try
        {
            channel.readFully(byteBffer);
        }
        catch(Exception e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
        return byteBffer;
    }

    @Override
    public int readInt(String filePath, long offset)
    {
        FSDataInputStream fileChannel = updateCache(filePath);
        int i = -1;
        try
        {
            fileChannel.seek(offset);
            i = fileChannel.readInt();
        }
        catch(IOException e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
        
        return i;
    }

    @Override
    public long readDouble(String filePath, long offset)
    {
        FSDataInputStream fileChannel = updateCache(filePath);
        long i = -1;
        try
        {
            fileChannel.seek(offset);
            i = fileChannel.readLong();
        }
        catch(IOException e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
        
        return i;
    }

    @Override
    public void finish()
    {
        for(Entry<String, FSDataInputStream> entry : fileNameAndStreamCache.entrySet())
        {
            try
            {
                FSDataInputStream channel = entry.getValue();
                if(null!=channel)
                {
                    channel.close();
                }
            }
            catch(IOException exception)
            {
//                exception.printStackTrace();
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, exception, exception.getMessage());
            }
        }
        
    }

    @Override
    public long getFileSize(String filePath)
    {
        long size = 0;

        try
        {
            FSDataInputStream fileChannel = updateCache(filePath);
            size=fileChannel.available();
        }
        catch(IOException e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
        return size;
    }

    @Override
    public byte[] readByteArray(String filePath, int length)
    {
        FSDataInputStream fileChannel = updateCache(filePath);
        byte[] byteBffer = read(fileChannel, length);
        return byteBffer;
    }

    @Override
    public ByteBuffer readByteBuffer(String filePath, long offset, int length)
    {
        FSDataInputStream fileChannel = updateCache(filePath);
        byte[] byteBffer = read(fileChannel, length,offset);
        ByteBuffer buffer = ByteBuffer.wrap(byteBffer);
        buffer.rewind();
        return buffer;
    }

    @Override
    public long readLong(String filePath, long offset)
    {
        FSDataInputStream fileChannel = updateCache(filePath);
        long i = -1;
        try
        {
            i = fileChannel.readLong();
        }
        catch(IOException e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,e,e.getMessage());
        }
        return i;
    }

    @Override
    public int readInt(String filePath)
    {
        FSDataInputStream fileChannel = updateCache(filePath);
        int i = -1;
        try
        {
            i = fileChannel.readInt();
        }
        catch(IOException e)
        {
//            e.printStackTrace();
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
        return i;
    }

}
