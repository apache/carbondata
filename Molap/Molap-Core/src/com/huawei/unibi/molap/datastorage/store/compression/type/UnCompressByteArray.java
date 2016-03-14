/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 *
 */
package com.huawei.unibi.molap.datastorage.store.compression.type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : UnCompressByteArray.java
 * Class Description : below class will be used to holder byte array value  
 * Class Version 1.0
 */
public class UnCompressByteArray implements UnCompressValue<byte[]>
{
    /**
     * byteCompressor.
     */
    private static Compressor<byte[]> byteCompressor = SnappyCompression.SnappyByteCompression.INSTANCE;

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressMaxMinByte.class.getName());
    /**
     * value.
     */
    private byte[] value;

    @Override
    public void setValue(byte[] value)
    {
        this.value = value;
        
    }

    @Override
    public void setValueInBytes(byte[] value)
    {
        this.value = value;
        
    }

    @Override
    public UnCompressValue<byte[]> getNew()
    {
        try
        {
            return (UnCompressValue)clone();
        }
        catch(CloneNotSupportedException e)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e, e.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressByteArray byte1 = new UnCompressByteArray();
        byte1.setValue(byteCompressor.compress(value));
        return byte1;
    }

    @Override
    public UnCompressValue uncompress(DataType dataType)
    {
        UnCompressValue byte1 = new UnCompressByteArray();
        byte1.setValue(byteCompressor.unCompress(value));
        return byte1;
    }

    @Override
    public byte[] getBackArrayData()
    {
        return this.value;
    }

    @Override
    public UnCompressValue getCompressorObject()
    {
        return new UnCompressByteArray();
    }

    @Override
    public MolapReadDataHolder getValues(int decimal, double maxValue)
    {
        List<byte[]> valsList = new ArrayList<byte[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.rewind();
        int length = 0;
        byte[] actualValue = null;
      //CHECKSTYLE:OFF    Approval No:Approval-367
        while(buffer.hasRemaining())
        {//CHECKSTYLE:ON
            length = buffer.getInt();
            actualValue = new byte[length];
            buffer.get(actualValue);
            valsList.add(actualValue);
            
        }
        MolapReadDataHolder holder = new MolapReadDataHolder();
        holder.setReadableByteValues(valsList.toArray(new byte[valsList.size()][]));
        return holder;
    }

}
