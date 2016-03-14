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
package com.huawei.unibi.molap.datastorage.store.compression.type;

import java.nio.ByteBuffer;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.ValueCompressionUtil;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

/**
 * @author S71955
 */
public class UnCompressNonDecimalInt implements UnCompressValue<int[]>
{
    /**
     * intCompressor.
     */
    private static Compressor<int[]> intCompressor = SnappyCompression.SnappyIntCompression.INSTANCE;

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressNonDecimalInt.class.getName());

    /**
     * value.
     */
    private int[] value;

//    @Override
//    public double getValue(int index, int decimal, double maxValue)
//    {
//        return value[index] / Math.pow(10, decimal);
//    }

    @Override
    public void setValue(int[] value)
    {
        this.value = (int[])value;

    }

    //TODO SIMIAN
    @Override
    public UnCompressValue getNew()
    {
        try
        {
            return (UnCompressValue)clone();
        }
        catch(CloneNotSupportedException csne1)  
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, csne1, csne1.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressNonDecimalByte byte1 = new UnCompressNonDecimalByte();
        byte1.setValue(intCompressor.compress(value));
        return byte1;
    }

    @Override
    public byte[] getBackArrayData()
    {
        return ValueCompressionUtil.convertToBytes(value);
    }
    
    //TODO SIMIAN
    @Override
    public void setValueInBytes(byte[] bytesArr)    
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytesArr);
        this.value = ValueCompressionUtil.convertToIntArray(buffer, bytesArr.length);
    }

    /**
     * 
     * @see com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue#getCompressorObject()
     * 
     */
    @Override
    public UnCompressValue getCompressorObject()
    {
        return new UnCompressNonDecimalByte();
    }
    
    @Override
    public MolapReadDataHolder getValues(int decimal, double maxValue)
    {
        double[] vals = new double[value.length];
        for(int k = 0;k < vals.length;k++)  
        {
            vals[k] = value[k] / Math.pow(10, decimal);
        }
        MolapReadDataHolder dataHolder = new MolapReadDataHolder();
        dataHolder.setReadableDoubleValues(vals);
        return dataHolder;
    }
    
    //TODO SIMIAN    
    @Override
    public UnCompressValue uncompress(DataType dataType)
    {
        return null;
    }

}
