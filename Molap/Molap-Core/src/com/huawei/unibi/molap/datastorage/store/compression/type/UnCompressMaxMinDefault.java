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
public class UnCompressMaxMinDefault implements UnCompressValue<double[]>
{

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressMaxMinDefault.class.getName());

    /**
     * doubleCompressor.
     */
    private static Compressor<double[]> doubleCompressor = SnappyCompression.SnappyDoubleCompression.INSTANCE;

 

//    @Override
//    public double getValue(int index, int decimal, double maxValue)
//    {
//        if(value[index] == 0)
//        {
//            return maxValue;
//        }
//        return maxValue - value[index];
//    }

    @Override
    public void setValue(double[] value)
    {
        this.value = (double[])value;

    }
    //TODO SIMIAN
    /**
     * value.
     */
    private double[] value;

    //TODO SIMIAN
    @Override
    public UnCompressValue getNew()
    {
        try
        {
            return (UnCompressValue)clone();
        }
        catch(CloneNotSupportedException ex5)   
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, ex5, ex5.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressMaxMinByte byte1 = new UnCompressMaxMinByte();
        byte1.setValue(doubleCompressor.compress(value));
        return byte1;
    }

    @Override
    public UnCompressValue uncompress(DataType dataType)
    {
        return null;
    }

    @Override
    public byte[] getBackArrayData()
    {
        return ValueCompressionUtil.convertToBytes(value);
    }

    @Override
    public void setValueInBytes(byte[] value)
    {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        this.value = ValueCompressionUtil.convertToDoubleArray(buffer, value.length);
    }

    /**
     * 
     * @see com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue#getCompressorObject()
     * 
     */
    @Override
    public UnCompressValue getCompressorObject()
    {
        return new UnCompressMaxMinByte();
    }
 
    //TODO SIMIAN
    @Override
    public MolapReadDataHolder getValues(int decimal, double maxValue)
    {
        double[] vals = new double[value.length];
        MolapReadDataHolder dataHolderInfoObj = new MolapReadDataHolder();
        for(int i = 0;i < vals.length;i++)
        {
            if(value[i] == 0)
            {
                vals[i] = maxValue;
            }
            else
            {
                vals[i] =  maxValue - value[i];
            }
             
        }
        dataHolderInfoObj.setReadableDoubleValues(vals);
        return dataHolderInfoObj;
    }

}
