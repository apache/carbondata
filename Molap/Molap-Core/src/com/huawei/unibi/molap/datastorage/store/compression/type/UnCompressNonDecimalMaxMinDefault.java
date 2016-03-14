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
public class UnCompressNonDecimalMaxMinDefault implements UnCompressValue<double[]>
{
    /**
     * doubleCompressor.
     */
    private static Compressor<double[]> doubleCompressor = SnappyCompression.SnappyDoubleCompression.INSTANCE;

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressNonDecimalMaxMinDefault.class
            .getName());

    /**
     * value.
     */
    private double[] value;

//    @Override
//    public double getValue(int index, int decimal, double maxValue)
//    {
//        if(value[index] == 0)
//        {
//            return maxValue;
//        }
//        return (maxValue - value[index]) / Math.pow(10, decimal);
//    }

    @Override
    public void setValue(double[] value)
    {
        this.value = (double[])value;
    }

    //TODO SIMIAN
    @Override
    public UnCompressValue getNew()
    {
        try
        {
            return (UnCompressValue)clone();
        }
        catch(CloneNotSupportedException exce)     
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, exce, exce.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressNonDecimalMaxMinByte byte1 = new UnCompressNonDecimalMaxMinByte();
        byte1.setValue(doubleCompressor.compress(value));
        return byte1;
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
        return new UnCompressNonDecimalMaxMinByte();
    } 
    
    //TODO SIMIAN
    @Override
    public UnCompressValue uncompress(DataType dataType)
    {
        return null;
    }

    
    @Override
    public MolapReadDataHolder getValues(int decimal, double maxVal)
    {
        double[] vals = new double[value.length];
        MolapReadDataHolder holder = new MolapReadDataHolder();
        for(int i = 0;i < vals.length;i++)
        {
            vals[i] = value[i] / Math.pow(10, decimal);
            
            if(value[i] == 0)
            {
                vals[i] = maxVal;
            }
            else
            {
                vals[i] =  (maxVal - value[i]) / Math.pow(10, decimal);
            }
            
        }
        holder.setReadableDoubleValues(vals);
        return holder;
    }

}
