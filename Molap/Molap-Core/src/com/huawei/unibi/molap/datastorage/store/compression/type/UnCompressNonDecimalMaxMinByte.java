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

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.ValueCompressionUtil;
import com.huawei.unibi.molap.util.ValueCompressionUtil.DataType;

/**
 * @author S71955
 */
public class UnCompressNonDecimalMaxMinByte implements UnCompressValue<byte[]>
{
    /**
     * byteCompressor.
     */
    private static Compressor<byte[]> byteCompressor = SnappyCompression.SnappyByteCompression.INSTANCE;

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressNonDecimalMaxMinByte.class
            .getName());

    

//    @Override
//    public double getValue(int index, int decimal, double maxValue)
//    {
//        if(value[index] == 0)
//        {
//            return maxValue;
//        }
//        return (maxValue - value[index]) / Math.pow(10, decimal);
//    }
   
    //TODO SIMIAN
    @Override
    public UnCompressValue getNew() 
    {
        try
        {
            return (UnCompressValue)clone();
        }
        catch(CloneNotSupportedException cloneNotSupportedException)
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, cloneNotSupportedException, cloneNotSupportedException.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressNonDecimalMaxMinByte byte1 = new UnCompressNonDecimalMaxMinByte();
        byte1.setValue(byteCompressor.compress(value));
        return byte1;
    }

    @Override
    public UnCompressValue uncompress(DataType dataType)
    {
        UnCompressValue byte1 = ValueCompressionUtil.unCompressNonDecimalMaxMin(dataType, dataType);
        ValueCompressonHolder.unCompress(dataType, byte1, value);
        return byte1;
    }

    @Override
    public byte[] getBackArrayData()
    {
        return value;
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
    public void setValueInBytes(byte[] value)
    {
        this.value = value;
    }

    
    @Override
    public MolapReadDataHolder getValues(int decimalVal, double maxValue)
    {
        double[] vals = new double[value.length];
        MolapReadDataHolder dataHolder = new MolapReadDataHolder();
        for(int i = 0;i < vals.length;i++)
        {
            vals[i] = value[i] / Math.pow(10, decimalVal);
             
            if(value[i] == 0)
            {
                vals[i] = maxValue;
            }
            else
            {
                vals[i] =  (maxValue - value[i]) / Math.pow(10, decimalVal);
            }
            
        }
        dataHolder.setReadableDoubleValues(vals);
        return dataHolder;
    }
    
    //TODO SIMIAN
    
    /**
     * value.
     */
    private byte[] value;
    
    @Override
    public void setValue(byte[] value)
    {
        this.value = value;
    }

}
