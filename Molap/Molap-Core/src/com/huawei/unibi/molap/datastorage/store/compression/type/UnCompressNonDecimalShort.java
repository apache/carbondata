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
public class UnCompressNonDecimalShort implements UnCompressValue<short[]>
{
    /**
     * shortCompressor.
     */
    private static Compressor<short[]> shortCompressor = SnappyCompression.SnappyShortCompression.INSTANCE;

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressNonDecimalShort.class.getName());



//    @Override
//    public double getValue(int index, int decimal, double maxValue)
//    {
//        return value[index] / Math.pow(10, decimal);
//    }

    @Override
    public void setValue(short[] value)
    {
        this.value = value;

    }
    
    //TODO SIMIAN

    /**
     * value.
     */
    private short[] value;
    
    @Override
    public byte[] getBackArrayData()
    {
        return ValueCompressionUtil.convertToBytes(value);
    }

    @Override
    public UnCompressValue getNew()
    {
        try
        {
            return (UnCompressValue)clone();
        }
        catch(CloneNotSupportedException exception1) 
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, exception1, exception1.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressNonDecimalByte byte1 = new UnCompressNonDecimalByte();
        byte1.setValue(shortCompressor.compress(value));
        return byte1;
    }

    @Override
    public UnCompressValue uncompress(DataType dataType)
    {
        return null;
    }

  

    @Override
    public void setValueInBytes(byte[] value)
    {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        this.value = ValueCompressionUtil.convertToShortArray(buffer, value.length);
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
        MolapReadDataHolder dataHolder = new MolapReadDataHolder();
        double[] vals = new double[value.length];
        for(int i = 0;i < vals.length;i++)
        {
            vals[i] = value[i] / Math.pow(10, decimal);
        }
        dataHolder.setReadableDoubleValues(vals);
        return dataHolder;
    }

}
