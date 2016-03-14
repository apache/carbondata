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
public class UnCompressNoneLong implements UnCompressValue<long[]>
{
    /**
     * longCompressor.
     */
    private static Compressor<long[]> longCompressor = SnappyCompression.SnappyLongCompression.INSTANCE;

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressNoneLong.class.getName());

    /**
     * value.
     */
    private long[] value;

    @Override
    public void setValue(long[] value)
    {
        this.value = value;

    }
//
//    @Override
//    public double getValue(int index, int decimal, double maxValue)
//    {
//        return value[index];
//    }

    //TODO SIMIAN
    @Override
    public UnCompressValue getNew()
    {
        try
        {
            return (UnCompressValue)clone();
        }
        catch(CloneNotSupportedException clnNotSupportedExc)  
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, clnNotSupportedExc, clnNotSupportedExc.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressNoneByte byte1 = new UnCompressNoneByte();
        byte1.setValue(longCompressor.compress(value));
        return byte1;

    }

    @Override
    public UnCompressValue uncompress(DataType dType)
    {
        return null;
    }

    @Override
    public byte[] getBackArrayData()
    {
        return ValueCompressionUtil.convertToBytes(value);
    }

    @Override
    public void setValueInBytes(byte[] byteValue) 
    {
        ByteBuffer buffer = ByteBuffer.wrap(byteValue);
        this.value = ValueCompressionUtil.convertToLongArray(buffer, byteValue.length);
    }

    /**
     * 
     * @see com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue#getCompressorObject()
     * 
     */
    @Override
    public UnCompressValue getCompressorObject()
    {
        return new UnCompressNoneByte();
    }
    
    @Override
    public MolapReadDataHolder getValues(int decimal, double maxValue)
    {
        MolapReadDataHolder dataHolder = new MolapReadDataHolder();
        double[] vals = new double[value.length];
//        System.arraycopy(value, 0, vals, 0, value.length);
        for(int i = 0;i < vals.length;i++)
        {
            vals[i] = value[i];
        }
        dataHolder.setReadableDoubleValues(vals);
        return dataHolder;
    }

}
