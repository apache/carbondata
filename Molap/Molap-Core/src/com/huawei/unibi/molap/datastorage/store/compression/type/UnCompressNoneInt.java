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
public class UnCompressNoneInt implements UnCompressValue<int[]>
{
    /**
     * intCompressor.
     */
    private static Compressor<int[]> intCompressor = SnappyCompression.SnappyIntCompression.INSTANCE;

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressNoneInt.class.getName());

    /**
     * value.
     */
    private int[] value;

    @Override
    public void setValue(int[] value)
    {
        this.value = value;

    }

    @Override
    public byte[] getBackArrayData()
    {
        return ValueCompressionUtil.convertToBytes(value);
    }


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
        catch(CloneNotSupportedException exc) 
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, exc, exc.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressNoneByte byte1 = new UnCompressNoneByte();
        byte1.setValue(intCompressor.compress(value));

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
        this.value = ValueCompressionUtil.convertToIntArray(buffer, value.length);

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
    
    //TODO SIMIAN
    @Override
    public MolapReadDataHolder getValues(int decimal, double maxValue)
    {
        MolapReadDataHolder dataHolderInfoObj = new MolapReadDataHolder();  
        double[] vals = new double[value.length];
        for(int i = 0;i < vals.length;i++)
        {
            vals[i] = value[i];
        }
        
        dataHolderInfoObj.setReadableDoubleValues(vals);
        return dataHolderInfoObj;
    }

}
