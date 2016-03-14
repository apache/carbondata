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
public class UnCompressNonDecimalDefault implements UnCompressValue<double[]>
{
    /**
     * doubleCompressor.
     */
    private static Compressor<double[]> doubleCompressor = SnappyCompression.SnappyDoubleCompression.INSTANCE;

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(UnCompressNonDecimalDefault.class
            .getName());

   

//    @Override
//    public double getValue(int index, int decimal, double maxValue)
//    {
//        return value[index] / Math.pow(10, decimal);
//    }

   

    @Override
    public UnCompressValue getNew()
    {
        try
        {
            return (UnCompressValue)clone();
        }
        catch(CloneNotSupportedException cnse1) 
        {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, cnse1, cnse1.getMessage());
        }
        return null;
    }

    @Override
    public UnCompressValue compress()
    {
        UnCompressNonDecimalByte byte1 = new UnCompressNonDecimalByte();
        byte1.setValue(doubleCompressor.compress(value));
        return byte1;
    }

    @Override
    public UnCompressValue uncompress(DataType dataType)
    {
        return null;
    }

    //TODO SIMIAN
    /**
     * value.
     */
    private double[] value;
    
    @Override
    public void setValue(double[] value)
    {
        this.value = value;

    }
    
    @Override
    public void setValueInBytes(byte[] value)
    {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        this.value = ValueCompressionUtil.convertToDoubleArray(buffer, value.length);
    }


    @Override
    public byte[] getBackArrayData()
    {
        return ValueCompressionUtil.convertToBytes(value);
    }

   
    @Override
    public UnCompressValue getCompressorObject()
    {
        return new UnCompressNonDecimalByte();
    }
    
    @Override
    public MolapReadDataHolder getValues(int decimal, double maxValue)
    {
        double[] dblVals = new double[value.length]; 
        for(int i = 0;i < dblVals.length;i++)
        {
            dblVals[i] = value[i] / Math.pow(10, decimal);
        }
        MolapReadDataHolder dataHolder= new  MolapReadDataHolder();
        dataHolder.setReadableDoubleValues(dblVals);
        return dataHolder;
    }

}
