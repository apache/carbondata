/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */

package com.huawei.unibi.molap.datastorage.store.impl.data.compressed;

import com.huawei.unibi.molap.datastorage.store.NodeMeasureDataStore;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapWriteDataHolder;
import com.huawei.unibi.molap.util.ValueCompressionUtil;

/**
 * 
 * Project Name NSE V3R7C00 Module Name : Molap Core Author K00900841 Created
 * Date :21-May-2013 7:06:55 PM FileName :
 * AbstractHeavyCompressedDoubleArrayDataStore.java Class Description :
 * 
 * Version 1.0
 */
public abstract class AbstractHeavyCompressedDoubleArrayDataStore implements NodeMeasureDataStore //NodeMeasureDataStore<double[]>
{
//    /**
//     * totalSize.
//     */
//    private final int totalSize;

    /**
     * values.
     */
    protected UnCompressValue[] values;

//    /**
//     * datastore.
//     */
//    protected Object[] datastore;

    /**
     * compressionModel.
     */
    protected ValueCompressionModel compressionModel;
    
    /**
     * type
     */
    private char[] type;
    
//    /**
//     * elementSize.
//     */
//    private int elementSize;
    
//    /**
//     * AbstractHeavyCompressedDoubleArrayDataStore constructor.
//     * @param totalSize
//     * @param elementSize
//     * @param compressionModel
//     */
//    public AbstractHeavyCompressedDoubleArrayDataStore(int totalSize, int elementSize, ValueCompressionModel compressionModel)
//    {
//        this(totalSize, elementSize, compressionModel, true);
//    }
//    
//    /**
//     * AbstractHeavyCompressedDoubleArrayDataStore constructor.
//     * @param totalSize
//     * @param elementSize
//     * @param compressionModel
//     */
//    public AbstractHeavyCompressedDoubleArrayDataStore(int totalSize, int elementSize, ValueCompressionModel compressionModel,boolean createDataStore)
//    {
//        this.totalSize = totalSize;
//        if(createDataStore)
//        {
//            datastore = new Object[elementSize];
//        }
//        this.compressionModel = compressionModel;
//        this.elementSize = elementSize;
//        values = new UnCompressValue[compressionModel.getUnCompressValues().length];
//    }
    
    /**
     * AbstractHeavyCompressedDoubleArrayDataStore constructor.
     * @param totalSize
     * @param elementSize
     * @param compressionModel
     */
    public AbstractHeavyCompressedDoubleArrayDataStore(ValueCompressionModel compressionModel)
    {
        this.compressionModel = compressionModel;
        if(null!=compressionModel)
        {
            this.type=compressionModel.getType();
            values = new UnCompressValue[compressionModel.getUnCompressValues().length];
        }
    }
    
//    /**
//     * AbstractHeavyCompressedDoubleArrayDataStore overloaded constructor.
//     * @param totalSize
//     * @param elementSize
//     */
//    public AbstractHeavyCompressedDoubleArrayDataStore(int totalSize, int elementSize)
//    {
//        this.totalSize = totalSize;
//        datastore = new Object[elementSize];
//        this.elementSize = elementSize;
//    }
//
//    @Override
//    public void put(int index, double[] value)
//    {
//        for(int i = 0;i < datastore.length;i++)
//        {
//            double[] copy = (double[])datastore[i];
//            if(copy == null)
//            {
//                copy = new double[totalSize];
//                datastore[i] = copy;
//            }//CHECKSTYLE:OFF    Approval No:Approval-357
//            copy[index] = value[i];
//        }//CHECKSTYLE:ON
//    }

//    @Override
//    public byte[][] getWritableMeasureDataArray()
//    {
//        for(int i = 0;i < compressionModel.getUnCompressValues().length;i++)
//        {
//            values[i] = compressionModel.getUnCompressValues()[i].getNew();
//            values[i].setValue(ValueCompressionUtil.getCompressedValues(compressionModel.getCompType()[i],
//                    ((double[])datastore[i]), compressionModel.getChangedDataType()[i],
//                    compressionModel.getMaxValue()[i], compressionModel.getDecimal()[i]));
//            values[i]=values[i].compress();
//        }
////
////        int totalSize = MolapCommonConstants.INT_SIZE_IN_BYTE * values.length;
////        for(int i = 0;i < values.length;i++)
////        {
////            totalSize += values[i].getBackArrayData().length;
////        }
////        ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
////        for(int i = 0;i < values.length;i++)
////        {
////            byteBuffer.putInt(values[i].getBackArrayData().length);
////            byteBuffer.put(values[i].getBackArrayData());
////        }
//        
//        byte[][] resturnValue = new byte[values.length][];
//        for(int i = 0;i < values.length;i++)
//        {
//            resturnValue[i]=values[i].getBackArrayData();
//        }
//        return resturnValue;
//    }
    
    @Override
    public byte[][] getWritableMeasureDataArray(MolapWriteDataHolder[] dataHolder)
    {
        for(int i = 0;i < compressionModel.getUnCompressValues().length;i++)
        {
            values[i] = compressionModel.getUnCompressValues()[i].getNew();
            if(type[i]!='c')
            {
                values[i].setValue(ValueCompressionUtil.getCompressedValues(compressionModel.getCompType()[i],
                        dataHolder[i].getWritableDoubleValues(), compressionModel.getChangedDataType()[i],
                        compressionModel.getMaxValue()[i], compressionModel.getDecimal()[i]));
            }
            else
            {
                values[i].setValue(dataHolder[i].getWritableByteArrayValues());
            }
            values[i]=values[i].compress();
        }
        byte[][] resturnValue = new byte[values.length][];
        for(int i = 0;i < values.length;i++)
        {
            resturnValue[i]=values[i].getBackArrayData();
        }
        return resturnValue;
    }

    @Override
    public short getLength()
    {
        return values != null ? (short)values.length : 0;
    }
    

//    /**
//     * This method will clear the store and create the new empty store
//     * 
//     */
//    public void clear()
//    {
//        datastore = new Object[elementSize];
//    }
}
