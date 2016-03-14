package com.huawei.unibi.molap.datastorage.store.impl.data.uncompressed;

import com.huawei.unibi.molap.datastorage.store.NodeMeasureDataStore;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapWriteDataHolder;
import com.huawei.unibi.molap.util.ValueCompressionUtil;


/**
 * 
 */
public abstract class  AbstractDoubleArrayDataStore implements NodeMeasureDataStore //NodeMeasureDataStore<double[]>
{

//    /**
//     * size.
//     */
//    protected final int size;

    /**
     * values.
     */
    protected UnCompressValue[] values;
    
    private char[] type;
//    /**
//     * datastore.
//     */
//    protected Object[] datastore;
    
    /**
     * compressionModel.
     */
    protected ValueCompressionModel compressionModel;
    
    public AbstractDoubleArrayDataStore(ValueCompressionModel compressionModel)
    {
        this.compressionModel = compressionModel;
        if(null!=compressionModel)
        {
            values = new UnCompressValue[compressionModel.getUnCompressValues().length];
            type = compressionModel.getType();
        }
    }
    
//    /**
//     * elementSize.
//     */
//    private int elementSize;
//    
//    /**
//     * AbstractDoubleArrayDataStore constructor.
//     * @param size
//     * @param elementSize
//     * @param compressionModel
//     */
//    public AbstractDoubleArrayDataStore(int size, int elementSize, ValueCompressionModel compressionModel)
//    {
//        this.size = size;
//        datastore = new Object[elementSize];
//        this.compressionModel = compressionModel;
//        this.elementSize = elementSize;
//        values = new UnCompressValue[compressionModel.getUnCompressValues().length];
//    }
//    /** 
//     * overloaded constructor.
//     * @param size
//     * @param elementSize
//     */
//    public AbstractDoubleArrayDataStore(int size, int elementSize)
//    {
//        this.size = size;
//        datastore = new Object[elementSize];
//        this.elementSize = elementSize;
//    }
    
//    @Override
//    public void put(int index, double []value)
//    {
//        for(int i = 0;i < datastore.length;i++)
//        {
//            double[] copy = (double[])datastore[i];
//            if(Arrays.equals(null, copy))
//            {
//                copy = new double[size];
//            }
//            copy[index] = value[i];
//            datastore[i] = copy;
//        }
//    }

    @Override
    public byte[][]getWritableMeasureDataArray(MolapWriteDataHolder[] dataHolder)
    {
        values = new UnCompressValue[compressionModel.getUnCompressValues().length];
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
        }
        
        byte[][] resturnValue = new byte[values.length][];
//        int totalSize=4*values.length;
//        byte[][] tempValues = new byte[values.length][];
//        for(int i = 0;i < values.length;i++)
//        {
//            tempValues[i] = values[i].getBackArrayData();
//            totalSize+=tempValues[i].length;
//        }
//        ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
//        for(int i = 0;i < values.length;i++)
//        {
//            byteBuffer.putInt(tempValues[i].length);
//            byteBuffer.put(tempValues[i]);
//        }
        
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
//     * This method will clear the store and create the new empty store.
//     * 
//     */
//    public void clear()
//    {
//        datastore = new Object[elementSize];
//    }
}
