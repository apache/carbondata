package com.huawei.unibi.molap.datastorage.store.impl.data.uncompressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.datastorage.store.impl.CompressedDataMeasureDataWrapper;

/**
 * DoubleArrayDataInMemoryStore.
 * 
 * @author S71955
 * 
 */
public class DoubleArrayDataInMemoryStore extends AbstractDoubleArrayDataStore
{

    // /**
    // * DoubleArrayDataInMemoryStore.
    // * @param size
    // * @param elementSize
    // * @param compressionModel
    // */
    // public DoubleArrayDataInMemoryStore(int size, int elementSize,
    // ValueCompressionModel compressionModel)
    // {
    // super(size, elementSize, compressionModel);
    // }
    //
    // /**
    // * DoubleArrayDataInMemoryStore.
    // * @param size
    // * @param elementSize
    // */
    // public DoubleArrayDataInMemoryStore(int size, int elementSize)
    // {
    // super(size, elementSize);
    // }

    // /**
    // * DoubleArrayDataInMemoryStore.
    // * @param size
    // * @param elementSize
    // * @param compressionModel
    // * @param measuresOffsetsArray
    // * @param measuresLengthArray
    // * @param fileName
    // * @param fileHolder
    // */
    // public DoubleArrayDataInMemoryStore(int size, int elementSize,
    // ValueCompressionModel compressionModel,
    // long[] measuresOffsetsArray, int[] measuresLengthArray, String fileName,
    // FileHolder fileHolder)
    // {
    // super(size, elementSize, compressionModel);
    // UnCompressValue[] unCompValues = compressionModel.getUnCompressValues();
    // if(null != unCompValues)
    // {
    // for(int i = 0;i < measuresLengthArray.length;i++)
    // {
    //
    // values[i] = unCompValues[i].getNew();
    // values[i].setValueInBytes(fileHolder.readByteArray(fileName,
    // measuresOffsetsArray[i],
    // measuresLengthArray[i]));
    // }
    // }
    // }

    /**
     * DoubleArrayDataInMemoryStore.
     * 
     * @param size
     * @param elementSize
     * @param compressionModel
     * @param measuresOffsetsArray
     * @param measuresLengthArray
     * @param fileName
     * @param fileHolder
     */
    public DoubleArrayDataInMemoryStore(ValueCompressionModel compressionModel, long[] measuresOffsetsArray,
            int[] measuresLengthArray, String fileName, FileHolder fileHolder)
    {
        super(compressionModel);
        if(null!=compressionModel)
        {
            UnCompressValue[] unCompValues = compressionModel.getUnCompressValues();
            if(null != unCompValues)
            {
                for(int i = 0;i < measuresLengthArray.length;i++)
                {
    
                    values[i] = unCompValues[i].getNew();
                    values[i].setValueInBytes(fileHolder.readByteArray(fileName, measuresOffsetsArray[i],
                            measuresLengthArray[i]));
                }
            }
        }
    }
    
    /**
     * DoubleArrayDataInMemoryStore.
     * 
     * @param size
     * @param elementSize
     * @param compressionModel
     * @param measuresOffsetsArray
     * @param measuresLengthArray
     * @param fileName
     * @param fileHolder
     */
    public DoubleArrayDataInMemoryStore(ValueCompressionModel compressionModel)
    {
        super(compressionModel);
    }

    @Override
    public MeasureDataWrapper getBackData(int[] cols, FileHolder fileHolder)
    {
        if(null == compressionModel)
        {
            return null;
        }
        MolapReadDataHolder[] vals = new MolapReadDataHolder[values.length];
        if(null == cols)
        {
            for(int i = 0;i < vals.length;i++)
            {
                vals[i] = values[i].getValues(compressionModel.getDecimal()[i], compressionModel.getMaxValue()[i]);
            }
        }
        else
        {
            for(int i = 0;i < cols.length;i++)
            {
                vals[cols[i]] = values[cols[i]].getValues(compressionModel.getDecimal()[cols[i]],
                        compressionModel.getMaxValue()[cols[i]]);
            }
        }
        // return new CompressedDataMeasureDataWrapper(values,
        // compressionModel.getDecimal(), compressionModel.getMaxValue());
        return new CompressedDataMeasureDataWrapper(vals);
    }

    @Override
    public MeasureDataWrapper getBackData(int cols, FileHolder fileHolder)
    {
        if(null == compressionModel)
        {
            return null;
        }
        MolapReadDataHolder[] vals = new MolapReadDataHolder[values.length];
        
        vals[cols] = values[cols].getValues(compressionModel.getDecimal()[cols], compressionModel.getMaxValue()[cols]);
        return new CompressedDataMeasureDataWrapper(vals);
    }

}
