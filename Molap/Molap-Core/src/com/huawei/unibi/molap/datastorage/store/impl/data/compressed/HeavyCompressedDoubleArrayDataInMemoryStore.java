package com.huawei.unibi.molap.datastorage.store.impl.data.compressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.datastorage.store.impl.CompressedDataMeasureDataWrapper;

/**
 * HeavyCompressedDoubleArrayDataInMemoryStore.
 * @author S71955
 *
 */
public class HeavyCompressedDoubleArrayDataInMemoryStore extends AbstractHeavyCompressedDoubleArrayDataStore
{

//    public HeavyCompressedDoubleArrayDataInMemoryStore(int size, int elementSize, ValueCompressionModel valueCompressionModel)
//    {
//        super(size, elementSize, valueCompressionModel);
//    }
//
//    public HeavyCompressedDoubleArrayDataInMemoryStore(int size, int elementSize)
//    {
//        super(size, elementSize);
//    }
//    public HeavyCompressedDoubleArrayDataInMemoryStore(int size, int elementSize,
//            ValueCompressionModel compressionModel, long[] measuresOffsetsArray, int[] measuresLengthArray,
//            String fileName, FileHolder fileHolder)
//    {
//        super(size, elementSize, compressionModel);
//        for(int i = 0;i < measuresLengthArray.length;i++)
//        {
//            values[i] = compressionModel.getUnCompressValues()[i].getCompressorObject();
//            values[i].setValue(fileHolder.readByteArray(fileName, measuresOffsetsArray[i], measuresLengthArray[i]));
//        }
//    }
    
    public HeavyCompressedDoubleArrayDataInMemoryStore(ValueCompressionModel compressionModel, long[] measuresOffsetsArray, int[] measuresLengthArray,
            String fileName, FileHolder fileHolder)
    {
        super(compressionModel);
        for(int i = 0;i < measuresLengthArray.length;i++)
        {
            values[i] = compressionModel.getUnCompressValues()[i].getCompressorObject();
            values[i].setValue(fileHolder.readByteArray(fileName, measuresOffsetsArray[i], measuresLengthArray[i]));
        }
    }
    
    public HeavyCompressedDoubleArrayDataInMemoryStore(ValueCompressionModel compressionModel)
    {
        super(compressionModel);
    }


    @Override
    public MeasureDataWrapper getBackData(int[] cols, FileHolder fileHolder)
    {
        if(null==compressionModel)
        {
            return null;
        }
//        UnCompressValue[] unComp = new UnCompressValue[values.length];
        MolapReadDataHolder[] vals = new MolapReadDataHolder[values.length];
        if(cols != null)
        {
            for(int i = 0;i < cols.length;i++)
            {
                vals[cols[i]] = values[cols[i]].uncompress(compressionModel.getChangedDataType()[cols[i]]).getValues(compressionModel.getDecimal()[cols[i]], compressionModel.getMaxValue()[cols[i]]);
            }
        }
        else
        {
            for(int i = 0;i < vals.length;i++)
            {

                vals[i] = values[i].uncompress(compressionModel.getChangedDataType()[i]).getValues(compressionModel.getDecimal()[i], compressionModel.getMaxValue()[i]);
            }
        }
//        return new CompressedDataMeasureDataWrapper(unComp, compressionModel.getDecimal(), compressionModel.getMaxValue());
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
        vals[cols] = values[cols].uncompress(compressionModel.getChangedDataType()[cols]).getValues(
                compressionModel.getDecimal()[cols], compressionModel.getMaxValue()[cols]);
        return new CompressedDataMeasureDataWrapper(vals);
    }
}
