package com.huawei.unibi.molap.datastorage.store.impl.data.uncompressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.datastorage.store.impl.CompressedDataMeasureDataWrapper;

/**
 */
public class DoubleArrayDataFileStore extends AbstractDoubleArrayDataStore
{

    /**
     * measuresOffsetsArray.
     */
    private long[] measuresOffsetsArray;

    /**
     * measuresLengthArray.
     */
    private int[] measuresLengthArray;

    /**
     * fileName.
     */
    private String fileName;

    // public DoubleArrayDataFileStore(int size, int elementSize,
    // ValueCompressionModel compressionModel)
    // {
    // super(size, elementSize, compressionModel);
    // }
    //
    // public DoubleArrayDataFileStore(int size, int elementSize,
    // ValueCompressionModel compressionModel,
    // long[] measuresOffsetsArray, String fileName, int[] measuresLengthArray)
    // {
    // super(size, elementSize, compressionModel);
    // this.fileName = fileName;
    // this.measuresLengthArray = measuresLengthArray;
    // this.measuresOffsetsArray = measuresOffsetsArray;
    // datastore=null;
    // }

    public DoubleArrayDataFileStore(ValueCompressionModel compressionModel, long[] measuresOffsetsArray,
            String fileName, int[] measuresLengthArray)
    {
        super(compressionModel);
        this.fileName = fileName;
        this.measuresLengthArray = measuresLengthArray;
        this.measuresOffsetsArray = measuresOffsetsArray;
    }
    
    public DoubleArrayDataFileStore(ValueCompressionModel compressionModel)
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
        UnCompressValue[] unComp = new UnCompressValue[measuresLengthArray.length];
        MolapReadDataHolder[] vals = new MolapReadDataHolder[measuresLengthArray.length];
        if(cols != null)
        {
            for(int i = 0;i < cols.length;i++)
            {
                unComp[cols[i]] = compressionModel.getUnCompressValues()[cols[i]].getNew();
                unComp[cols[i]].setValueInBytes(fileHolder.readByteArray(fileName, measuresOffsetsArray[cols[i]],
                        measuresLengthArray[cols[i]]));
                vals[cols[i]] = unComp[cols[i]].getValues(compressionModel.getDecimal()[cols[i]],
                        compressionModel.getMaxValue()[cols[i]]);
            }
        }
        else
        {
            for(int i = 0;i < unComp.length;i++)
            {

                unComp[i] = compressionModel.getUnCompressValues()[i].getNew();
                unComp[i].setValueInBytes(fileHolder.readByteArray(fileName, measuresOffsetsArray[i],
                        measuresLengthArray[i]));
                vals[i] = unComp[i].getValues(compressionModel.getDecimal()[i], compressionModel.getMaxValue()[i]);
            }
        }
        // return new CompressedDataMeasureDataWrapper(unComp,
        // compressionModel.getDecimal(), compressionModel.getMaxValue());
        return new CompressedDataMeasureDataWrapper(vals);
    }

    @Override
    public MeasureDataWrapper getBackData(int cols, FileHolder fileHolder)
    {
        if(null==compressionModel)
        {
            return null;
        }
        UnCompressValue[] unComp = new UnCompressValue[measuresLengthArray.length];
        MolapReadDataHolder[] vals = new MolapReadDataHolder[measuresLengthArray.length];
        
        unComp[cols] = compressionModel.getUnCompressValues()[cols].getNew();
        unComp[cols].setValueInBytes(fileHolder.readByteArray(fileName, measuresOffsetsArray[cols],
                measuresLengthArray[cols]));
        vals[cols] = unComp[cols].getValues(compressionModel.getDecimal()[cols],
                compressionModel.getMaxValue()[cols]);
        return new CompressedDataMeasureDataWrapper(vals);
    }
}
