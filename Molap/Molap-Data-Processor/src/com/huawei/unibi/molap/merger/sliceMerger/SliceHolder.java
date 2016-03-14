package com.huawei.unibi.molap.merger.sliceMerger;

//import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
//import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
//import com.huawei.unibi.molap.merger.Util.SliceModel;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :r70299
 * Created Date :Sep 3, 2013
 * FileName : NonTimeBasedMerger.java
 * Class Description : 
 * Version 1.0
 */
public class SliceHolder
{
   /* private SliceModel sliceModel;

    private int keyIndex;

    private int measureIndex;

    private byte[] keyBackArray;
    
    *//**
     * mdkeyLength
     *//*
    private int mdkeyLength;

    private byte[] mdKey;

    private MeasureDataWrapper backData;

    private int entryCount;

    private double[] sliceUniqueValue;
    
     SliceHolder(SliceModel sliceModel, int keyIndex, int measureIndex,int mdkeyLength)
    {
        this.sliceModel = sliceModel;
        this.keyIndex = keyIndex;
        this.measureIndex = measureIndex;
        this.keyBackArray = this.sliceModel.getKeyData();
        this.mdKey = new byte[mdkeyLength];
        this.backData = this.sliceModel.getMeasureData();
        entryCount = this.sliceModel.getEnrtyCount() * mdkeyLength;
        sliceUniqueValue=this.sliceModel.getUniqueValue();
        this.mdkeyLength=mdkeyLength;
    }

    public byte[] getMdKey()
    {
        return mdKey;
    }

    public byte[] getKey()
    {
        System.arraycopy(keyBackArray, keyIndex, mdKey, 0, mdkeyLength);
        return mdKey;
    }

    public MolapReadDataHolder[] getMeasure()
    {
        return backData.getValues();
    }

    public boolean hasNext()
    {
        if(this.keyIndex + mdkeyLength < this.entryCount)
        {
            keyIndex += mdkeyLength;
            measureIndex = measureIndex + 1;
        }
        else if(!sliceModel.hasRemaning())
        {
            sliceModel.close();
            return false;
        }
        else
        {
            this.keyBackArray = this.sliceModel.getKeyData();
            this.entryCount = this.sliceModel.getEnrtyCount() * mdkeyLength;
            this.backData = this.sliceModel.getMeasureData();
            keyIndex = 0;
            measureIndex = 0;
        }
        return true;
    }
    public int getMeasureIndexToRead()
    {
        return measureIndex;
    }
    
    *//**
	 * @return the sliceUniqueValue
	 *//*
	public double[] getSliceUniqueValue() {
		return sliceUniqueValue;
	}*/
}