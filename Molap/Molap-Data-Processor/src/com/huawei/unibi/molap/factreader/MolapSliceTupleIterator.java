/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 *
 */
package com.huawei.unibi.molap.factreader;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.dataholder.MolapReadDataHolder;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.util.ValueCompressionUtil;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapSliceTupleIterator.java
 * Class Description : Iterator class to iterate over leaf node and return the tuple
 * Class Version 1.0
 */
public class MolapSliceTupleIterator implements
        MolapIterator<MolapSurrogateTupleHolder>
{
    /**
     * mdkey index
     */
    private int keyIndex;

    /**
     * measure index
     */
    private int measureIndex;

    /**
     * key array
     */
    private byte[] keyBackArray;

    /**
     * data holder for measure values 
     */
    private MolapReadDataHolder[] dataHolder;

    /**
     * number of keys in one leaf node
     */
    private int entryCount;

    /**
     * unique value if slice
     */
    private double[] uniqueValue;

    /**
     * mdkey length
     */
    private int mdkeyLength;

    /**
     * hash next
     */
    private boolean hasNext;

    /**
     * leaf node iterator
     */
    private MolapIterator<MolapLeafNodeTuplesHolder> leafNodeIterator;
    
    /**
     * measureCount
     */
    private int measureCount;
    
    /**
     * aggType
     */
    private String [] aggType;

    /**
     * MolapSliceTupleIterator constructor to initialise
     * 
     * @param sliceModel
     *            slice model which will hold the slice information
     * @param mdkeyLength
     *            mdkey length
     * @param measureCount
     *            measure count
     */
    public MolapSliceTupleIterator(String sliceLocation, MolapFile[] factFiles,String tableName, int mdkeyLength,
            int measureCount, String[] aggType)
    {
        this.mdkeyLength = mdkeyLength;
        ValueCompressionModel compressionModel = getCompressionModel(sliceLocation,
                tableName, measureCount);
        this.uniqueValue=compressionModel.getUniqueValue();
        this.leafNodeIterator = new MolapLeafNodeIterator(factFiles,
                measureCount, mdkeyLength,compressionModel);
        this.measureCount=measureCount;
        this.aggType=aggType;
        initialise();
    }
    
    /**
     * below method will be used to initialise
     */
    private void initialise()
    {
        if(this.leafNodeIterator.hasNext())
        {
            MolapLeafNodeTuplesHolder next = this.leafNodeIterator.next();
            this.keyBackArray=next.getMdKey();
            this.dataHolder=next.getMeasureDataWrapper().getValues();
            this.entryCount=next.getEntryCount() * this.mdkeyLength;
            this.hasNext=true;
        }
    }

    /**
     * This method will be used to get the compression model for slice
     * 
     * @param path
     *          slice path
     * @param measureCount
     *          measure count
     * @return compression model
     *
     */
    private ValueCompressionModel getCompressionModel(String sliceLocation,String tableName, int measureCount)
    {
        ValueCompressionModel compressionModel = ValueCompressionUtil
                .getValueCompressionModel(sliceLocation
                        + MolapCommonConstants.MEASURE_METADATA_FILE_NAME
                        + tableName
                        + MolapCommonConstants.MEASUREMETADATA_FILE_EXT,
                        measureCount);
        return compressionModel;
    }
    /**
     * below method will be used to get the key from key back array
     * 
     * @return mdkey
     */
    private byte[] getKey()
    {
        byte[] mdKey = new byte[mdkeyLength];
        System.arraycopy(keyBackArray, keyIndex, mdKey, 0, mdkeyLength);
        this.keyIndex += mdkeyLength;
        return mdKey;
    }

    /**
     * below method will be used to get the measure value from measure data
     * wrapper
     * 
     * @return
     */
    private Object[] getMeasure()
    {
        Object[] measures = new Object[measureCount];
        double values=0;
        for(int i = 0;i < measures.length;i++)
        {
            if(aggType[i].charAt(0)=='n')
            {
                values = dataHolder[i].getReadableDoubleValueByIndex(measureIndex);
                if(values != uniqueValue[i])
                {
                    measures[i] = values;
                }
            }
            else
            {
                measures[i] = dataHolder[i]
                        .getReadableByteArrayValueByIndex(measureIndex);
            }
        }
        measureIndex += 1;
        return measures;
    }

    /**
     * below method will be used to check whether any data is present in the
     * slice
     */
    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    /**
     * below method will be used to get the slice tuple
     */
    @Override
    public MolapSurrogateTupleHolder next()
    {
        MolapSurrogateTupleHolder tuple = new MolapSurrogateTupleHolder();
        tuple.setSurrogateKey(getKey());
        tuple.setMeasures(getMeasure());
        if(this.keyIndex < this.entryCount)
        {
            return tuple;
        }
        else if(!leafNodeIterator.hasNext())
        {
            hasNext = false;
        }
        else
        {
            initialise();
            keyIndex = 0;
            measureIndex = 0;
        }
        return tuple;
    }
}
