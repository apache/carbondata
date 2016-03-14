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

import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapLeafNodeTuplesHolder.java
 * Class Description : holder class to hold leaf
 * Class Version 1.0
 */
public class MolapLeafNodeTuplesHolder
{
    /**
     * mdkey
     */
    private byte[] mdkey;
    
    /**
     * measureDataWrapper
     */
    private MeasureDataWrapper measureDataWrapper;
    
    /**
     * entryCount
     */
    private int entryCount;
    
    /**
     * @return the mdkey
     */
    public byte[] getMdKey()
    {
        return mdkey;
    }

    /**
     * @param mdkey the mdkey to set
     */
    public void setMdKey(byte[] mdkey)
    {
        this.mdkey = mdkey;
    }

    /**
     * @return the measureDataWrapper
     */
    public MeasureDataWrapper getMeasureDataWrapper()
    {
        return measureDataWrapper;
    }

    /**
     * @param measureDataWrapper the measureDataWrapper to set
     */
    public void setMeasureDataWrapper(MeasureDataWrapper measureDataWrapper)
    {
        this.measureDataWrapper = measureDataWrapper;
    }

    /**
     * @return the entryCount
     */
    public int getEntryCount()
    {
        return entryCount;
    }

    /**
     * @param entryCount the entryCount to set
     */
    public void setEntryCount(int entryCount)
    {
        this.entryCount = entryCount;
    }
}
