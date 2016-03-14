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


/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapSliceTupleIterator.java
 * Class Description : holder class to hold the tuple
 * Class Version 1.0
 */
public class MolapSurrogateTupleHolder
{
    /**
     * surrogateKey
     */
    private byte[] mdKey;
    
    /**
     * measures
     */
    private Object[] measures;

    /**
     * @return the surrogateKey
     */
    public byte[] getMdKey()
    {
        return mdKey;
    }

    /**
     * @param surrogateKey the surrogateKey to set
     */
    public void setSurrogateKey(byte[] mdKey)
    {
        this.mdKey = mdKey;
    }

    /**
     * @return the measure
     */
    public Object[] getMeasures()
    {
        return measures;
    }

    /**
     * @param measure the measure to set
     */
    public void setMeasures(Object[] measures)
    {
        this.measures = measures;
    }

}
