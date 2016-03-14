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
package com.huawei.unibi.molap.merger.step.autoaggregate;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : MolapSliceMergerStepData.java
 * Class Description : 
 * Version 1.0
 */
public class MolapAutoAggregateSliceMergerData extends BaseStepData implements
        StepDataInterface
{
    /**
     * outputRowMeta
     */
    private RowMetaInterface outputRowMeta;

    public RowMetaInterface getOutputRowMeta()
    {
        return outputRowMeta;
    }

    public void setOutputRowMeta(RowMetaInterface outputRowMeta)
    {
        this.outputRowMeta = outputRowMeta;
    }

    /**
     * MolapSliceMergerStepData
     * 
     */
    public MolapAutoAggregateSliceMergerData()
    {
        super();
    }
}
