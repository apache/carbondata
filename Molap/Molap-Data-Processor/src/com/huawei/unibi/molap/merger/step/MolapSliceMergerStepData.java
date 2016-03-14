/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwe/owl+XpObKvwejIomJrN10iZBX17jBC5vj/zP
61+XacHUYdMzzkqKbPQ2YeC1DWrFgWdhbbUKWBNyJsZvquj3I9M7VZlezsNnDZIuX92ebA7G
DqvWAlrUylrsyD3VELqXwF76CGW+xfuMTb4WG1j0MALtK0/c8rtzeBMZ7BQgGQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2011
 * =====================================
 *
 */
package com.huawei.unibi.molap.merger.step;

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
public class MolapSliceMergerStepData extends BaseStepData implements
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
    public MolapSliceMergerStepData()
    {
        super();
    }
}
