/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/gpbaJgUcbLgeXp5DxMtQNtAeFuIcLJm7J7+CDz514I5jdSjhjsukTHJvp7sI4jwun8k
yqbEbMlFjhUXbGr2Iq5oG4/TtRR19WmNxMeMDndP6h40glKSj7TxAASj0O1cBA==*/
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
package com.huawei.unibi.molap.store;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : MolapDataWriterStepData.java
 * Class Description : 
 * Version 1.0
 */
public class MolapDataWriterStepData extends BaseStepData implements
        StepDataInterface
{
    /**
     * outputRowMeta
     */
    protected RowMetaInterface outputRowMeta;

    /**
     * MolapDataWriterStepData
     * 
     */
    public MolapDataWriterStepData()
    {
        super();
    }

}
