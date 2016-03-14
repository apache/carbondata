/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnEvyNkzcGCWqiGXCLrb+waaYw2FB2tM83Z+dskU5VmnbEw+zfsxWGQPegE8tquIvCOxd
S6CB3AtG3MmMDYYqouryYW9+P8e8iqdnoxD2ZzdNQ4joCiloafKb1nnOQZ6u4A==*/
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
package com.huawei.unibi.molap.groupby.step;

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
public class MolapGroupByStepData extends BaseStepData implements
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
    public MolapGroupByStepData()
    {
        super();
    }
}
