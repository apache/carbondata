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
package com.huawei.unibi.molap.autoaggregategraphgenerator.step;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapAutoAGGGraphGeneratorData.java 
 * Class Description : Below class is responsible for holding step data information
 * Version 1.0
 */
public class MolapAutoAGGGraphGeneratorData extends BaseStepData implements StepDataInterface
{
    /**
     * constructor
     */
    public MolapAutoAGGGraphGeneratorData()
    {
        super();
    }
    /**
     * outputRowMeta
     */
    protected RowMetaInterface outputRowMeta;
    
    /**
     * rowMeta
     */
//    protected RowMetaInterface rowMeta;
    
}
