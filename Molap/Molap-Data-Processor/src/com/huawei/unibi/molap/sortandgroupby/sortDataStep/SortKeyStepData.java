/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/iB6RtjeNhzoLQrFu010RnT+PKAsRfbe2fa40BlM3ZF1vF7SknrVEMnuVhPczAz9wqbk
5pjSUq4SXi73U/+ANdeKqL5ytGgSQS8R1W56NQPv12fp0nO4o9fKWTzQY+nCEw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.sortandgroupby.sortDataStep;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 25-Aug-2015
 * FileName 		: SortKeyStepData.java
 * Description 		: Kettle step data to sort data
 * Class Version 	: 1.0
 */
public class SortKeyStepData extends BaseStepData implements StepDataInterface
{
    
    /**
     * outputRowMeta
     */
    private RowMetaInterface outputRowMeta;
    
    /**
     * rowMeta
     */
    private RowMetaInterface rowMeta;

    public RowMetaInterface getOutputRowMeta()
    {
        return outputRowMeta;
    }

    public void setOutputRowMeta(RowMetaInterface outputRowMeta)
    {
        this.outputRowMeta = outputRowMeta;
    }

    public RowMetaInterface getRowMeta()
    {
        return rowMeta;
    }

    public void setRowMeta(RowMetaInterface rowMeta)
    {
        this.rowMeta = rowMeta;
    }
}