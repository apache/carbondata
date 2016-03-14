/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnFEUo96JbRbYQMzRLD5YYcW4SOo7vQNX3Qt7Oxt6whovzDqvkDZ/M78vLYArcgzNTHnY
XjpxIXd+vWanV9hsZN+IStsMREql0BrjvHHnRE4yIKdtJ7wcrUnd0jijKbrDbA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/
package com.huawei.unibi.molap.groupby.step;

//import org.apache.log4j.Logger;
import java.util.Arrays;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.groupby.MolapGroupBy;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : MolapGroupByStep.java
 * Class Description : MolapGroupByStep
 * Version 1.0
 */
public class MolapGroupByStep extends BaseStep implements StepInterface
{

    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapGroupByStep.class.getName());
    /**
     * molap data writer step data class
     */
    private MolapGroupByStepData data;

    /**
     * molap data writer step meta
     */
    private MolapGroupByStepMeta meta;
    
    /**
     * molapGroupBy
     */
    private MolapGroupBy molapGroupBy;
    
    /**
     * MolapSliceMergerStep Constructor
     * 
     * @param stepMeta
     *          stepMeta
     * @param stepDataInterface
     *          stepDataInterface
     * @param copyNr
     *          copyNr
     * @param transMeta
     *          transMeta
     * @param trans
     *          trans
     *
     */
    public MolapGroupByStep(StepMeta stepMeta,
            StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans)
    {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * Perform the equivalent of processing one row. Typically this means
     * reading a row from input (getRow()) and passing a row to output
     * (putRow)).
     * 
     * @param smi
     *            The steps metadata to work with
     * @param sdi
     *            The steps temporary working data to work with (database
     *            connections, result sets, caches, temporary variables, etc.)
     * @return false if no more rows can be processed or an error occurred.
     * @throws KettleException
     */
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi)
            throws KettleException
    {

        // molap data writer step meta
        meta = (MolapGroupByStepMeta)smi;

        // molap data writer step data
        data = (MolapGroupByStepData)sdi;
        // get row from previous step, blocks when needed!
        Object[] row = getRow();
        
        //outRow
        
        Object[] outRow = null;
        // if row is null then there is no more incoming data
        if(null == row)
        {
            if(null!=this.molapGroupBy)
            {
                outRow = this.molapGroupBy.getLastRow();
                if(null != outRow)
                {
                    putRow(data.getOutputRowMeta(), outRow);
                }
            }
            setOutputDone();
            return false;
        }
        else if(checkAllValuesAreNull(row))
        {
        	int outSize = Integer.parseInt(meta.getOutputRowSize());
        	outRow = new Object[outSize];
        	this.data.setOutputRowMeta((RowMetaInterface)getInputRowMeta().clone());
        	 this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null,
                     this);
        	setStepOutputInterface(outSize);
        	putRow(data.getOutputRowMeta(), outRow);
        	setOutputDone();
        	return false;
        }
        if(first)
        {
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Start group by");
            first = false;
            this.data.setOutputRowMeta((RowMetaInterface)getInputRowMeta().clone());
            this.meta.getFields(data.getOutputRowMeta(), getStepname(), null, null,
                    this);
            this.molapGroupBy= new MolapGroupBy(meta.getAggTypeString(), meta.getColumnName(), meta.getActualColumnName(), row);
            setStepOutputInterface(Integer.parseInt(meta.getOutputRowSize()));
            return true;
        }
        outRow=this.molapGroupBy.add(row);
        if(null!=outRow)
        {
            putRow(data.getOutputRowMeta(), outRow);
        }
        return true;
    }

    private boolean checkAllValuesAreNull(Object[] row)
    {
		for (int i = 0; i < row.length; i++) 
		{
			if(null!=row[i])
			{
				return false;
			}
		}
		return true;
	}

	/**
     * This method will be used for setting the output interface.
     * Output interface is how this step will process the row to next step  
     * 
     * @param dimLens
     *      number of dimensions
     *
     */
    private void setStepOutputInterface(int outRowSize)
    {
        ValueMetaInterface[] out = new ValueMetaInterface[outRowSize];
        int l = 0;
        for(int i = 0;i < outRowSize-1;i++)
        {
            
            out[l] = new ValueMeta(i+"",
                    ValueMetaInterface.TYPE_NUMBER,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL);
            out[l].setStorageMetadata(new ValueMeta(i+"",
                    ValueMetaInterface.TYPE_NUMBER,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL));
            l++;
        }
        out[out.length-1] = new ValueMeta("id",
                ValueMetaInterface.TYPE_BINARY,
                ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
        out[out.length-1].setStorageMetadata(new ValueMeta("id",
                ValueMetaInterface.TYPE_STRING,
                ValueMetaInterface.STORAGE_TYPE_NORMAL));
        out[out.length-1].setLength(256);
        out[out.length-1].setStringEncoding(MolapCommonConstants.BYTE_ENCODING);
        out[out.length-1].getStorageMetadata().setStringEncoding(
                MolapCommonConstants.BYTE_ENCODING);
        data.getOutputRowMeta().setValueMetaList(Arrays.asList(out));
    }
    /**
     * Initialize and do work where other steps need to wait for...
     * 
     * @param smi
     *            The metadata to work with
     * @param sdi
     *            The data to initialize
     * @return step initialize or not
     */
    public boolean init(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (MolapGroupByStepMeta)smi;
        data = (MolapGroupByStepData)sdi;
        return super.init(smi, sdi);
    }

    /**
     * Dispose of this step: close files, empty logs, etc.
     * 
     * @param smi
     *            The metadata to work with
     * @param sdi
     *            The data to dispose of
     */
    public void dispose(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (MolapGroupByStepMeta)smi;
        data = (MolapGroupByStepData)sdi;
        super.dispose(smi, sdi);
        meta= null;
        data= null;
        this.molapGroupBy= null;
    }

}
