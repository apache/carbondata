/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/tpgVHjbAWZ/W4zSZmVBVtTTxS7wAIBGiAcNaBs8bLSe8jeKZ95zh5EV2t/WjlWPj27H
45i1lIoct/dNHs7jPxDrVhOHiZwbM1DDKsxwsK9onBGQnmaEOWwNeQb8MHKR/A==*/
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

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import com.huawei.unibi.molap.util.MolapDataProcessorUtil;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 25-Aug-2015
 * FileName 		: SortKeyStepData.java
 * Description 		: Kettle step meta to sort data
 * Class Version 	: 1.0
 */
public class SortKeyStepMeta extends BaseStepMeta implements StepMetaInterface
{
    /**
     * PKG
     */
    private static final Class<?> PKG = SortKeyStepMeta.class;

    /**
     * tabelName
     */
    private String tabelName;

    /**
     * outputRowSize
     */
    private String outputRowSize;
    
    /**
     * cubeName
     */
    private String cubeName;
    
    /**
     * schemaName
     */
    private String schemaName;
    
    /**
     * Dimension Count
     */
    private String dimensionCount;

    /**
     * ComplexTypes Count
     */
    private String complexDimensionCount;
    
    /**
     * Dimension Count
     */
    private int highCardinalityCount;
    
    /**
     * measureCount
     */
    private String measureCount;
    

	private String factDimLensString;
	
	/**
	 * isUpdateMemberRequest
	 */
	private String updateMemberRequest;
	
	private int currentRestructNumber;

	private String  highCardinalityDims;
    /**
     * set the default value for all the properties
     * 
     */
    @Override
    public void setDefault()
    {
        this.tabelName = "";
        factDimLensString = "";
        outputRowSize = "";
        schemaName = "";
        highCardinalityDims="";
        cubeName = "";
        dimensionCount = "";
        complexDimensionCount = "";
        measureCount = "";
        updateMemberRequest = "";
        currentRestructNumber = -1;
    }

    /**
     * Get the XML that represents the values in this step
     * 
     * @return the XML that represents the metadata in this step
     * @throws KettleException
     *             in case there is a conversion or XML encoding error
     */
    public String getXML()
    {
        StringBuffer retval = new StringBuffer(150);
        retval.append("    ").append(XMLHandler.addTagValue("TableName", this.tabelName));
        retval.append("    ").append(
                XMLHandler.addTagValue("factDimLensString", factDimLensString));
        retval.append("    ").append(XMLHandler.addTagValue("outputRowSize", this.outputRowSize));
        retval.append("    ").append(XMLHandler.addTagValue("cubeName", this.cubeName));
        retval.append("    ").append(XMLHandler.addTagValue("schemaName", this.schemaName));
        retval.append("    ").append(XMLHandler.addTagValue("dimensionCount", this.dimensionCount));
        retval.append("    ").append(XMLHandler.addTagValue("highCardinalityDims", this.highCardinalityDims));
        retval.append("    ").append(XMLHandler.addTagValue("complexDimensionCount", this.complexDimensionCount));
        retval.append("    ").append(XMLHandler.addTagValue("measureCount", this.measureCount));
        retval.append("    ").append(XMLHandler.addTagValue("isUpdateMemberRequest", this.updateMemberRequest));
        retval.append("    ").append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        return retval.toString();
    }

    /**
     * Load the values for this step from an XML Node
     * 
     * @param stepnode
     *            the Node to get the info from
     * @param databases
     *            The available list of databases to reference to
     * @param counters
     *            Counters to reference.
     * @throws KettleXMLException
     *             When an unexpected XML error occurred. (malformed etc.)
     */
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleXMLException
    {
        try
        {
            this.tabelName = XMLHandler.getTagValue(stepnode, "TableName");
            this.outputRowSize = XMLHandler.getTagValue(stepnode, "outputRowSize");
            this.factDimLensString=XMLHandler.getTagValue(stepnode, "factDimLensString");
            this.cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            this.schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            this.dimensionCount = XMLHandler.getTagValue(stepnode, "dimensionCount");
            this.highCardinalityDims=XMLHandler.getTagValue(stepnode, "highCardinalityDims");
            this.complexDimensionCount = XMLHandler.getTagValue(stepnode, "complexDimensionCount");
            this.measureCount = XMLHandler.getTagValue(stepnode, "measureCount");
            this.updateMemberRequest = XMLHandler.getTagValue(stepnode, "isUpdateMemberRequest");
            currentRestructNumber = Integer.parseInt(XMLHandler.getTagValue(stepnode, "currentRestructNumber"));
        }
        catch(Exception e)
        {
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

    /**
     * Save the steps data into a Kettle repository
     * 
     * @param rep
     *            The Kettle repository to save to
     * @param idTransformation
     *            The transformation ID
     * @param idStep
     *            The step ID
     * @throws KettleException
     *             When an unexpected error occurred (database, network, etc)
     */
    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep) throws KettleException
    {
        try
        { 
            rep.saveStepAttribute(idTransformation, idStep, "TableName", this.tabelName);
            
            rep.saveStepAttribute(idTransformation, idStep, "factDimLensString",
            		factDimLensString);
            rep.saveStepAttribute(idTransformation, idStep, "outputRowSize", this.outputRowSize); 
            rep.saveStepAttribute(idTransformation, idStep, "cubeName", this.cubeName);
            rep.saveStepAttribute(idTransformation, idStep, "schemaName", this.schemaName); 
            rep.saveStepAttribute(idTransformation, idStep, "dimensionCount", this.dimensionCount);
            rep.saveStepAttribute(idTransformation, idStep, "highCardinalityDims", this.highCardinalityDims);
            rep.saveStepAttribute(idTransformation, idStep, "complexDimensionCount", this.complexDimensionCount);
            rep.saveStepAttribute(idTransformation, idStep, "measureCount", this.measureCount); 
            rep.saveStepAttribute(idTransformation, idStep, "isUpdateMemberRequest", this.updateMemberRequest); 
            rep.saveStepAttribute(idTransformation, idStep, "currentRestructNumber", currentRestructNumber);
        }
        catch(Exception e)
        {
            throw new KettleException(BaseMessages.getString(PKG,
                    "TemplateStep.Exception.UnableToSaveStepInfoToRepository", new String[0]) + idStep, e);
        }
    }

    /**
     * Read the steps information from a Kettle repository
     * 
     * @param rep
     *            The repository to read from
     * @param idStep
     *            The step ID
     * @param databases
     *            The databases to reference
     * @param counters
     *            The counters to reference
     * @throws KettleException
     *             When an unexpected error occurred (database, network, etc)
     */
    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleException
    {
        try 
        {
            this.tabelName = rep.getStepAttributeString(idStep, "TableName");
            this.outputRowSize = rep.getStepAttributeString(idStep, "outputRowSize");
            this.schemaName = rep.getStepAttributeString(idStep, "schemaName");
            this.cubeName = rep.getStepAttributeString(idStep, "cubeName");
            this.dimensionCount = rep.getStepAttributeString(idStep, "dimensionCount");
            this.highCardinalityDims=rep.getStepAttributeString(idStep, "highCardinalityDims");
            this.complexDimensionCount = rep.getStepAttributeString(idStep, "complexDimensionCount");
            this.measureCount = rep.getStepAttributeString(idStep, "measureCount");
            this.updateMemberRequest = rep.getStepAttributeString(idStep, "isUpdateMemberRequest");
            this.currentRestructNumber = (int)rep.getStepAttributeInteger(idStep, "currentRestructNumber");
        }
        catch(Exception ex)
        {
            throw new KettleException(BaseMessages.getString(PKG,
                    "MolapDataWriterStepMeta.Exception.UnexpectedErrorInReadingStepInfo", new String[0]), ex);
        }
    }

    /**
     * Checks the settings of this step and puts the findings in a remarks List.
     * 
     * @param remarks
     *            The list to put the remarks in @see
     *            org.pentaho.di.core.CheckResult
     * @param stepMeta
     *            The stepMeta to help checking
     * @param prev
     *            The fields coming from the previous step
     * @param input
     *            The input step names
     * @param output
     *            The output step names
     * @param info
     *            The fields that are used as information by the step
     */
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
            RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info)
    {
//        if(input.length > 0)
//        {
//            CheckResult cr = new CheckResult(1, "Step is receiving info from other steps.", stepMeta);
//            remarks.add(cr);
//        }
//        else
//        {
//            CheckResult cr = new CheckResult(4, "No input received from other steps!", stepMeta);
//            remarks.add(cr);
//        }
        MolapDataProcessorUtil.checkResult(remarks, stepMeta, input);
    }

    /**
     * Get the executing step, needed by Trans to launch a step.
     * 
     * @param stepMeta
     *            The step info
     * @param stepDataInterface
     *            the step data interface linked to this step. Here the step can
     *            store temporary data, database connections, etc.
     * @param copyNr
     *            The copy nr to get
     * @param transMeta
     *            The transformation info
     * @param trans
     *            The launching transformation
     */
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans)
    {
        return new SortKeyStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * Get a new instance of the appropriate data class. This data class
     * implements the StepDataInterface. It basically contains the persisting
     * data that needs to live on, even if a worker thread is terminated.
     * 
     * @return The appropriate StepDataInterface class.
     */
    public StepDataInterface getStepData()
    {
        return new SortKeyStepData();
    }



    /**
     * Below method will be used to get the out row size 
     * 
     * @return outputRowSize
     *
     */
    public String getOutputRowSize()
    {
        return outputRowSize;
    }

    /**
     * below mthod will be used to set the out row size
     * 
     * @param outputRowSize
     *
     */
    public void setOutputRowSize(String outputRowSize)
    {
        this.outputRowSize = outputRowSize;
    }
    
    /**
     * This method will return the table name
     * 
     * @return tabelName
     * 
     */
    
    public String getTabelName()
    {
        return this.tabelName;
    }

    /**
     * This method will set the table name
     * 
     * @param tabelName
     * 
     */
    public void setTabelName(String tabelName)
    {
        this.tabelName = tabelName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }
    
    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeName;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

	/**
	 * @return the dimensionCount
	 */
	public int getDimensionCount()
	{
		return Integer.parseInt(dimensionCount);
	}
	
	public void setDimensionCount(String dimensionCount)
	{
		this.dimensionCount = dimensionCount;
	}

	
	/**
	 * @return the complexDimensionCount
	 */
	public int getComplexDimensionCount()
	{
		return Integer.parseInt(complexDimensionCount);
	}
	
	public void setComplexDimensionCount(String complexDimensionCount)
	{
		this.complexDimensionCount = complexDimensionCount;
	}

	/**
     * @return the measureCount
     */
    public int getMeasureCount()
    {
        return Integer.parseInt(measureCount);
    }

    /**
     * @param measureCount the measureCount to set
     */
    public void setMeasureCount(String measureCount)
    {
        this.measureCount = measureCount;
    }

    /**
     * @return the factDimLensString
     */
    public String getFactDimLensString()
    {
        return factDimLensString; 
    }

    /**
     * @param factDimLensString the factDimLensString to set
     */
    public void setFactDimLensString(String factDimLensString)
    {
        this.factDimLensString = factDimLensString;
    }

    /**
     * @return the isUpdateMemberRequest
     */
    public boolean isUpdateMemberRequest()
    {
        return Boolean.parseBoolean(updateMemberRequest);
    }

    /**
     * @param isUpdateMemberRequest the isUpdateMemberRequest to set
     */
    public void setIsUpdateMemberRequest(String isUpdateMemberRequest)
    {
        this.updateMemberRequest = isUpdateMemberRequest;
    }
    
    /**
     * @return the currentRestructNumber
     */
    public int getCurrentRestructNumber()
    {
        return currentRestructNumber;
    }

    /**
     * @param currentRestructNum the currentRestructNumber to set
     */
    public void setCurrentRestructNumber(int currentRestructNum)
    {
        this.currentRestructNumber = currentRestructNum;
    }

	public String getHighCardinalityDims() {
		return highCardinalityDims;
	}

	public void setHighCardinalityDims(String highCardinalityDims) {
		this.highCardinalityDims = highCardinalityDims;
	}

    /**
     * @return the highCardinalityCount
     */
    public int getHighCardinalityCount()
    {
        return highCardinalityCount;
    }

    /**
     * @param highCardinalityCount the highCardinalityCount to set
     */
    public void setHighCardinalityCount(int highCardinalityCount)
    {
        this.highCardinalityCount = highCardinalityCount;
    }

}