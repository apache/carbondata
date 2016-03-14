/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwe/owl+XpObKvwejIomJrN10iZBX17jBC5vj/zP
61+XaaIOHfsn8R+mKZAren6JUO7WXV8R7WkTVVGk2KlS3R1wSt8TZRNS4YEraxgfbb9eOX+o
EJhePFboIE4n6QJPc/NxsiBbNyD7IJHw1nZfRZm8k4uYYKOcLD7vzdWbgmQJtg==*/
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

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
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

import com.huawei.unibi.molap.constants.MolapCommonConstants;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : MolapSliceMergerStepMeta.java
 * Class Description : This class is the entry point for MolapDataWriterStepMeta plug-in
 * Version 1.0
 */
public class MolapSliceMergerStepMeta extends BaseStepMeta implements
        StepMetaInterface, Cloneable
{

    /**
     * for i18n purposes
     */
    private static final Class<?> PKG = MolapSliceMergerStepMeta.class;

    /**
     * table name
     */
    private String tabelName;

    /**
     * mdkey size
     */
    private String mdkeySize;
    
    /**
     * measureCount
     */
    private String measureCount;
    
    /**
     * heirAndKeySize
     */
    private String heirAndKeySize;
    
    /**
     * schemaName
     */
    private String schemaName;
    
    /**
     * cubeName
     */
    private String cubeName;
    
    /**
     * isGroupByEnabled
     */
    private String groupByEnabled;

    /**
     * aggregatorString
     */
    private String aggregatorString; 

    /**
     * aggregatorClassString
     */
    private String aggregatorClassString;
    
    /**
     * factDimLensString
     */
    private String factDimLensString;
    
    private int currentRestructNumber = -1;
    
    private String levelAnddataTypeString;
    
    /**
     * 
     * MolapDataWriterStepMeta constructor to initialize this class
     * 
     */
    public MolapSliceMergerStepMeta()
    {
        super();
    }

    /**
     * set the default value for all the properties
     * 
     */
    @Override
    public void setDefault()
    {
        tabelName = "";
        mdkeySize = "";
        measureCount= "";
        heirAndKeySize="";
        cubeName="";
        schemaName="";
        groupByEnabled="";
        aggregatorClassString="";
        aggregatorString= "";
        factDimLensString="";
        currentRestructNumber = -1;
        levelAnddataTypeString="";
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
        retval.append("    ").append(
                XMLHandler.addTagValue("TableName", tabelName));
        retval.append("    ").append(
                XMLHandler.addTagValue("MDKeySize", mdkeySize));
        retval.append("    ").append(
                XMLHandler.addTagValue("Measurecount", measureCount));
        retval.append("    ").append(
                XMLHandler.addTagValue("HeirAndKeySize", heirAndKeySize));
        retval.append("    ").append(
                XMLHandler.addTagValue("cubeName", cubeName));
        retval.append("    ").append(
                XMLHandler.addTagValue("schemaName", schemaName));
        retval.append("    ").append(
                XMLHandler.addTagValue("isGroupByEnabled", groupByEnabled));
        retval.append("    ").append(
                XMLHandler.addTagValue("aggregatorClassString", aggregatorClassString));
        retval.append("    ").append(
                XMLHandler.addTagValue("aggregatorString", aggregatorString));
        retval.append("    ").append(
                XMLHandler.addTagValue("factDimLensString", factDimLensString));
        retval.append("    ").append(
                XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        retval.append("    ").append(
                XMLHandler.addTagValue("levelAnddataTypeString", levelAnddataTypeString));
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
    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases,
            Map<String, Counter> counters) throws KettleXMLException
    {
        try
        {
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            tabelName = XMLHandler.getTagValue(stepnode, "TableName");
            mdkeySize = XMLHandler.getTagValue(stepnode, "MDKeySize");
            measureCount = XMLHandler.getTagValue(stepnode, "Measurecount");
            heirAndKeySize = XMLHandler.getTagValue(stepnode, "HeirAndKeySize");
            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            groupByEnabled = XMLHandler.getTagValue(stepnode, "isGroupByEnabled");
            aggregatorClassString = XMLHandler.getTagValue(stepnode, "aggregatorClassString");
            aggregatorString = XMLHandler.getTagValue(stepnode, "aggregatorString");
            factDimLensString = XMLHandler.getTagValue(stepnode, "factDimLensString");
            levelAnddataTypeString = XMLHandler.getTagValue(stepnode, "levelAnddataTypeString");
            currentRestructNumber = Integer.parseInt(
                    XMLHandler.getTagValue(stepnode, "currentRestructNumber"));
        }
        catch(Exception e)
        {
            // TODO Auto-generated catch block
            throw new KettleXMLException(
                    "Unable to read step info from XML node", e);
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
    @Override
    public void saveRep(Repository rep, ObjectId idTransformation,
            ObjectId idStep) throws KettleException
    {
        try
        { 
            rep.saveStepAttribute(idTransformation, idStep,
                    "TableName", tabelName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep,
                    "MDKeySize", mdkeySize); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "Measurecount",
                    measureCount);
            rep.saveStepAttribute(idTransformation, idStep,
                    "HeirAndKeySize", heirAndKeySize); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep,
                    "cubeName", cubeName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep,
                    "schemaName", schemaName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "isGroupByEnabled",
                    groupByEnabled);
            rep.saveStepAttribute(idTransformation, idStep, "aggregatorClassString",
                    aggregatorClassString);
            rep.saveStepAttribute(idTransformation, idStep, "aggregatorString",
                    aggregatorString);
            rep.saveStepAttribute(idTransformation, idStep, "factDimLensString",
                    factDimLensString);
            rep.saveStepAttribute(idTransformation, idStep, "levelAnddataTypeString",
                    levelAnddataTypeString);
            rep.saveStepAttribute(idTransformation, idStep, 
                    "currentRestructNumber", currentRestructNumber);
            
        }
        catch(Exception e)
        {
            throw new KettleException(BaseMessages.getString(PKG,
                    "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
                    + idStep, e);
        }
    }
    
    //TODO SIMIAN
    /**
     * Make an exact copy of this step, make sure to explicitly copy Collections
     * etc.
     * 
     * @return an exact copy of this step
     */
    public Object clone()
    {
        Object retval = super.clone();
        return retval;
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
    @Override
    public void readRep(Repository rep, ObjectId idStep,
            List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleException
    {
        try 
        {
            tabelName = rep.getStepAttributeString(idStep, "TableName");
            mdkeySize = rep.getStepAttributeString(idStep, "MDKeySize");
            measureCount = rep.getStepAttributeString(idStep, "Measurecount");
            heirAndKeySize = rep.getStepAttributeString(idStep, "HeirAndKeySize");
            schemaName = rep.getStepAttributeString(idStep, "schemaName");
            cubeName = rep.getStepAttributeString(idStep, "cubeName");
            groupByEnabled = rep.getStepAttributeString(idStep, "isGroupByEnabled");
            aggregatorClassString = rep.getStepAttributeString(idStep, "aggregatorClassString");
            aggregatorString = rep.getStepAttributeString(idStep, "aggregatorString");
            factDimLensString = rep.getStepAttributeString(idStep, "factDimLensString");
            levelAnddataTypeString = rep.getStepAttributeString(idStep, "levelAnddataTypeString");
            currentRestructNumber = (int)rep.getStepAttributeInteger(idStep, "currentRestructNumber");
        }
        catch(Exception exception)
        {
            // TODO Auto-generated catch block
            throw new KettleException(
                    BaseMessages
                            .getString(PKG,
                                    "MolapDataWriterStepMeta.Exception.UnexpectedErrorInReadingStepInfo"),
                                    exception);
        }

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
    @Override
    public StepInterface getStep(StepMeta stepMeta,
            StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans)
    {
        // TODO Auto-generated method stub
        return new MolapSliceMergerStep(stepMeta, stepDataInterface, copyNr,
                transMeta, trans);
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
    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
            StepMeta stepMeta, RowMetaInterface prev, String[] input,
            String[] output, RowMetaInterface info)
    {
        // TODO Auto-generated method stub

        CheckResult checkResVal;

        // See if we have input streams leading to this step!
        if(input.length > 0)
        {
            checkResVal = new CheckResult(CheckResult.TYPE_RESULT_OK,
                    "Step is receiving info from other steps.", stepMeta);
            remarks.add(checkResVal);
        }
        else
        {
            checkResVal = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
                    "No input received from other steps!", stepMeta);
            remarks.add(checkResVal);
        }

    }

    /**
     * Get a new instance of the appropriate data class. This data class
     * implements the StepDataInterface. It basically contains the persisting
     * data that needs to live on, even if a worker thread is terminated.
     * 
     * @return The appropriate StepDataInterface class.
     */
    @Override
    public StepDataInterface getStepData()
    {
        // TODO Auto-generated method stub
        return new MolapSliceMergerStepData();
    }

    /**
     * This method will return the table name
     * 
     * @return tabelName
     * 
     */
    public String getTabelName()
    {
        return tabelName;
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
     * This method will return mdkey size
     * 
     * @return mdkey size
     *
     */
    public String getMdkeySize()
    {
        return mdkeySize;
    }

    /**
     * This method will be used to set the mdkey
     * 
     * @param mdkeySize
     *
     */
    public void setMdkeySize(String mdkeySize)
    {
        this.mdkeySize = mdkeySize;
    }

    /**
     * This method will be used to get the measure count 
     * 
     * @return measure count
     *
     */
    public String getMeasureCount()
    {
        return measureCount;
    }

    /**
     * This method will be used to set the measure count 
     * 
     * @param measureCount
     *
     */
    public void setMeasureCount(String measureCount)
    {
        this.measureCount = measureCount;
    }
    
    /**
     * This method will be used to get the heir and its key suze string 
     * 
     * @return heirAndKeySize
     *
     */
    public String getHeirAndKeySize()
    {
        return heirAndKeySize;
    }

    /**
     * This method will be used to set the heir and key size string 
     * 
     * @param heirAndKeySize
     *
     */
    public void setHeirAndKeySize(String heirAndKeySize)
    {
        this.heirAndKeySize = heirAndKeySize;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }
    
    /**
     * @return the isGroupByEnabled
     */
    public boolean isGroupByEnabled()
    {
        return Boolean.parseBoolean(groupByEnabled);
    }

    /**
     * @param isGroupByEnabled the isGroupByEnabled to set
     */
    public void setGroupByEnabled(String isGroupByEnabled)
    {
        this.groupByEnabled = isGroupByEnabled;
    }

    /**
     * @return the aggregators
     */
    public String[] getAggregators()
    {
        return aggregatorString.split(MolapCommonConstants.HASH_SPC_CHARACTER);
    }

    /**
     * @return the aggregatorClass
     */
    public String[] getAggregatorClass()
    {
        return aggregatorClassString.split(MolapCommonConstants.HASH_SPC_CHARACTER);
    }

    /**
     * @return the aggregatorString
     */
    public String getAggregatorString()
    {
        return aggregatorString;
    }

    /**
     * @param aggregatorString the aggregatorString to set
     */
    public void setAggregatorString(String aggregatorString)
    {
        this.aggregatorString = aggregatorString;
    }

    /**
     * @return the aggregatorClassString
     */
    public String getAggregatorClassString()
    {
        return aggregatorClassString;
    }

    /**
     * @param aggregatorClassString the aggregatorClassString to set
     */
    public void setAggregatorClassString(String aggregatorClassString)
    {
        this.aggregatorClassString = aggregatorClassString;
    }

    /**
     * @return the factDimLensString
     */
    public String getFactDimLensString()
    {
        return factDimLensString;
    }

    /**
     * @param factDimLensString1 the factDimLensString to set
     */
    public void setFactDimLensString(String factDimLensString1)
    {
        this.factDimLensString = factDimLensString1;
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

    public String getLevelAnddataTypeString()
    {
        return levelAnddataTypeString;
    }

    public void setLevelAnddataTypeString(String levelAnddataTypeString)
    {
        this.levelAnddataTypeString = levelAnddataTypeString;
    }
}
