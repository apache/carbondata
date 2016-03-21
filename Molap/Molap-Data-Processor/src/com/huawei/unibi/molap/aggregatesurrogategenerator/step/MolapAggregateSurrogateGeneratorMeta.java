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

package com.huawei.unibi.molap.aggregatesurrogategenerator.step;

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

public class MolapAggregateSurrogateGeneratorMeta extends BaseStepMeta
        implements StepMetaInterface
{
    /**
     * for i18n purposes
     */
    private static final Class<?> PKG = MolapAggregateSurrogateGeneratorMeta.class; // for
                                                                              // i18n

    /**
     * aggDimeLensString
     */
    private String aggDimeLensString;

    /**
     * aggregatorString
     */
    private String aggregatorString;

    /**
     * aggregators
     */
    private String[] aggregators;

    /**
     * aggDimeLens
     */
    private int[] aggDimeLens;

    /**
     * measureCountString
     */
    private String measureCountString;

    /**
     * aggregateMeasuresString
     */
    private String aggregateMeasuresString;

    /**
     * aggregateMeasures
     */
    private String[] aggregateMeasures;

    /**
     * aggregateMeasuresColumnNameString
     */
    private String aggregateMeasuresColumnNameString;

    /**
     * aggregateMeasuresColumnName
     */
    private String[] aggregateMeasuresColumnName;

    /**
     * aggregateLevelsString
     */
    private String aggregateLevelsString;

    /**
     * aggregateLevels
     */
    private String[] aggregateLevels;

    /**
     * factMeasureString
     */
    private String factMeasureString;

    /**
     * factMeasure
     */
    private String[] factMeasure;

    /**
     * factLevelsString
     */
    private String factLevelsString;

    /**
     * factLevels
     */
    private String[] factLevels;

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * tableName
     */
    private String tableName;

    /**
     * factTableName
     */
    private String factTableName;

    /**
     * heirAndKeySize
     */
    private String heirAndKeySize;

    /**
     * heirAndDimLens
     */
    private String heirAndDimLens;
    
    /**
     * isMdkeyInOutRowRequired
     */
    private String mdkeyInOutRowRequired;
    
    /**
     * isManualAutoAggRequest
     */
    private String manualAutoAggRequest;
    
    /**
     * factDimLensString
     */
    private String factDimLensString;
    
    /**
     * factDimLens
     */
    private int[] factDimLens;
    
    private String factStorePath;

    /**
     * set the default value for all the properties
     * 
     */
    @Override
    public void setDefault()
    {
        tableName = "";
        schemaName = "";
        cubeName = "";
        factLevelsString = "";
        factMeasureString = "";
        aggregateLevelsString = "";
        aggregateMeasuresString = "";
        measureCountString = "";
        factTableName = "";
        heirAndKeySize = "";
        heirAndDimLens = "";
        aggregatorString = "";
        aggDimeLensString = "";
        aggregateMeasuresColumnNameString = "";
        mdkeyInOutRowRequired= "";
        manualAutoAggRequest="";
        factDimLensString="";
        factStorePath = "";
    }

    /**
     * below method will be used initialise the meta
     */
    public void initialize()
    {
        factMeasure = factMeasureString
                .split(MolapCommonConstants.AMPERSAND_SPC_CHARACTER);
        factLevels = factLevelsString
                .split(MolapCommonConstants.HASH_SPC_CHARACTER);
        aggregateLevels = aggregateLevelsString
                .split(MolapCommonConstants.HASH_SPC_CHARACTER);
        aggregateMeasures = aggregateMeasuresString
                .split(MolapCommonConstants.HASH_SPC_CHARACTER);
        aggregateMeasuresColumnName = aggregateMeasuresColumnNameString
                .split(MolapCommonConstants.HASH_SPC_CHARACTER);
        String[] aggDimeLensStringArray = aggDimeLensString
                .split(MolapCommonConstants.COMA_SPC_CHARACTER);
        aggDimeLens = new int[aggDimeLensStringArray.length];
        for(int i = 0;i < aggDimeLens.length;i++)
        {
            aggDimeLens[i] = Integer.parseInt(aggDimeLensStringArray[i]);
        }
        aggregators = aggregatorString
                .split(MolapCommonConstants.HASH_SPC_CHARACTER);
        String[] dimLensStringArray = factDimLensString.split(MolapCommonConstants.COMA_SPC_CHARACTER);
        factDimLens= new int[dimLensStringArray.length];
        for(int i = 0;i < dimLensStringArray.length;i++)
        {
        	factDimLens[i]=Integer.parseInt(dimLensStringArray[i]);
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
    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
            StepMeta stepMeta, RowMetaInterface prev, String[] input,
            String[] output, RowMetaInterface info) 
    {
        CheckResult chkResRef=null;
        // See if we have input streams leading to this step! 
        if(input.length > 0)
        {
            chkResRef = new CheckResult(CheckResult.TYPE_RESULT_OK,
                    "Step is receiving info from other steps.", stepMeta); 
            remarks.add(chkResRef);
        }
        else
        {
            chkResRef = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
                    "No input received from other steps!", stepMeta);
            remarks.add(chkResRef);
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
        return new MolapAggregateSurrogateGeneratorStep(stepMeta,
                stepDataInterface, copyNr, transMeta, trans);
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
        return new MolapAggregateSurrogateGeneratorData();
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
        StringBuffer stringBuffer1 = new StringBuffer(150);
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("TableName", tableName));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("cubeName", cubeName));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("schemaName", schemaName));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("factLevelsString", factLevelsString));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("factMeasureString", factMeasureString));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("aggregateLevelsString",
                        aggregateLevelsString));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("aggregateMeasuresString",
                        aggregateMeasuresString));
        stringBuffer1.append("    ").append(
                XMLHandler
                        .addTagValue("measureCountString", measureCountString));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("factTableName", factTableName));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("heirAndKeySize", heirAndKeySize));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("heirAndDimLens", heirAndDimLens));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("aggDimeLensString", aggDimeLensString));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("aggregatorString", aggregatorString));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("aggregateMeasuresColumnNameString",
                        aggregateMeasuresColumnNameString));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("isMdkeyInOutRowRequired",
                        mdkeyInOutRowRequired));
        stringBuffer1.append("    ").append(
                XMLHandler.addTagValue("isManualAutoAggRequest",
                        manualAutoAggRequest));
        stringBuffer1.append("    ").append(
        		XMLHandler.addTagValue("factDimLensString",
        				factDimLensString));
        
        stringBuffer1.append("    ").append(
        		XMLHandler.addTagValue("factStorePath",
        				factStorePath));
        
        return stringBuffer1.toString();
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
            tableName = XMLHandler.getTagValue(stepnode, "TableName");
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            factLevelsString = XMLHandler.getTagValue(stepnode,
                    "factLevelsString");
            factMeasureString = XMLHandler.getTagValue(stepnode,
                    "factMeasureString");
            aggregateLevelsString = XMLHandler.getTagValue(stepnode,
                    "aggregateLevelsString");
            aggregateMeasuresString = XMLHandler.getTagValue(stepnode,
                    "aggregateMeasuresString");
            measureCountString = XMLHandler.getTagValue(stepnode,
                    "measureCountString");
            factTableName = XMLHandler.getTagValue(stepnode, "factTableName");
            heirAndKeySize = XMLHandler.getTagValue(stepnode, "heirAndKeySize");
            heirAndDimLens = XMLHandler.getTagValue(stepnode, "heirAndDimLens");
            aggDimeLensString = XMLHandler.getTagValue(stepnode,
                    "aggDimeLensString");
            aggregatorString = XMLHandler.getTagValue(stepnode,
                    "aggregatorString");
            aggregateMeasuresColumnNameString = XMLHandler.getTagValue(
                    stepnode, "aggregateMeasuresColumnNameString");
            mdkeyInOutRowRequired = XMLHandler.getTagValue(
                    stepnode, "isMdkeyInOutRowRequired");
            manualAutoAggRequest = XMLHandler.getTagValue(
                    stepnode, "isManualAutoAggRequest");
            factDimLensString = XMLHandler.getTagValue(
            		stepnode, "factDimLensString");
            factStorePath = XMLHandler.getTagValue(
            		stepnode, "factStorePath");
        }
        catch(Exception e)
        {
            throw new KettleXMLException(
                    "Unable to read step info from XML node", e);
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
    @Override
    public void readRep(Repository rep, ObjectId idStep,
            List<DatabaseMeta> databases, Map<String, Counter> counters) 
            throws KettleException
    {
        try
        {
            tableName = rep.getStepAttributeString(idStep, "TableName");
            schemaName = rep.getStepAttributeString(idStep, "schemaName");
            cubeName = rep.getStepAttributeString(idStep, "cubeName");
            factLevelsString = rep.getStepAttributeString(idStep,
                    "factLevelsString");
            factMeasureString = rep.getStepAttributeString(idStep,
                    "factMeasureString");
            aggregateLevelsString = rep.getStepAttributeString(idStep,
                    "aggregateLevelsString");
            measureCountString = rep.getStepAttributeString(idStep,
                    "measureCountString");
            aggregateMeasuresString = rep.getStepAttributeString(idStep,
                    "aggregateMeasuresString");
            factTableName = rep
                    .getStepAttributeString(idStep, "factTableName");
            heirAndDimLens = rep.getStepAttributeString(idStep,
                    "heirAndDimLens");
            heirAndKeySize = rep.getStepAttributeString(idStep,
                    "heirAndKeySize");
            aggDimeLensString = rep.getStepAttributeString(idStep,
                    "aggDimeLensString");
            aggregatorString = rep.getStepAttributeString(idStep,
                    "aggregatorString");
            aggregateMeasuresColumnNameString = rep.getStepAttributeString(
                    idStep, "aggregateMeasuresColumnNameString");
            mdkeyInOutRowRequired = rep.getStepAttributeString(
                    idStep, "isMdkeyInOutRowRequired");
            manualAutoAggRequest = rep.getStepAttributeString(
                    idStep, "isManualAutoAggRequest");
            factDimLensString = rep.getStepAttributeString(
            		idStep, "factDimLensString");
            factStorePath = rep.getStepAttributeString(
            		idStep, "factStorePath");
        }
        catch(Exception e)
        {
            throw new KettleException(
                    BaseMessages
                            .getString(PKG,
                                    "MolapMDKeyStepMeta.Exception.UnexpectedErrorInReadingStepInfo"),
                    e);
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
                    "TableName", tableName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "schemaName",
                    schemaName);
            rep.saveStepAttribute(idTransformation, idStep, "cubeName",
                    cubeName);
            rep.saveStepAttribute(idTransformation, idStep,
                    "factLevelsString", factLevelsString); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep,
                    "factMeasureString", factMeasureString);
            rep.saveStepAttribute(idTransformation, idStep,
                    "aggregateLevelsString", aggregateLevelsString);
            rep.saveStepAttribute(idTransformation, idStep,
                    "measureCountString", measureCountString);
            rep.saveStepAttribute(idTransformation, idStep,
                    "aggregateMeasuresString", aggregateMeasuresString);
            rep.saveStepAttribute(idTransformation, idStep, "factTableName",
                    factTableName);
            rep.saveStepAttribute(idTransformation, idStep, "heirAndDimLens",
                    heirAndDimLens);
            rep.saveStepAttribute(idTransformation, idStep, "heirAndKeySize",
                    heirAndKeySize);
            rep.saveStepAttribute(idTransformation, idStep,
                    "aggDimeLensString", aggDimeLensString);
            rep.saveStepAttribute(idTransformation, idStep,
                    "aggregatorString", aggregatorString);
            rep.saveStepAttribute(idTransformation, idStep,
                    "aggregateMeasuresColumnNameString",
                    aggregateMeasuresColumnNameString);
            rep.saveStepAttribute(idTransformation, idStep,
                    "isMdkeyInOutRowRequired",
                    mdkeyInOutRowRequired);
            rep.saveStepAttribute(idTransformation, idStep,
                    "isManualAutoAggRequest",
                    manualAutoAggRequest);
            rep.saveStepAttribute(idTransformation, idStep,
            		"factDimLensString",
            		factDimLensString);
            rep.saveStepAttribute(idTransformation, idStep,
            		"factStorePath",
            		factStorePath);
        }
        catch(Exception e)
        {
            throw new KettleException(BaseMessages.getString(PKG,
                    "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
                    + idStep, e);
        }
    }

    /**
     * @return the measureCount
     */
    public int getMeasureCount()
    {
        return Integer.parseInt(measureCountString);
    }

    /**
     * @param measureCount
     *            the measureCount to set
     */
    public void setMeasureCount(int measureCount)
    {
        this.measureCountString = measureCount + "";
    }

    /**
     * @return the aggregateMeasures
     */
    public String[] getAggregateMeasures()
    {
        return aggregateMeasures;
    }

    /**
     * @return the aggregateLevels
     */
    public String[] getAggregateLevels()
    {
        return aggregateLevels;
    }


    /**
     * @return the factMeasure
     */
    public String[] getFactMeasure()
    {
        return factMeasure;
    }

    /**
     * @return the factLevels
     */
    public String[] getFactLevels()
    {
        return factLevels;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @param schemaName
     *            the schemaName to set
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
     * @param cubeName
     *            the cubeName to set
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * @return the tableName
     */
    public String getTableName()
    {
        return tableName;
    }

    /**
     * @param tableName
     *            the tableName to set
     */
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    /**
     * @return the factTableName
     */
    public String getFactTableName()
    {
        return factTableName;
    }

    /**
     * @param factTableName
     *            the factTableName to set
     */
    public void setFactTableName(String factTableName)
    {
        this.factTableName = factTableName;
    }

    /**
     * @param measureCountString
     *            the measureCountString to set
     */
    public void setMeasureCountString(String measureCountString)
    {
        this.measureCountString = measureCountString;
    }

    /**
     * @param aggregateMeasuresString
     *            the aggregateMeasuresString to set
     */
    public void setAggregateMeasuresString(String aggregateMeasuresString)
    {
        this.aggregateMeasuresString = aggregateMeasuresString;
    }

    /**
     * @param aggregateLevelsString
     *            the aggregateLevelsString to set
     */
    public void setAggregateLevelsString(String aggregateLevelsString)
    {
        this.aggregateLevelsString = aggregateLevelsString;
    }

    /**
     * @param factMeasureString
     *            the factMeasureString to set
     */
    public void setFactMeasureString(String factMeasureString)
    {
        this.factMeasureString = factMeasureString;
    }

    /**
     * @param factLevelsString
     *            the factLevelsString to set
     */
    public void setFactLevelsString(String factLevelsString)
    {
        this.factLevelsString = factLevelsString;
    }

    /**
     * @param heirAndKeySize
     *            the heirAndKeySize to set
     */
    public void setHeirAndKeySize(String heirAndKeySize)
    {
        this.heirAndKeySize = heirAndKeySize;
    }

    /**
     * @param heirAndDimLens
     *            the heirAndDimLens to set
     */
    public void setHeirAndDimLens(String heirAndDimLens)
    {
        this.heirAndDimLens = heirAndDimLens;
    }

    /**
     * @return the aggregators
     */
    public String[] getAggregators()
    {
        return aggregators;
    }

    /**
     * @param aggregators
     *            the aggregators to set
     */
    public void setAggregators(String[] aggregators)
    {
        this.aggregators = aggregators;
    }

    /**
     * @param aggDimeLensString
     *            the aggDimeLensString to set
     */
    public void setAggDimeLensString(String aggDimeLensString)
    {
        this.aggDimeLensString = aggDimeLensString;
    }

    /**
     * @param aggregatorString
     *            the aggregatorString to set
     */
    public void setAggregatorString(String aggregatorString)
    {
        this.aggregatorString = aggregatorString;
    }

    /**
     * @param aggregateMeasuresColumnNameString
     *            the aggregateMeasuresColumnNameString to set
     */
    public void setAggregateMeasuresColumnNameString(
            String aggregateMeasuresColumnNameString)
    {
        this.aggregateMeasuresColumnNameString = aggregateMeasuresColumnNameString;
    }

    /**
     * @return the isMdkeyInOutRowRequired
     */
    public boolean isMdkeyInOutRowRequired()
    {
        return Boolean.parseBoolean(mdkeyInOutRowRequired);
    }

    /**
     * @param isMdkeyInOutRowRequired the isMdkeyInOutRowRequired to set
     */
    public void setMdkeyInOutRowRequired(String isMdkeyInOutRowRequired)
    {
        this.mdkeyInOutRowRequired = isMdkeyInOutRowRequired;
    }

    /**
     * @param manualAutoAggRequest the isManualAutoAggRequest to set
     */
    public void setIsManualAutoAggRequest(String manualAutoAggRequest)
    {
        this.manualAutoAggRequest = manualAutoAggRequest;
    }

	/**
	 * @return the factDimLens
	 */
	public int[] getFactDimLens() 
	{
		return factDimLens;
	}

	/**
	 * @param factDimLensString the factDimLensString to set
	 */
	public void setFactDimLensString(String factDimLensString) 
	{
		this.factDimLensString = factDimLensString;
	}

	public void setFactStorePath(String factStorePath)
	{
		this.factStorePath = factStorePath;
	}
}
