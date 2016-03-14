/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnJ+V/602UknMsGpYHSUgZQpdq1zy6rWrfnMCuFGD6FSa4QJE67IGLDdIW4SJPc2kHaEy
ha0lV5CtaKBMz+q4WgyOq9DZZsMS3YQWs9M5OKLquLd1C9HNPFoI0S8QtbtLrA==*/
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
package com.huawei.unibi.molap.mdkeygen;

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
 * Project Name NSE V3R7C00 Module Name : MOLAP Author :C00900810 Created Date
 * :24-Jun-2013 FileName : MolapMDKeyGenStepMeta.java Class Description :
 * Version 1.0
 */
public class MolapMDKeyGenStepMeta extends BaseStepMeta implements StepMetaInterface,Cloneable
{
    /**
     * for i18n purposes
     */
    private static Class<?> pkg = MolapMDKeyGenStepMeta.class; // for i18n
                                                               // purposes

    /**
     * tableName
     */
    private String tableName;

    /**
     * aggregateLevels
     */
    private String aggregateLevels;

    /**
     * numberOfCores
     */
    private String numberOfCores;
    
    /**
     * cubeName
     */
    private String cubeName;
    
    /**
     * schemaName
     */
    private String schemaName;
    
    /**
     * isAutoAggRequest
     */
    private String autoAggRequest;
    
    /**
     * isAutoAggRequest
     */
    private String factMdKeyInInputRow;
    
    /**
     * measureCount
     */
    private String measureCount;
    
    private int currentRestructNumber;
    
    /**
     * Constructor
     */
    public MolapMDKeyGenStepMeta()
    {
        super();
    }

    @Override
    public void setDefault()
    {
        tableName = "";
        aggregateLevels = "";
        numberOfCores="";
        cubeName="";
        schemaName="";
        autoAggRequest="";
        factMdKeyInInputRow="";
        currentRestructNumber = -1;
    }

    /**
 * 
 */
    public String getXML()
    {
        StringBuffer retval = new StringBuffer(150);

        retval.append("    ").append(XMLHandler.addTagValue("TableName", tableName));
        retval.append("    ").append(XMLHandler.addTagValue("AggregateLevels", aggregateLevels));
        retval.append("    ").append(XMLHandler.addTagValue("NumberOfCores", numberOfCores));
        
        retval.append("    ").append(XMLHandler.addTagValue("cubeName", cubeName));
        retval.append("    ").append(XMLHandler.addTagValue("schemaName", schemaName));
        retval.append("    ").append(XMLHandler.addTagValue("isAutoAggRequest", autoAggRequest));
        retval.append("    ").append(XMLHandler.addTagValue("isFactMdKeyInInputRow", factMdKeyInInputRow));
        retval.append("    ").append(XMLHandler.addTagValue("measureCount", measureCount));
        retval.append("    ").append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        return retval.toString();
    }

    @Override
    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep) throws KettleException
    {
        try
        { 
            rep.saveStepAttribute(idTransformation, idStep, "TableName", tableName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "AggregateLevels", aggregateLevels); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "NumberOfCores", numberOfCores);
            
            rep.saveStepAttribute(idTransformation, idStep, "schemaName", schemaName);
            rep.saveStepAttribute(idTransformation, idStep, "cubeName", cubeName);
            rep.saveStepAttribute(idTransformation, idStep, "isAutoAggRequest", autoAggRequest);
            rep.saveStepAttribute(idTransformation, idStep, "isFactMdKeyInInputRow", factMdKeyInInputRow);
            rep.saveStepAttribute(idTransformation, idStep, "measureCount", measureCount);
            rep.saveStepAttribute(idTransformation, idStep, "currentRestructNumber", currentRestructNumber);
            
        }
        catch(Exception e)
        {
            throw new KettleException(BaseMessages.getString(pkg,
                    "TemplateStep.Exception.UnableToSaveStepInfoToRepository") + idStep, e);
        }

    }
    
    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleXMLException
    {
        // TODO Auto-generated method stub
        try
        {
            tableName = XMLHandler.getTagValue(stepnode, "TableName");
            aggregateLevels = XMLHandler.getTagValue(stepnode, "AggregateLevels");
            numberOfCores = XMLHandler.getTagValue(stepnode, "NumberOfCores");
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            autoAggRequest = XMLHandler.getTagValue(stepnode, "isAutoAggRequest");
            factMdKeyInInputRow = XMLHandler.getTagValue(stepnode, "isFactMdKeyInInputRow");
            measureCount = XMLHandler.getTagValue(stepnode, "measureCount");
            currentRestructNumber = Integer.parseInt(XMLHandler.getTagValue(stepnode, "currentRestructNumber"));
        }
        catch(Exception ex)
        {
            // TODO Auto-generated catch block
            throw new KettleXMLException("Unable to read step info from XML node", ex);
        }
    }
    
    public Object clone()
    {
        Object retval = super.clone();
        return retval;
    }

    @Override
    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleException
    {
        try
        { 
            tableName = rep.getStepAttributeString(idStep, "TableName");
            aggregateLevels = rep.getStepAttributeString(idStep, "AggregateLevels");
            numberOfCores = rep.getStepAttributeString(idStep, "NumberOfCores");
            
            schemaName = rep.getStepAttributeString(idStep, "schemaName");
            cubeName = rep.getStepAttributeString(idStep, "cubeName");
            autoAggRequest = rep.getStepAttributeString(idStep, "isAutoAggRequest");
            factMdKeyInInputRow = rep.getStepAttributeString(idStep, "isFactMdKeyInInputRow");
            measureCount = rep.getStepAttributeString(idStep, "measureCount");
            currentRestructNumber = (int)rep.getStepAttributeInteger(idStep, "currentRestructNumber");
        }
        catch(Exception e)
        {
            // TODO Auto-generated catch block
            throw new KettleException(BaseMessages.getString(pkg,
                    "MolapMDKeyStepMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
        }

    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
            RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info)
    {
    	MolapDataProcessorUtil.checkResult(remarks, stepMeta, input);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans)
    {
        // TODO Auto-generated method stub
        return new MolapMDKeyGenStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData()
    {
        // TODO Auto-generated method stub
        return new MolapMDKeyGenStepData();
    }

    public String getTableName()
    {
        return tableName;
    }
    
    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }
    
    public String getNumberOfCores()
    {
        return numberOfCores;
    }

    public String getAggregateLevels()
    {
        return aggregateLevels;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName()
    {
        return cubeName;
    }
    
    public void setAggregateLevels(String aggregateLevels)
    {
        this.aggregateLevels = aggregateLevels;
    }

    public void setNumberOfCores(String numberOfCores)
    {
        this.numberOfCores = numberOfCores;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName)
    {
        this.cubeName = cubeName;
    }

    /**
     * @return the isAutoAggRequest
     */
    public boolean isAutoAggRequest()
    {
        return Boolean.parseBoolean(autoAggRequest);
    }

    /**
     * @param isAutoAggRequest the isAutoAggRequest to set
     */
    public void setIsAutoAggRequest(String isAutoAggRequest)
    {
        this.autoAggRequest = isAutoAggRequest;
    }

    /**
     * @return the isFactMdKeyInInputRow
     */
    public boolean isFactMdKeyInInputRow()
    {
        return Boolean.parseBoolean(factMdKeyInInputRow);
    }

    /**
     * @param isFactMdKeyInInputRow the isFactMdKeyInInputRow to set
     */
    public void setFactMdKeyInInputRow(String isFactMdKeyInInputRow)
    {
        this.factMdKeyInInputRow = isFactMdKeyInInputRow;
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
}
