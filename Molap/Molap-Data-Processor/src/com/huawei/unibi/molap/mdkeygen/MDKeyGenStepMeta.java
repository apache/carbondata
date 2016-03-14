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

import java.util.LinkedHashMap;
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

import com.huawei.datasight.molap.datatypes.ArrayDataType;
import com.huawei.datasight.molap.datatypes.GenericDataType;
import com.huawei.datasight.molap.datatypes.PrimitiveDataType;
import com.huawei.datasight.molap.datatypes.StructDataType;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapDataProcessorUtil;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 25-Aug-2015
 * FileName 		: MDKeyGenStep.java
 * Description 		: Kettle step meta to generate MD Key
 * Class Version 	: 1.0
 */
public class MDKeyGenStepMeta extends BaseStepMeta implements StepMetaInterface
{
	/**
     * for i18n purposes
     */
    private static Class<?> pkg = MDKeyGenStepMeta.class;

    /**
     * tableName
     */
    private String tableName;

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
     * aggregateLevels
     */
    private String aggregateLevels;
    
    /**
     * measureCount
     */
    private String measureCount;
    
    /**
     * dimensionCount
     */
    private String dimensionCount;
    
    /**
     * complexDimsCount
     */
    private String complexDimsCount;
    
    /**
     * ComplexTypeString
     */
    private String complexTypeString;
    
    private Map<String, GenericDataType> complexTypes;
    
    private int currentRestructNumber;
    
	private String  highCardinalityDims;
	
	 /**
     * highCardinalityCount 
     */
    private int highCardinalityCount;
    /**
     * Constructor
     */
    public MDKeyGenStepMeta()
    {
        super();
    }

    @Override
    public void setDefault()
    {
        tableName = "";
        numberOfCores = "";
        aggregateLevels = "";
        cubeName = "";
        schemaName = "";
        highCardinalityDims="";
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
        retval.append("    ").append(XMLHandler.addTagValue("highCardinalityDims", highCardinalityDims));
        retval.append("    ").append(XMLHandler.addTagValue("measureCount", measureCount));
        retval.append("    ").append(XMLHandler.addTagValue("dimensionCount", dimensionCount));
        retval.append("    ").append(XMLHandler.addTagValue("complexDimsCount", complexDimsCount));
        retval.append("    ").append(XMLHandler.addTagValue("complexTypeString", complexTypeString));
        retval.append("    ").append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        return retval.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleXMLException
    {
        try
        {
            tableName = XMLHandler.getTagValue(stepnode, "TableName");
            aggregateLevels = XMLHandler.getTagValue(stepnode,
                    "AggregateLevels");
            numberOfCores = XMLHandler.getTagValue(stepnode, "NumberOfCores");
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            highCardinalityDims = XMLHandler.getTagValue(stepnode, "highCardinalityDims");
            measureCount = XMLHandler.getTagValue(stepnode, "measureCount");
            dimensionCount = XMLHandler.getTagValue(stepnode, "dimensionCount");
            complexDimsCount = XMLHandler.getTagValue(stepnode, "complexDimsCount");
            complexTypeString = XMLHandler.getTagValue(stepnode, "complexTypeString");
            currentRestructNumber = Integer.parseInt(XMLHandler.getTagValue(
                    stepnode, "currentRestructNumber"));
        }
        catch(Exception e)
        {
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

   /* public Object clone()
    {
        Object retval = super.clone();
        return retval;
    }*/

    @Override
    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep) throws KettleException 
    { 
        try
        {
            rep.saveStepAttribute(idTransformation, idStep, "TableName", tableName);
            rep.saveStepAttribute(idTransformation, idStep, "AggregateLevels", aggregateLevels);
            rep.saveStepAttribute(idTransformation, idStep, "NumberOfCores", numberOfCores);
            rep.saveStepAttribute(idTransformation, idStep, "schemaName", schemaName);
            rep.saveStepAttribute(idTransformation, idStep, "cubeName", cubeName);
            rep.saveStepAttribute(idTransformation, idStep, "highCardinalityDims", highCardinalityDims);
            rep.saveStepAttribute(idTransformation, idStep, "measureCount", measureCount);
            rep.saveStepAttribute(idTransformation, idStep, "dimensionCount", dimensionCount);
            rep.saveStepAttribute(idTransformation, idStep, "complexDimsCount", complexDimsCount);
            rep.saveStepAttribute(idTransformation, idStep, "complexTypeString", complexTypeString);
            rep.saveStepAttribute(idTransformation, idStep, "currentRestructNumber", 
                    currentRestructNumber);
        }
        catch(Exception e)
        {
            throw new KettleException(BaseMessages.getString(pkg,
                    "TemplateStep.Exception.UnableToSaveStepInfoToRepository") + idStep, e);
        }

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
            highCardinalityDims=rep.getStepAttributeString(idStep, "highCardinalityDims");
            measureCount = rep.getStepAttributeString(idStep, "measureCount");
            dimensionCount = rep.getStepAttributeString(idStep, "dimensionCount");
            complexDimsCount = rep.getStepAttributeString(idStep, "complexDimsCount");
            complexTypeString = rep.getStepAttributeString(idStep, "complexTypeString");
            currentRestructNumber = (int)rep.getStepAttributeInteger(idStep, 
                    "currentRestructNumber");
        }
        catch(Exception e)
        {
            throw new KettleException(BaseMessages.getString(pkg,
                    "MolapMDKeyStepMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
        }
    }
    
    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans)
    {
        return new MDKeyGenStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
            RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info)
    {
        MolapDataProcessorUtil.checkResult(remarks, stepMeta, input);
    }

    @Override
    public StepDataInterface getStepData()
    {
        return new MDKeyGenStepData();
    }
    
    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String getTableName()
    {
        return tableName;
    }
    
    public void setAggregateLevels(String aggregateLevels)
    {
        this.aggregateLevels = aggregateLevels;
    }

    public String getAggregateLevels()
    {
        return aggregateLevels;
    }
    
    public Map<String, GenericDataType> getComplexTypes() {
		return complexTypes;
	}

	public void setComplexTypes(Map<String, GenericDataType> complexTypes) {
		this.complexTypes = complexTypes;
	}
    
    public String getNumberOfCores()
    {
        return numberOfCores;
    }

    public void setNumberOfCores(String numberOfCores)
    {
        this.numberOfCores = numberOfCores;
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
     * @param measureCount the measureCount to set
     */
    public void setMeasureCount(String measureCount)
    {
        this.measureCount = measureCount;
    }
    
    /**
     * @param dimensionCount the dimensionCount to set
     */
    public void setDimensionCount(String dimensionCount)
    {
    	this.dimensionCount = dimensionCount;
    }
    
    /**
     * @param complexDimsCount the complexDimsCount to set
     */
    public void setComplexDimsCount(String complexDimsCount)
    {
    	this.complexDimsCount = complexDimsCount;
    }

    /**
     * @param complexTypeString the complexTypeString to set
     */
    public void setComplexTypeString(String complexTypeString)
    {
    	this.complexTypeString = complexTypeString;
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
     * @return the measureCount
     */
    public int getMeasureCount()
    {
        return Integer.parseInt(measureCount);
    }
    
    /**
     * @return the dimensionCount
     */
    public int getDimensionCount()
    {
    	return Integer.parseInt(dimensionCount);
    }

    /**
     * @return the complexDimsCount
     */
    public int getComplexDimsCount()
    {
    	return Integer.parseInt(complexDimsCount);
    }

    /**
     * @return the complexTypeString
     */
    public int getComplexTypeString()
    {
    	return Integer.parseInt(complexTypeString);
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
    /**
     * 
     * @return
     */
    public String  getHighCardinalityDims() {
		return highCardinalityDims;
	}

    /**
     * 
     * @param highCardinalityDims
     */
	public void setHighCardinalityDims(String  highCardinalityDims) {
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
    
    public void initialize()
    {
    	complexTypes = getComplexTypesMap(complexTypeString);
    }
    private Map<String,GenericDataType> getComplexTypesMap(String complexTypeString)
    {
    	if(null==complexTypeString)
    	{
    		return new LinkedHashMap<>();
    	}
    	Map<String,GenericDataType> complexTypesMap = new LinkedHashMap<String,GenericDataType>();
    	String[] hierarchies = complexTypeString.split(MolapCommonConstants.SEMICOLON_SPC_CHARACTER);
        for(int i = 0;i < hierarchies.length;i++)
        {
            String[] levels = hierarchies[i].split(MolapCommonConstants.HASH_SPC_CHARACTER);
            String[] levelInfo = levels[0].split(MolapCommonConstants.COLON_SPC_CHARACTER);
			GenericDataType g = levelInfo[1].equals("Array")?
						new ArrayDataType(levelInfo[0], ""):new StructDataType(levelInfo[0], "");
			complexTypesMap.put(levelInfo[0], g);
            for(int j = 1;j < levels.length;j++)
            {
            	levelInfo = levels[j].split(MolapCommonConstants.COLON_SPC_CHARACTER);
				switch(levelInfo[1])
				{
					case "Array" : 
						g.addChildren(new ArrayDataType(levelInfo[0], levelInfo[2]));
						break;
					case "Struct" : 
						g.addChildren(new StructDataType(levelInfo[0], levelInfo[2]));
						break;
					default :
						g.addChildren(new PrimitiveDataType(levelInfo[0], levelInfo[2]));
				}
            }
        }
        return complexTypesMap;
    }
}
