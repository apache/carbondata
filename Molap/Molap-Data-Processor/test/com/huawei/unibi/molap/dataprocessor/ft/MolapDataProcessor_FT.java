/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2012
 * =====================================
 *
 */
package com.huawei.unibi.molap.dataprocessor.ft;

import java.io.File;
import java.util.List;

import junit.framework.TestCase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.HighPriorityTest;
import org.junit.MediumPriorityTest;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.sort.SortRowsMeta;

import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;

/**
 * 
 * @author K00900841
 *
 */
public class MolapDataProcessor_FT extends TestCase
{
    private String kettleProperty;

    @BeforeClass
    public void setUp()
    {
        System.out.println("*********************** Started setup");
        File file = new File("");
        kettleProperty = System.getProperty("KETTLE_HOME");
        System.setProperty("KETTLE_HOME", file.getAbsolutePath() + File.separator + "molapplugins" + File.separator
                + "molapplugins");
        try
        {
            EnvUtil.environmentInit();
            KettleEnvironment.init();
        }
        catch(KettleException e)
        {
            assertTrue(false);
        }
    }
    
    @AfterClass
    public void tearDown()
    {
        System.out.println("*********************** Started tearDown");
        if(null!=kettleProperty)
        System.setProperty("KETTLE_HOME", kettleProperty);
        
        String[] s = new String[1];
        File file = new File ("");
        s[0]= file.getAbsolutePath()+File.separator+"../unibi-solutions";
        try
        {
            MolapUtil.deleteFoldersAndFiles(s);
        }
        catch(MolapUtilException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    @MediumPriorityTest
    @Test
    public void test_MolapDataProcessor_DataLoading_Form_DB_Output_MolapStoreFiles()
    {
        Trans trans = null;
        File file = new File("");
        String graphFile = file.getAbsolutePath() + File.separator + "test" + File.separator + "resources"
                + File.separator + "DATA_FACT_SMALL1.ktr";
        TransMeta transMeta = null;
        try
        {
            transMeta = new TransMeta(graphFile);
        }
        catch(KettleXMLException e)
        {
        }
        transMeta.setFilename(graphFile);
        trans = new Trans(transMeta);
        trans.setVariable("csvInputFilePath", file.getAbsolutePath() + File.separator + "test" + File.separator
                + "resources" + File.separator + "DATA_FACT_SMALL.csv");
        List<StepMeta> stepsMeta = trans.getTransMeta().getSteps();
        for(StepMeta step : stepsMeta)
        {
            if (step.getStepMetaInterface() instanceof SortRowsMeta)
            {
                SortRowsMeta selectValues= (SortRowsMeta)step.getStepMetaInterface();
                selectValues.setDirectory(file.getAbsolutePath() + File.separator + "test" + File.separator
                        + "resources");
                break;
            }
        }
        try
        {
            trans.execute(null);
        }
        catch(KettleException e)
        {
            e.printStackTrace();
        }
        trans.waitUntilFinished();
        
        transMeta = null;
        try
        {
            transMeta = new TransMeta(graphFile);
        }
        catch(KettleXMLException e)
        {
        }
        transMeta.setFilename(graphFile);
        trans = new Trans(transMeta);
        trans.setVariable("csvInputFilePath", file.getAbsolutePath() + File.separator + "test" + File.separator
                + "resources" + File.separator + "DATA_FACT_SMALL.csv");
        stepsMeta = trans.getTransMeta().getSteps();
        for(StepMeta step : stepsMeta)
        {
            if (step.getStepMetaInterface() instanceof SortRowsMeta)
            {
                SortRowsMeta selectValues= (SortRowsMeta)step.getStepMetaInterface();
                selectValues.setDirectory(file.getAbsolutePath() + File.separator + "test" + File.separator
                        + "resources");
                break;
            }
        }
        try
        {
            trans.execute(null);
        }
        catch(KettleException e)
        {
            e.printStackTrace();
        }
        trans.waitUntilFinished();
    }
    @MediumPriorityTest
    @Test
    public void test_MolapDataProcessor_DataLoading_Form_DB_Output_MolapStoreFiles_Merger()
    {
        Trans trans = null;
        File file = new File("");
        String graphFile = file.getAbsolutePath() + File.separator + "test" + File.separator + "resources"
                + File.separator + "CSV.ktr";
        TransMeta transMeta = null;
        try
        {
            transMeta = new TransMeta(graphFile);
        }
        catch(KettleXMLException e)
        {
        }
        transMeta.setFilename(graphFile);
        trans = new Trans(transMeta);
        trans.setVariable("csvInputFilePath", file.getAbsolutePath() + File.separator + "test" + File.separator
                + "resources" + File.separator + "DATA_FACT_SMALL.csv");
        List<StepMeta> stepsMeta = trans.getTransMeta().getSteps();
        for(StepMeta step : stepsMeta)
        {
            if (step.getStepMetaInterface() instanceof SortRowsMeta)
            {
                SortRowsMeta selectValues= (SortRowsMeta)step.getStepMetaInterface();
                selectValues.setDirectory(file.getAbsolutePath() + File.separator + "test" + File.separator
                        + "resources");
                break;
            }
        }
        try
        {
            trans.execute(null);
        }
        catch(KettleException e)
        {
            e.printStackTrace();
        }
        trans.waitUntilFinished();
    }
}
