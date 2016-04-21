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

/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2012
 * =====================================
 */
package org.carbondata.processing.dataprocessor.ft;

import java.io.File;
import java.util.List;

import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.sort.SortRowsMeta;

public class CarbonDataProcessor_FT extends TestCase {
  private String kettleProperty;

  @BeforeClass public void setUp() {
    System.out.println("*********************** Started setup");
    File file = new File("");
    kettleProperty = System.getProperty("KETTLE_HOME");
    System.setProperty("KETTLE_HOME",
        file.getAbsolutePath() + File.separator + "carbonplugins" + File.separator
            + "carbonplugins");
    try {
      EnvUtil.environmentInit();
      KettleEnvironment.init();
    } catch (KettleException e) {
      assertTrue(false);
    }
  }

  @AfterClass public void tearDown() {
    System.out.println("*********************** Started tearDown");
    if (null != kettleProperty) System.setProperty("KETTLE_HOME", kettleProperty);

    String[] s = new String[1];
    File file = new File("");
    s[0] = file.getAbsolutePath() + File.separator + "../unibi-solutions";
    try {
      CarbonUtil.deleteFoldersAndFiles(s);
    } catch (CarbonUtilException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  @Test public void test_CarbonDataProcessor_DataLoading_Form_DB_Output_CarbonStoreFiles() {
    Trans trans = null;
    File file = new File("");
    String graphFile =
        file.getAbsolutePath() + File.separator + "test" + File.separator + "src/test/resources"
            + File.separator + "DATA_FACT_SMALL1.ktr";
    TransMeta transMeta = null;
    try {
      transMeta = new TransMeta(graphFile);
    } catch (KettleXMLException e) {
      throw new RuntimeException(e);
    }
    transMeta.setFilename(graphFile);
    trans = new Trans(transMeta);
    trans.setVariable("csvInputFilePath",
        file.getAbsolutePath() + File.separator + "test" + File.separator + "src/test/resources"
            + File.separator + "DATA_FACT_SMALL.csv");
    List<StepMeta> stepsMeta = trans.getTransMeta().getSteps();
    for (StepMeta step : stepsMeta) {
      if (step.getStepMetaInterface() instanceof SortRowsMeta) {
        SortRowsMeta selectValues = (SortRowsMeta) step.getStepMetaInterface();
        selectValues.setDirectory(file.getAbsolutePath() + File.separator + "test" + File.separator
            + "src/test/resources");
        break;
      }
    }
    try {
      trans.execute(null);
    } catch (KettleException e) {
      e.printStackTrace();
    }
    trans.waitUntilFinished();

    transMeta = null;
    try {
      transMeta = new TransMeta(graphFile);
    } catch (KettleXMLException e) {
      throw new RuntimeException(e);
    }
    transMeta.setFilename(graphFile);
    trans = new Trans(transMeta);
    trans.setVariable("csvInputFilePath",
        file.getAbsolutePath() + File.separator + "test" + File.separator + "src/test/resources"
            + File.separator + "DATA_FACT_SMALL.csv");
    stepsMeta = trans.getTransMeta().getSteps();
    for (StepMeta step : stepsMeta) {
      if (step.getStepMetaInterface() instanceof SortRowsMeta) {
        SortRowsMeta selectValues = (SortRowsMeta) step.getStepMetaInterface();
        selectValues.setDirectory(file.getAbsolutePath() + File.separator + "test" + File.separator
            + "src/test/resources");
        break;
      }
    }
    try {
      trans.execute(null);
    } catch (KettleException e) {
      e.printStackTrace();
    }
    trans.waitUntilFinished();
  }

  @Test public void test_CarbonDataProcessor_DataLoading_Form_DB_Output_CarbonStoreFiles_Merger() {
    Trans trans = null;
    File file = new File("");
    String graphFile =
        file.getAbsolutePath() + File.separator + "test" + File.separator + "src/test/resources"
            + File.separator + "CSV.ktr";
    TransMeta transMeta = null;
    try {
      transMeta = new TransMeta(graphFile);
    } catch (KettleXMLException e) {
      throw new RuntimeException(e);
    }
    transMeta.setFilename(graphFile);
    trans = new Trans(transMeta);
    trans.setVariable("csvInputFilePath",
        file.getAbsolutePath() + File.separator + "test" + File.separator + "src/test/resources"
            + File.separator + "DATA_FACT_SMALL.csv");
    List<StepMeta> stepsMeta = trans.getTransMeta().getSteps();
    for (StepMeta step : stepsMeta) {
      if (step.getStepMetaInterface() instanceof SortRowsMeta) {
        SortRowsMeta selectValues = (SortRowsMeta) step.getStepMetaInterface();
        selectValues.setDirectory(file.getAbsolutePath() + File.separator + "test" + File.separator
            + "src/test/resources");
        break;
      }
    }
    try {
      trans.execute(null);
    } catch (KettleException e) {
      e.printStackTrace();
    }
    trans.waitUntilFinished();
  }
}
