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

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.api.dataloader.DataLoadModel;
import com.huawei.unibi.molap.api.dataloader.SchemaInfo;
import com.huawei.unibi.molap.graphgenerator.AggregateTableComparator;
import com.huawei.unibi.molap.graphgenerator.AggregateTableDerivative;
import com.huawei.unibi.molap.graphgenerator.AggregateTableDerivativeComposite;
import com.huawei.unibi.molap.graphgenerator.AggregateTableSelecter;
import com.huawei.unibi.molap.graphgenerator.AutoAggregateTableSelecter;
import com.huawei.unibi.molap.graphgenerator.GraphGenerator;
import com.huawei.unibi.molap.olap.MolapDef;
import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.schema.metadata.AggregateTable;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapSchemaParser;


/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapAutoAGGGraphGeneratorStep.java
 * Class Description : Step responsible for generating and running the aggregate table graph
 * Class Version 1.0
 */
public class MolapAutoAGGGraphGeneratorStep extends BaseStep implements
        StepInterface
{

    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapAutoAGGGraphGeneratorStep.class.getName());

    /**
     * meta
     */
    private MolapAutoAGGGraphGeneratorMeta meta;

    /**
     * data
     */
    private MolapAutoAGGGraphGeneratorData data;

    /**
     * writeCounter
     */
    private long writeCounter;

//    /**
//     * aggCounter
//     */
//    private int aggCounter;
//
//    /**
//     * numberOfParallelGeneration
//     */
//    private int numberOfParallelGeneration;
//
//    /**
//     * aggregateTable
//     */
//    private String[] aggregateTable;

    /**
     * generator
     */
    private GraphGenerator generator;

//    /**
//     * loadedTableList
//     */
//    private List<String> loadedTableList;
    
    /**
     * aggTableQueue
     */
    private Deque<AggTableInfo> aggTableQueue;

    
    /**
     * 
     * MolapAutoAGGGraphGeneratorStep Constructor to initialise the step
     * 
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     * 
     */
    public MolapAutoAGGGraphGeneratorStep(StepMeta stepMeta,
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
        try
        {
            Object[] row = getRow();
            if(first)
            {
                meta = (MolapAutoAGGGraphGeneratorMeta)smi;
                data = (MolapAutoAGGGraphGeneratorData)sdi;
                if(null != getInputRowMeta())
                {
                    this.data.outputRowMeta = (RowMetaInterface)getInputRowMeta()
                            .clone();
                    this.meta.getFields(data.outputRowMeta, getStepname(),
                            null, null, this);
                }
                first = false;
                setStepConfiguration();
            }
            if(null == row)
            {
                String generateGraph = null;
//                List<String> list = null;
//                while(aggCounter < aggregateTable.length)
//                {
//                    list = new ArrayList<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
//                    if(aggregateTable.length - aggCounter >= numberOfParallelGeneration)
//                    {
//                        for(int i = 0;i < numberOfParallelGeneration;i++)
//                        {
//                            list.add(aggregateTable[aggCounter++]);
//                            writeCounter++;
//                        }
//                    }
//                    else
//                    {
//                        int leftOver = aggregateTable.length - aggCounter;
//                        for(int i = 0;i < leftOver;i++)
//                        {
//                            list.add(aggregateTable[aggCounter++]);
//                            writeCounter++;
//                        }
//                    }
//                  //CHECKSTYLE:OFF    Approval No:V3R8C00_002
//                    try
//                    {
//                        generateGraph = generator.generateGraph(list,
//                                meta.getFactTableName());
//                        loadedTableList.addAll(list);
//                    }
//                    catch(GraphGeneratorException e)
//                    {
//                        throw new KettleException(
//                                "Problem while generating the auto aggregate graph",
//                                e);
//                    }
//                    //CHECKSTYLE:ON
//                    executeAggregateGeneration(generateGraph);
//                }
                
//                int size = aggTableQueue.size();
//                String[] storeLocs = getStorelocs();
                AggTableInfo poll = aggTableQueue.poll();
//                for (String loc : storeLocs)
//				{
//                    String aggTableLoc = loc.replace(meta.getFactTableName(), meta.getAggTables());
//                    if(new File(aggTableLoc).exists())
//                    {
//                        continue;
//                    }
                    generateGraph = generator.generateGraph(
                            meta.getFactTableName(), meta.getFactStoreLocation(), generator.getAllAggTables(),true, meta.getAggTables());
                    
                    LOGGER.info(
                            MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Loaded From Table: " + poll.factTableName + " : " + meta.getAggTables());
                    executeAggregateGeneration(generateGraph);
//				}
                
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Procerssed For Auto Aggregate Table: ");
                String logMessage = "Summary: Molap Fact Reader Step: Read: "
                        + 1 + ": Write: " + writeCounter;
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        logMessage);
                putRow(data.outputRowMeta, new Object[0]);
                setOutputDone();
                return false;
            }
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
        putRow(data.outputRowMeta, new Object[0]);
        return true;
    }
    
//    private String[] getStorelocs()
//    {
//    	String[] loadFolders;
//    	
//    	if(!Boolean.parseBoolean(meta.isAutoMode()))
//    	{
//    		FileType fType = FileType.LOCAL;
//    		
//    		String factStorepath = getFactStoreFilePath(fType);
//    		MolapFile file = FileFactory.getMolapFile(factStorepath, fType);
//            
//    		if(!file.exists())
//            {
//                return new String[0];
//            }
//    		
//            MolapFile[] listFiles = file.listFiles(new MolapFileFilter()
//            {
//                @Override
//                public boolean accept(MolapFile pathname)
//                {
//                    return pathname.getName().startsWith(
//                            MolapCommonConstants.LOAD_FOLDER)&& pathname.getName().endsWith(MolapCommonConstants.FILE_INPROGRESS_STATUS);
//                }
//            });
//            
//            loadFolders = new String[listFiles.length];
//            int count = 0;
//            
//            for(MolapFile loadFile : listFiles)
//            {
//            	loadFolders[count++] = loadFile.getAbsolutePath();
//            }
//    		
//    	}
//    	else
//    	{
//    		String factStorepath = getFactStoreFilePath(FileType.HDFS);
//    		
//    		// Chnage to LOCAL for testing in local
//            MolapFile file = FileFactory.getMolapFile(factStorepath, FileFactory.getFileType(factStorepath));
//            
//            if(!file.exists())
//            {
//            	return new String[0];
//            }
//            MolapFile[] listFiles = file.listFiles( new MolapFileFilter()
//            {
//                @Override
//                public boolean accept(MolapFile path)
//                {
//                    return path.getName().startsWith(
//                            MolapCommonConstants.LOAD_FOLDER)
//                            && !path
//                                    .getName()
//                                    .endsWith(
//                                            MolapCommonConstants.FILE_INPROGRESS_STATUS);
//                }
//            });
//            
//            loadFolders = new String[listFiles.length];
//            int count = 0;
//            
//            for(MolapFile loadFile : listFiles)
//            {
//            	loadFolders[count++] = loadFile.getAbsolutePath();
//            }
//    	}
//    	
//    	return loadFolders;
//    }
//    
//    private String getFactStoreFilePath(FileType fType)
//    {
//    	String localtion;
//    	
//    	if(fType.equals(FileType.HDFS))
//    	{
//    		localtion = MolapCommonConstants.STORE_LOCATION_HDFS;
//    	}
//    	else
//    	{
//    		localtion = MolapCommonConstants.STORE_LOCATION;
//    	}
//    	
//    	MolapProperties instance = MolapProperties.getInstance();
//        
//        String baseStorelocation = instance.getProperty(localtion,
//                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
//                + File.separator + meta.getSchemaName() + File.separator + meta.getCubeName();
//    	int restructFolderNumber = MolapUtil.checkAndReturnNextRestructFolderNumber(baseStorelocation,"RS_");
//
//        String restructFolderlocation = baseStorelocation + File.separator
//                + MolapCommonConstants.RESTRUCTRE_FOLDER + restructFolderNumber
//                + File.separator + meta.getFactTableName();
//
//    	return restructFolderlocation;
//    }
    
    private  class AggTableInfo
    { 
//        private String[] aggTablesName;
        
        private String factTableName;
        
//        private AggregateTable[] aggTables;
    }
    
//    public static void main(String[] args)
//    {
//        Schema loadXML = MolapSchemaParser.loadXML("D:\\SmartCare_ec.xml");
//        Cube cube = MolapSchemaParser.getMondrianCubes(loadXML)[0];
//        AggregateTable[] aggregateTable = MolapSchemaParser.getAggregateTable(cube, loadXML);
//        getAllAggregateTableDetails(aggregateTable,cube);
//        
//    }
    private void getAllAggregateTableDetails(AggregateTable[] aggTables, Cube cube)
    {
        List<AggregateTable> copyOfaggregateTable = new ArrayList<AggregateTable>(Arrays.asList(aggTables));
//        AggregateTable[] aggTable= copyOfaggregateTable.toArray(new AggregateTable[copyOfaggregateTable.size()]);
//        copyOfaggregateTable.addAll(Arrays.asList(aggregateTable));
        Collections.sort(copyOfaggregateTable,
                new AggregateTableComparator());
        AggregateTableSelecter tableSelecter = new AutoAggregateTableSelecter(
                copyOfaggregateTable);
        // tableSelecter.selectAggTableNeedsToGenFromFact(cube);
        tableSelecter.selectTableForAggTableAggregationProcess(
                aggTables, cube);

        AggregateTableDerivative aggregateTableDerivativeMetadata = tableSelecter
                .getAggregateTableDerivativeInstanceForAggEval();
        List<AggregateTableDerivative> listOfChildAggregateTableDerivativeMetadata = new ArrayList<AggregateTableDerivative>(
                10);
        listOfChildAggregateTableDerivativeMetadata
                .add(aggregateTableDerivativeMetadata);
        aggTableQueue = new  ArrayDeque<AggTableInfo>();
        
        evaluateAggregateTableDerivativeAndGenerateGraphs(
                listOfChildAggregateTableDerivativeMetadata,
                MolapSchemaParser.getFactTableName(cube),aggTableQueue);
        
    }
    
    private void evaluateAggregateTableDerivativeAndGenerateGraphs(
            List<AggregateTableDerivative> listOfaggregateTableDerivativeMetadata,
            String factTableName, Deque<AggTableInfo> aggInfoQueue)
    {
        List<String> tableNamesForEvaluation = null;
        List<AggregateTable> tableInstancesForEvaluation = null;
        List<AggregateTableDerivative> listOfChildAggregateTableDerivativeMetadata = null;
        Iterator<AggregateTableDerivative> itrMetadata = listOfaggregateTableDerivativeMetadata
                .iterator();
        while(itrMetadata.hasNext())
        {
            AggregateTableDerivative aggregateTableDerivativeMetadata = itrMetadata
                    .next();
            if(!aggregateTableDerivativeMetadata.getChildrens().isEmpty())
            {
                listOfChildAggregateTableDerivativeMetadata = new ArrayList<AggregateTableDerivative>(
                        10);
                if(aggregateTableDerivativeMetadata instanceof AggregateTableDerivativeComposite)
                {
                    AggregateTableDerivative aggTableDer = null;
                    Iterator<AggregateTableDerivative> itr = aggregateTableDerivativeMetadata
                            .getChildrens().iterator();
                    tableNamesForEvaluation = new ArrayList<String>(10);
                    tableInstancesForEvaluation = new ArrayList<AggregateTable>(
                            10);
                    while(itr.hasNext())
                    {
                        aggTableDer = itr.next();
                        if(null != aggTableDer.getAggregateTable())
                        {
                            tableNamesForEvaluation.add(aggTableDer
                                    .getAggregateTable()
                                    .getAggregateTableName());
                            tableInstancesForEvaluation.add(aggTableDer
                                    .getAggregateTable());
                        }
                        listOfChildAggregateTableDerivativeMetadata
                                .add(aggTableDer);

                    }
                    if(null != aggregateTableDerivativeMetadata
                            .getAggregateTable())
                    {

                        AggTableInfo aggTableInfo= new AggTableInfo();
//                        aggTableInfo.aggTables= tableInstancesForEvaluation
//                                .toArray(new AggregateTable[tableInstancesForEvaluation.size()]);
                        aggTableInfo.factTableName= aggregateTableDerivativeMetadata
                                .getAggregateTable()
                                .getAggregateTableName();
//                        aggTableInfo.aggTablesName=tableNamesForEvaluation.toArray(new String[tableNamesForEvaluation.size()]);
                        aggInfoQueue.offer(aggTableInfo);
                        
                    }
                    else
                    {

                        AggTableInfo aggTableInfo= new AggTableInfo();
//                        aggTableInfo.aggTables= tableInstancesForEvaluation
//                                .toArray(new AggregateTable[tableInstancesForEvaluation.size()]);
                        aggTableInfo.factTableName= factTableName;
//                        aggTableInfo.aggTablesName=tableNamesForEvaluation.toArray(new String[tableNamesForEvaluation.size()]);
                        aggInfoQueue.offer(aggTableInfo);
                    }
                    if(!listOfChildAggregateTableDerivativeMetadata.isEmpty())
                    {
                        evaluateAggregateTableDerivativeAndGenerateGraphs(
                                listOfChildAggregateTableDerivativeMetadata,
                                factTableName,aggInfoQueue);
                    }

                }
            }
        }

    }
    /**
     * Below method will be used to set the step configuration
     * @throws Exception 
     */
    private void setStepConfiguration() throws Exception
    {
//        numberOfParallelGeneration = 0;
//        try
//        {
//            numberOfParallelGeneration = Integer
//                    .parseInt(MolapProperties
//                            .getInstance()
//                            .getProperty(
//                                    MolapCommonConstants.MOLAP_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION,
//                                    MolapCommonConstants.MOLAP_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION_DEFAULT));
//        }
//        catch(NumberFormatException e)
//        {
//            numberOfParallelGeneration = 1;
//        }
//        aggregateTable = meta.getAggTables();
        DataLoadModel model = new DataLoadModel();
        SchemaInfo info = new SchemaInfo();
        info.setSchemaName(meta.getSchemaName());
        info.setCubeName(meta.getCubeName());
        info.setSchemaPath(null);
        info.setAutoAggregateRequest(true);
        model.setSchemaInfo(info);
        model.setTableName(meta.getAggTables());
        generator = new GraphGenerator(model,meta.isHDFSMode(), meta.getPartitionId(), parseStringToSchema(meta.getSchema()), meta.getFactStoreLocation(), meta.getCurrentRestructNumber(), 1);
//        loadedTableList = new ArrayList<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        getAllAggregateTableDetails(generator.getAllAggTables(), generator.getCube());
    }
    
    private Schema parseStringToSchema(String schema) throws Exception 
    {
        Parser xmlParser = XOMUtil.createDefaultParser();
        ByteArrayInputStream baoi = new ByteArrayInputStream(schema.getBytes(Charset.defaultCharset()));
        return new MolapDef.Schema(xmlParser.parse(baoi));
    }

    /**
     * Below method will be used to to run aggregate table graph
     * @param graphFilePath
     * @throws KettleException
     */
    private void executeAggregateGeneration(String graphFilePath)
            throws KettleException
    {
        Trans trans = null;

        TransMeta transMeta = null;
        try
        {
            transMeta = new TransMeta(graphFilePath);
        }
        catch(KettleXMLException e)
        {
            throw new KettleException("Problem while creating the trans", e);
        }
        transMeta.setFilename(graphFilePath);
        trans = new Trans(transMeta);
        try
        {
            trans.execute(null);
        }
        catch(KettleException e)
        {
            throw new KettleException(
                    "Problem while running the auto aggregate graph For Auto aggregation",
                    e);
        }
        trans.waitUntilFinished();
        if((trans.getErrors() > 0) && (trans.isStopped()))
        {
            throw new KettleException(
                    "Problem while running the auto aggregate graph For Auto aggregation");
        }
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
        meta = (MolapAutoAGGGraphGeneratorMeta)smi;
        data = (MolapAutoAGGGraphGeneratorData)sdi;
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
        meta = (MolapAutoAGGGraphGeneratorMeta)smi;
        data = (MolapAutoAGGGraphGeneratorData)sdi;
        super.dispose(smi, sdi);
    }

}
