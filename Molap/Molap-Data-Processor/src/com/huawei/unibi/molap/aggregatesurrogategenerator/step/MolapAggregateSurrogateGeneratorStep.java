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
package com.huawei.unibi.molap.aggregatesurrogategenerator.step;

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
import com.huawei.unibi.molap.aggregatesurrogategenerator.AggregateSurrogateGenerator;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapAggregateSurrogateGeneratorStep.java
 * Class Description : Step class is generating surrogate keys for aggregate tables
 * Class Version 1.0
 */
public class MolapAggregateSurrogateGeneratorStep extends BaseStep implements
        StepInterface
{

    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapAggregateSurrogateGeneratorStep.class.getName());

    /**
     * BYTE ENCODING
     */
    private static final String BYTE_ENCODING = "ISO-8859-1";

    /**
     * meta
     */
    private MolapAggregateSurrogateGeneratorMeta meta;

    /**
     * data
     */
    private MolapAggregateSurrogateGeneratorData data;

    /**
     * rowCounter
     */
    private long readCounter;

    /**
     * writeCounter
     */
    private long writeCounter;

    /**
     * logCounter
     */
    private int logCounter;

    /**
     * aggregateRecordIterator
     */
    private AggregateSurrogateGenerator aggregateRecordIterator;

    /**
     * 
     * MolapAggregateSurrogateGeneratorStep Constructor to initialize the step
     * 
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     * 
     */
    public MolapAggregateSurrogateGeneratorStep(StepMeta stepMeta,
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
            Object[] factTuple = getRow();
            if(first)
            {
                meta = (MolapAggregateSurrogateGeneratorMeta)smi;
                data = (MolapAggregateSurrogateGeneratorData)sdi;
                meta.initialize();
                first = false;
                if(null != getInputRowMeta())
                {
                    this.data.outputRowMeta = (RowMetaInterface)getInputRowMeta()
                            .clone();
                    this.meta.getFields(data.outputRowMeta, getStepname(),
                            null, null, this);
                    setStepOutputInterface(meta.isMdkeyInOutRowRequired());
                }
                
                String[] aggLevels = meta.getAggregateLevels(); 
                String[] factLevels = meta.getFactLevels();
                int[] cardinality = meta.getFactDimLens();
                
                int[] aggCardinality = new int[aggLevels.length];
                Arrays.fill(aggCardinality, -1);
                
                for(int i = 0; i < aggLevels.length; i++)
                {
                    for(int j = 0; j < factLevels.length; j++)
                    {
                        if(aggLevels[i].equals(factLevels[j]))
                        {      //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_001
                        	aggCardinality[i] = cardinality[j];
                            break;
                        }// CHECKSTYLE:ON
                    }
                }
//                meta.setAggDimeLens(aggCardinality);
                this.aggregateRecordIterator = new AggregateSurrogateGenerator(
                		factLevels, aggLevels,
                        meta.getFactMeasure(), meta.getAggregateMeasures(),
                        meta.isMdkeyInOutRowRequired(), aggCardinality);
                
                this.logCounter = Integer
                        .parseInt(MolapCommonConstants.DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER);
//                createStoreAndWriteSliceMetadata(meta.isManualAutoAggRequest(), factTuple, aggCardinality);
            }
            if(null == factTuple)
            {
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Procerssed For table: " + meta.getTableName());
                String logMessage = "Summary: Molap Auto Aggregate Generator Step: Read: "
                        + readCounter + ": Write: " + writeCounter;
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        logMessage);
                setOutputDone();
                return false;
            }
            readCounter++;
            putRow(data.outputRowMeta,
                    this.aggregateRecordIterator.generateSurrogate(factTuple));
            writeCounter++;
            if(readCounter % logCounter == 0)
            {
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Record Procerssed For table: " + meta.getTableName());
                String logMessage = "Molap Auto Aggregate Generator Step: Read: "
                        + readCounter + ": Write: " + writeCounter;
                LOGGER.info(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        logMessage);
            }
        }
        catch(Exception ex)
        {
            LOGGER.error(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex);
            throw new RuntimeException(ex);
        }
        return true;
    }
    
//    /**
//     * Below method will be used to create the load folder and write the slice meta data for aggregate table
//     * @throws KettleException
//     */
//    private void createStoreAndWriteSliceMetadata(boolean deleteExistingStore, Object[] isFirstIsNull, int[] aggCardinality) throws KettleException
//    {
//        String createStoreLocaion = MolapDataProcessorUtil.createStoreLocaion(
//                meta.getSchemaName(), meta.getCubeName(),
//                meta.getTableName(),deleteExistingStore);
//        
//        updateAndWriteSliceMetadataFile(createStoreLocaion);
//        writeAggLevelCardinalityFile(aggCardinality, createStoreLocaion);
        
//        if(null==isFirstIsNull)
//        {
//        	return;
//        }
//        
//        MeasureMetaDataModel msrModel = null;
//        msrModel = MolapDataProcessorUtil.getMeasureModelForManual(meta.getFactStorePath(),
//					meta.getFactTableName(), meta.getFactMeasure().length, FileFactory.getFileType(meta.getFactStorePath()));
//
//		String metaDataFileName = MolapCommonConstants.MEASURE_METADATA_FILE_NAME
//				+ meta.getTableName()
//				+ MolapCommonConstants.MEASUREMETADATA_FILE_EXT;
//		String measureMetaDataFileLocation = createStoreLocaion
//				+ metaDataFileName;
//		
//		
//		int[] measureIndex = new int[meta.getAggregateMeasures().length];
//		Arrays.fill(measureIndex, -1);
//		
//		for (int i = 0; i < meta.getAggregateMeasures().length - 1; i++) 
//		{
//			for (int j = 0; j < meta.getFactMeasure().length; j++) 
//			{
//				if (meta.getAggregateMeasures()[i]
//						.equals(meta.getFactMeasure()[j])) 
//				{
//					measureIndex[i] = j;
//					break;
//				}
//			}
//		}
//		measureIndex[measureIndex.length - 1] = measureIndex[0];
//		
//		double[] minValue = new double[measureIndex.length];
//		
//		for (int i = 0; i < measureIndex.length-1; i++)
//		{
//			minValue[i]=msrModel.getMinValue()[measureIndex[i]];
//		}
//		
//		minValue[minValue.length-1]=1;
//		
//		try 
//		{
//			MolapDataProcessorUtil.writeMeasureMetaDataToFile(
//					new double[measureIndex.length], minValue,
//					new int[measureIndex.length], new double[measureIndex.length],
//					new char[measureIndex.length], new byte[measureIndex.length],
//					measureMetaDataFileLocation);
//		} 
//		catch (MolapDataProcessorException e) 
//		{
//			LOGGER.error(
//					MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e);
//		}
//    }
    
//    /**
//	 * This method writes aggregate level cardinality of each agg level to a file
//	 * 
//	 * @param dimCardinality
//	 * @param storeLocation
//	 * @throws KettleException
//	 * 
//	 * @author Suprith T 72079
//	 */
//    private void writeAggLevelCardinalityFile(int[] dimCardinality, String storeLocation) throws KettleException
//    {
//		String aggLevelCardinalityFilePath = storeLocation + File.separator + 
//				MolapCommonConstants.LEVEL_METADATA_FILE + meta.getTableName() + ".metadata";
//		
//    	FileOutputStream fileOutputStream = null;
//    	FileChannel channel = null;
//    	try
//        {
//			int dimCardinalityArrLength = dimCardinality.length;
//			
//			// first four bytes for writing the length of array, remaining for array data
//			ByteBuffer buffer = ByteBuffer
//					.allocate(MolapCommonConstants.INT_SIZE_IN_BYTE
//							+ dimCardinalityArrLength * MolapCommonConstants.INT_SIZE_IN_BYTE);
//			
//			fileOutputStream = new FileOutputStream(aggLevelCardinalityFilePath);
//			channel = fileOutputStream.getChannel();
//			buffer.putInt(dimCardinalityArrLength);
//			
//			for (int i = 0; i < dimCardinalityArrLength; i++)
//			{
//				buffer.putInt(dimCardinality[i]);
//			}
//			
//			buffer.flip();
//			channel.write(buffer);
//			buffer.clear();
//        }
//        catch(IOException e)
//        {
//            throw new KettleException("Not able to write level cardinality file", e);
//        }
//        finally
//        {
//            MolapUtil.closeStreams(channel, fileOutputStream);
//        }
//    }

//    /**
//     * Below method will be used to update and write the slice meta data
//     * 
//     * @throws KettleException
//     */
//    private void updateAndWriteSliceMetadataFile(String path)
//            throws KettleException
//    {
//        File file = new File(path);
//        String sliceMetaDataFilePath = file.getParentFile().getAbsolutePath()
//                + File.separator + MolapCommonConstants.SLICE_METADATA_FILENAME;
//
//        SliceMetaData sliceMetaData = new SliceMetaData();
//        sliceMetaData.setDimensions(meta.getAggregateLevels());
//        sliceMetaData.setActualDimensions(meta.getAggregateLevels());
//        sliceMetaData.setMeasures(meta.getAggregateMeasuresColumnName());
//        sliceMetaData.setActualDimLens(meta.getAggDimeLens());
//        sliceMetaData.setDimLens(meta.getAggDimeLens());
//        String[] aggregators = meta.getAggregators();
//        sliceMetaData.setMeasuresAggregator(aggregators);
//        sliceMetaData.setHeirAnKeySize(meta.getHeirAndKeySize());
//        sliceMetaData.setTableNamesToLoadMandatory(null);
//        int measureOrdinal = 0;
//        //CHECKSTYLE:OFF    Approval No:Approval-367
//        for(String agg : aggregators)
//        {     //CHECKSTYLE:ON
//            if("count".equals(agg))
//            {
//                break;
//            }
//            measureOrdinal++;
//        }
//        sliceMetaData.setCountMsrOrdinal(measureOrdinal);
//        sliceMetaData.setHeirAndDimLens(meta.getHeirAndDimLens());
//        sliceMetaData.setKeyGenerator(KeyGeneratorFactory.getKeyGenerator(meta
//                .getAggDimeLens()));
//        MolapDataProcessorUtil.writeFileAsObjectStream(sliceMetaDataFilePath, sliceMetaData);
//    }

//    /**
//     * Below method will be used to create the store
//     * @param schemaName
//     * @param cubeName
//     * @param tableName
//     * @return store location
//     * @throws KettleException
//     */
//    private String createStoreLocaion(String schemaName, String cubeName,
//            String tableName,boolean deleteExistingStore) throws KettleException
//    {
//        String baseStorePath = MolapProperties.getInstance().getProperty(
//                MolapCommonConstants.STORE_LOCATION,
//                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
//        baseStorePath = baseStorePath + File.separator + schemaName
//                + File.separator + cubeName;
//        int restrctFolderCount = MolapUtil
//                .checkAndReturnNextRestructFolderNumber(baseStorePath, "RS_");
//        if(restrctFolderCount == -1)
//        {
//            restrctFolderCount = 0;
//        }
//        String baseStorePathWithTableName = baseStorePath + File.separator
//                + MolapCommonConstants.RESTRUCTRE_FOLDER + restrctFolderCount
//                + File.separator + tableName;
//        if(deleteExistingStore)
//        {
//            File file = new File(baseStorePathWithTableName);
//            if(file.exists())
//            {
//                try
//                {
//                    MolapUtil.deleteFoldersAndFiles(file);
//                }
//                catch(MolapUtilException e)
//                {
//                   throw new KettleException("Problem while deleting the existing aggregate table data in case of Manual Aggregation");
//                }
//            }
//        }
//        int counter = MolapUtil
//                .checkAndReturnNextRestructFolderNumber(baseStorePathWithTableName,"Load_");
//        counter++;
//        String basePath = baseStorePathWithTableName + File.separator
//                + MolapCommonConstants.LOAD_FOLDER + counter;
//        if(new File(basePath).exists())
//        {
//            counter++;
//        }
//        basePath = baseStorePathWithTableName + File.separator
//                + MolapCommonConstants.LOAD_FOLDER + counter
//                + MolapCommonConstants.FILE_INPROGRESS_STATUS;
//        boolean isDirCreated = new File(basePath).mkdirs();
//        if(!isDirCreated)
//        {
//            throw new KettleException("Unable to create dataload directory"
//                    + basePath);
//        }
//        return basePath;
//    }

    /**
     * This method will be used for setting the output interface. Output
     * interface is how this step will process the row to next step
     * 
     * @param dimLens
     *            number of dimensions
     * 
     */
    private void setStepOutputInterface(boolean isMdkeyRequiredInOutRow)
    {
        String[] aggregateMeasures = meta.getAggregateMeasures();
        int size=aggregateMeasures.length
                + 1;
        
        if(isMdkeyRequiredInOutRow)
        {
            size+=1;
        }
        ValueMetaInterface[] out = new ValueMetaInterface[size];
        int l = 0;
        ValueMetaInterface valueMetaInterface = null;
        ValueMetaInterface storageMetaInterface = null;
        for(int i = 0;i < aggregateMeasures.length;i++)
        {
            valueMetaInterface = new ValueMeta(aggregateMeasures[i],
                    ValueMetaInterface.TYPE_NUMBER,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL);
            storageMetaInterface = new ValueMeta(aggregateMeasures[i],
                    ValueMetaInterface.TYPE_NUMBER,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL);
            valueMetaInterface.setStorageMetadata(storageMetaInterface);
            out[l++] = valueMetaInterface;

        }
        valueMetaInterface = new ValueMeta("id",
                ValueMetaInterface.TYPE_BINARY,
                ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
        valueMetaInterface.setStorageMetadata((new ValueMeta("id",
                ValueMetaInterface.TYPE_STRING,
                ValueMetaInterface.STORAGE_TYPE_NORMAL)));
        valueMetaInterface.getStorageMetadata().setStringEncoding(
                BYTE_ENCODING);
        valueMetaInterface.setStringEncoding(BYTE_ENCODING);
        out[l++] = valueMetaInterface;
        if(isMdkeyRequiredInOutRow)
        {
            valueMetaInterface = new ValueMeta("factMdkey",
                    ValueMetaInterface.TYPE_BINARY,
                    ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
            valueMetaInterface.setStorageMetadata((new ValueMeta("factMdkey",
                    ValueMetaInterface.TYPE_STRING,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL)));
            valueMetaInterface.setStringEncoding(BYTE_ENCODING);
            valueMetaInterface.setStringEncoding(BYTE_ENCODING);
            valueMetaInterface.getStorageMetadata()
                    .setStringEncoding(BYTE_ENCODING);
            out[l] = valueMetaInterface;
        }

        data.outputRowMeta.setValueMetaList(Arrays.asList(out));
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
        meta = (MolapAggregateSurrogateGeneratorMeta)smi;
        data = (MolapAggregateSurrogateGeneratorData)sdi;
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
        meta = (MolapAggregateSurrogateGeneratorMeta)smi;
        data = (MolapAggregateSurrogateGeneratorData)sdi;
        super.dispose(smi, sdi);
    }
}
