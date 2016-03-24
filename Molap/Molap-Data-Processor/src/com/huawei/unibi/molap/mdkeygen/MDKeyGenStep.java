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

package com.huawei.unibi.molap.mdkeygen;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.huawei.datasight.molap.datatypes.GenericDataType;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.iweb.platform.logging.impl.StandardLogService;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.file.manager.composite.FileData;
import com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite;
import com.huawei.unibi.molap.file.manager.composite.LoadFolderData;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.store.MolapFactDataHandlerColumnar;
import com.huawei.unibi.molap.store.MolapFactHandler;
import com.huawei.unibi.molap.store.SingleThreadFinalSortFilesMerger;
import com.huawei.unibi.molap.store.writer.exception.MolapDataWriterException;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;
import com.huawei.unibi.molap.util.RemoveDictionaryUtil;
import com.huawei.unibi.molap.vo.HybridStoreModel;

/**
 * Project Name 	: Carbon 
 * Module Name 		: MOLAP Data Processor
 * Author 			: Suprith T 72079 
 * Created Date 	: 25-Aug-2015
 * FileName 		: MDKeyGenStep.java
 * Description 		: Kettle step to generate MD Key
 * Class Version 	: 1.0
 */
public class MDKeyGenStep extends BaseStep 
{
    private static final LogService LOGGER = LogServiceFactory
          .getLogService(MDKeyGenStep.class.getName());
    
    /**
     * molap mdkey generator step data class
     */
    private MDKeyGenStepData data;
    
    /**
     *  molap mdkey generator step meta
     */
    private MDKeyGenStepMeta meta;

    /**
     * dimension length
     */
    private int dimensionCount;
    
    /**
     * table name
     */
    private String tableName;
    
    /**
     * File manager
     */
    private IFileManagerComposite fileManager;
    
    private Map<Integer,GenericDataType> complexIndexMap;
    
    /**
     * readCounter
     */
    private long readCounter;
    
    /**
     * writeCounter
     */
    private long writeCounter;
    
    private int measureCount;
    
    private String dataFolderLocation;
    
    private SingleThreadFinalSortFilesMerger finalMerger;
    
    /**
     * dataHandler
     */
    private MolapFactHandler dataHandler;

	private HybridStoreModel hybridStoreModel;
    
    /**
     * MolapMDKeyGenStep
     * 
     * @param stepMeta
     * @param stepDataInterface
     * @param copyNr
     * @param transMeta
     * @param trans
     *
     */
    public MDKeyGenStep(StepMeta stepMeta,
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
//        try
//        {
            meta = (MDKeyGenStepMeta)smi;
            StandardLogService.setThreadName(StandardLogService.getPartitionID(meta.getCubeName()), null);
            data = (MDKeyGenStepData)sdi;
            
            meta.initialize();
            Object[] row = getRow();
            if(first)
            {
                first = false;
                
              //  data.rowMeta = getInputRowMeta();

                data.outputRowMeta = new RowMeta();
                boolean isExecutionRequired = setStepConfiguration();
                
                if(!isExecutionRequired)
                {
                    processingComplete();
                	return false;
                }
                setStepOutputInterface();
            }
            
            readCounter++;
            
            if(null != row)
            {
            	putRow(data.outputRowMeta, new Object[measureCount + 1]);
            	return true;
            }
            
            if(null!=dataHandler && null!=finalMerger)
            {
				try 
				{
					dataHandler.initialise();
					finalMerger.startFinalMerge();
					while (finalMerger.hasNext()) 
					{
						Object[] r = finalMerger.next();
						Object[] outputRow = process(r);
						dataHandler.addDataToStore(outputRow);
						writeCounter++;
					}
				}
				catch (MolapDataWriterException e)
				{
					 LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, "Failed for: "+ this.tableName);
					throw new KettleException(
							"Error while initializing data handler : "
									+ e.getMessage());
				}
				finally 
				{
					try {
						dataHandler.finish();
					}
                    catch (MolapDataWriterException e) {
						
						LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG ,"Error in  closing data handler ");
					}
					
				}
			}
            

            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Record Procerssed For table: "+ this.tableName);
            String logMessage= "Finished Molap Mdkey Generation Step: Read: " + readCounter + ": Write: "+ writeCounter;
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, logMessage);
            processingComplete();
//        }
//        catch(Exception ex)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, ex);
//            throw new RuntimeException(ex);
//        }
        return false;
    }
    
    private void processingComplete()
    {
        if(null != dataHandler)
        {
            dataHandler.closeHandler();
        }
        setOutputDone();
    }

    /**
     * This method will be used to get and update the step properties which will
     * required to run this step
     * @throws MolapUtilException 
     * 
     */
    private boolean setStepConfiguration()
    {
        this.tableName = meta.getTableName();
        MolapProperties instance = MolapProperties.getInstance();
        String tempLocationKey = meta.getSchemaName()+'_'+meta.getCubeName();
        String baseStorelocation = instance.getProperty(
                tempLocationKey,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
                + File.separator + meta.getSchemaName() + File.separator + meta.getCubeName();
        
        int restructFolderNumber = meta.getCurrentRestructNumber()/*MolapUtil.checkAndReturnNextRestructFolderNumber(baseStorelocation,"RS_")*/;

        String restructFolderlocation = baseStorelocation + File.separator
                + MolapCommonConstants.RESTRUCTRE_FOLDER + restructFolderNumber
                + File.separator + this.tableName;

        int counter = MolapUtil
                .checkAndReturnCurrentLoadFolderNumber(restructFolderlocation);
        
        // This check is just to get the absolute path because from the property file Relative path 
        // will come and sometimes FileOutPutstream was not able to Create the file.
        File file = new File(restructFolderlocation);
        String storeLocation = file.getAbsolutePath() + File.separator
                + MolapCommonConstants.LOAD_FOLDER + counter;
        
        fileManager = new LoadFolderData();
        fileManager.setName(MolapCommonConstants.LOAD_FOLDER + counter
                + MolapCommonConstants.FILE_INPROGRESS_STATUS);
        
        storeLocation = storeLocation
                + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        
        if(!(new File(storeLocation).exists()))
        {
              LOGGER.error(
                      MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                      "Load Folder Not Present for writing measure metadata  : "
                              + storeLocation);
              return false;
        }
        
        this.meta.setHighCardinalityCount(RemoveDictionaryUtil.extractHighCardCount(this.meta.getHighCardinalityDims()));
        
        String levelCardinalityFilePath = storeLocation + File.separator + 
				MolapCommonConstants.LEVEL_METADATA_FILE + meta.getTableName() + ".metadata";
        
        int[] dimLens = null;
		try {
			int[] dimLensWithComplex = MolapUtil.getCardinalityFromLevelMetadataFile(levelCardinalityFilePath);
			List<Integer> dimsLenList = new ArrayList<Integer>();
			for(int eachDimLen : dimLensWithComplex)
			{
				if(eachDimLen != 0)
					dimsLenList.add(eachDimLen);
			}
			dimLens = new int[dimsLenList.size()];
			for(int i=0;i<dimsLenList.size();i++)
			{
				dimLens[i] = dimsLenList.get(i);
			}
		} catch (MolapUtilException e) {
			LOGGER.error(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Level cardinality file :: " + e.getMessage());
			return false;
		}
		String[] dimStoreType = meta.getDimensionsStoreType().split(",");
		boolean[] dimensionStoreType = new boolean[dimLens.length];
		for(int i=0;i<dimensionStoreType.length;i++)
		{
				dimensionStoreType[i]=Boolean.parseBoolean(dimStoreType[i]);
		}
		this.hybridStoreModel = MolapUtil.getHybridStoreMeta(dimLens,
					dimensionStoreType,null);
		dimLens=hybridStoreModel.getHybridCardinality();  
		data.generator = new KeyGenerator[dimLens.length + 1];
		for(int i=0;i<dimLens.length;i++)
		{
			data.generator[i] = KeyGeneratorFactory.getKeyGenerator(new int[]{dimLens[i]});
		}
        
//      this.dimensionCount = dimLens.length;
      this.dimensionCount = meta.getDimensionCount();
      
      int simpleDimsCount = this.dimensionCount - meta.getComplexDimsCount()-meta.getHighCardinalityCount();
      int[] simpleDimsLen = new int[simpleDimsCount];
      for(int i=0;i<simpleDimsCount;i++)
      {
      	simpleDimsLen[i] = dimLens[i];
      }
     	
      //Actual primitive dimension used to generate start & end key 
     
      //data.generator[dimLens.length] = KeyGeneratorFactory.getKeyGenerator(simpleDimsLen);
      data.generator[dimLens.length] = KeyGeneratorFactory.getKeyGenerator(hybridStoreModel.getHybridCardinality(),hybridStoreModel.getDimensionPartitioner());
      
      //To Set MDKey Index of each primitive type in complex type 
      int surrIndex = simpleDimsCount;
      Iterator<Entry<String,GenericDataType>> complexMap = meta.getComplexTypes().entrySet().iterator();
      complexIndexMap = new HashMap<Integer,GenericDataType>(meta.getComplexDimsCount());
      while(complexMap.hasNext())
      {
          Entry<String,GenericDataType> complexDataType = complexMap.next();
          complexDataType.getValue().setOutputArrayIndex(0);
          complexIndexMap.put(simpleDimsCount, complexDataType.getValue());
          simpleDimsCount++;
          List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
          complexDataType.getValue().getAllPrimitiveChildren(primitiveTypes);
          for(GenericDataType eachPrimitive : primitiveTypes)
          {
          	eachPrimitive.setSurrogateIndex(surrIndex++);
          }
      }
      
        
        this.measureCount = meta.getMeasureCount();

        String metaDataFileName = MolapCommonConstants.MEASURE_METADATA_FILE_NAME
                + this.tableName + MolapCommonConstants.MEASUREMETADATA_FILE_EXT + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        
        FileData fileData = new FileData(metaDataFileName, storeLocation);
        fileManager.add(fileData);
        
        // Set the data file location
        this.dataFolderLocation = baseStorelocation + File.separator +
        		MolapCommonConstants.SORT_TEMP_FILE_LOCATION + File.separator + this.tableName;
        
        /*finalMerger = new SingleThreadFinalSortFilesMerger(dataFolderLocation,
    			tableName, dimensionCount, measureCount,meta.getHighCardinalityDims().length);*/
        finalMerger = new SingleThreadFinalSortFilesMerger(dataFolderLocation,
                tableName, dimensionCount - meta.getComplexDimsCount()-meta.getHighCardinalityCount(), meta.getComplexDimsCount(), measureCount,meta.getHighCardinalityCount());
        if(meta.getHighCardinalityCount() > 0 || meta.getComplexDimsCount() > 0)
        {
            dataHandler = new MolapFactDataHandlerColumnar(
                    meta.getSchemaName(), meta.getCubeName(), this.tableName,
                    false, measureCount, data.generator[dimLens.length].getKeySizeInBytes(),
                    measureCount + 1, null, null, storeLocation, dimLens,
                    false, false, dimLens, null, null, true,
                    meta.getCurrentRestructNumber(),
                    meta.getHighCardinalityCount(), dimensionCount, complexIndexMap, simpleDimsLen,this.hybridStoreModel);
        }
        else
        {
            dataHandler = new MolapFactDataHandlerColumnar(
                    meta.getSchemaName(), meta.getCubeName(), this.tableName,
                    false, measureCount, data.generator[dimLens.length].getKeySizeInBytes(),
                    measureCount, null, null, storeLocation, dimLens,
                    false, false, dimLens, null, null, true,
                    meta.getCurrentRestructNumber(),
                    meta.getHighCardinalityCount(), dimensionCount, complexIndexMap, simpleDimsLen,this.hybridStoreModel);
        }
        return true;
    }

    /**
     * This method will be used for setting the output interface.
     * Output interface is how this step will process the row to next step  
     */
    private void setStepOutputInterface()
    {
        ValueMetaInterface[] out = new ValueMetaInterface[measureCount + 1];
        
        for(int i =0; i < measureCount; i++)
        {
            out[i] = new ValueMeta("measure" + i, ValueMetaInterface.TYPE_NUMBER,
                    ValueMetaInterface.STORAGE_TYPE_NORMAL);
            out[i].setStorageMetadata(new ValueMeta("measure" + i,
                    ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.STORAGE_TYPE_NORMAL));
        }
        
        out[out.length - 1] = new ValueMeta("id", ValueMetaInterface.TYPE_BINARY,
                ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
        out[out.length - 1].setStorageMetadata(new ValueMeta("id",
                ValueMetaInterface.TYPE_STRING, ValueMetaInterface.STORAGE_TYPE_NORMAL));
        out[out.length - 1].setLength(256);
        out[out.length - 1].setStringEncoding(MolapCommonConstants.BYTE_ENCODING);
        out[out.length - 1].getStorageMetadata().setStringEncoding(MolapCommonConstants.BYTE_ENCODING);

        data.outputRowMeta.setValueMetaList(Arrays.asList(out));
    }

    /**
     * 
     * This method will be used to get the row from previous step and then it
     * will generate the mdkey and then send the mdkey to next step
     * 
     * @param row
     *          input row
     * @param outputrow
     *          output row
     *      
     * @throws KettleException
     * 
     */
    private Object[] process(Object[] row) throws KettleException
    {
        Object[] outputRow = null;
        // adding one for the high cardinality dims byte array.
        if(meta.getHighCardinalityCount() > 0 || meta.getComplexDimsCount() > 0)
        {
        	outputRow = new Object[measureCount + 1 + 1];
        }
        else
        {
            outputRow = new Object[measureCount + 1];
        }
        int[] keys = new int[this.dimensionCount];

        int l = 0;
        int index = 0;
        for(int i = 0; i < measureCount; i++)
        {
//            if(null != row[i])
//            {
            
            	outputRow[l++] = (Double)RemoveDictionaryUtil.getMeasure(index++, row);
//            }
        }
        outputRow[l] =  RemoveDictionaryUtil.getByteArrayForNoDictionaryCols(row);
        
        //copy all columnar dimension to key array
        int[] columnarStoreOrdinals=hybridStoreModel.getColumnStoreOrdinals();
        int[] columnarDataKeys=new int[columnarStoreOrdinals.length];
        for(int i=0;i<columnarStoreOrdinals.length;i++)
        {
        	Object key = RemoveDictionaryUtil.getDimension(columnarStoreOrdinals[i], row);
        	columnarDataKeys[i]=(Integer)key;
        }
        //copy all row dimension in row key array
        int[] rowStoreOrdinals=hybridStoreModel.getRowStoreOrdinals();
        int[] rowDataKeys = new int[rowStoreOrdinals.length];
        for(int i=0;i<rowStoreOrdinals.length;i++)
        {
        	Object key=RemoveDictionaryUtil.getDimension(rowStoreOrdinals[i], row);
        	rowDataKeys[i]=(Integer)key;
        }
        try
        {
        	int[] completeKeys=new int[columnarDataKeys.length+rowDataKeys.length];
        	System.arraycopy(rowDataKeys, 0, completeKeys, 0, rowDataKeys.length);
        	System.arraycopy(columnarDataKeys, 0, completeKeys, rowDataKeys.length, columnarDataKeys.length);
        	outputRow[outputRow.length-1]=data.generator[data.generator.length-1].generateKey(completeKeys);
        }
        catch(KeyGenException e)
        {
        	throw new KettleException("Unbale to generate the mdkey", e);	
        }
        
        return outputRow;
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
        meta = (MDKeyGenStepMeta)smi;
        data = (MDKeyGenStepData)sdi;

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
        meta = (MDKeyGenStepMeta)smi;
        data = (MDKeyGenStepData)sdi;
        super.dispose(smi, sdi);
        dataHandler= null;
        finalMerger= null;
    }
    
}
