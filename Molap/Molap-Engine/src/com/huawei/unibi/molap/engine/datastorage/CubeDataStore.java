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

package com.huawei.unibi.molap.engine.datastorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.iweb.platform.logging.impl.StandardLogService;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStore;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
import com.huawei.unibi.molap.engine.datastorage.tree.CSBTree;
import com.huawei.unibi.molap.engine.scanner.Scanner;
import com.huawei.unibi.molap.engine.util.MolapDataInputStreamFactory;
//import com.huawei.unibi.molap.engine.scanner.impl.NonFilterTreeScanner;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import com.huawei.unibi.molap.keygenerator.columnar.impl.MultiDimKeyVarLengthVariableSplitGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.metadata.SliceMetaData;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.vo.HybridStoreModel;

public class CubeDataStore
{

    /**
     * 
     */
    private String tableName;

    /**
     * 
     */
    private KeyGenerator keyGenerator;

    /**
     * tree holds data of the fact table.
     */
    private DataStore data;
    
    /**
     * startKey
     */
    private byte[] startKey;

//    private static final String COMMA = ", ";

    /**
     * 
     */
    private Cube metaCube;

    /**
     * 
     */
    private List<String> aggregateNames;

    /**
     * 
     */
    protected String factTableColumn;

    /**
     * 
     */
    private int[] msrOrdinal;
    
    /**
     * meta
     */
    private SliceMetaData smd;
    
    /**
     * unique value
     */
    private double[] uniqueValue;
    
    /**
     * min value
     */
    private double[] minValue;
    
    /**
     * min value
     */
    private double[] minValueFactForAgg;
    /**
     * type
     */
    private char[] type;
    
    private boolean isColumnar;
    
    private boolean[] aggKeyBlock;
    
    private int[] dimCardinality;

    private HybridStoreModel hybridStoreModel;
    

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(CubeDataStore.class.getName());

    /**
     * @return
     */
    public String getFactTableColumn()
    {
        return factTableColumn;
    }

    /**
     * @return
     */
    public boolean hasFactCount()
    {
        return factTableColumn != null && factTableColumn.length() > 0;
    }

    public CubeDataStore(String table, Cube metaCube, SliceMetaData smd,KeyGenerator keyGenerator, int[] dimCardinality,HybridStoreModel hybridStoreModel)
    {
        this.hybridStoreModel =hybridStoreModel;
        factTableColumn = metaCube.getFactCountColMapping(table);
        tableName = table;
        this.metaCube = metaCube;

        boolean hasFactCount = hasFactCount();
        this.smd = smd;
        List<Measure> measures = metaCube.getMeasures(table);
        prepareComplexDimensions(metaCube.getDimensions(table));
        aggregateNames = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        if(hasFactCount)
        {
            msrOrdinal = new int[measures.size() + 1];
        }
        else
        {
            msrOrdinal = new int[measures.size()];
        }
        int len = 0;
        for(Measure measure : measures)
        {
            aggregateNames.add(measure.getAggName());
            msrOrdinal[len] = len;
            len++;
        }
        if(hasFactCount)
        {
            aggregateNames.add("sum");
            msrOrdinal[len] = len;
        }
        this.keyGenerator=keyGenerator;
        this.dimCardinality = new int[dimCardinality.length];
        System.arraycopy(dimCardinality, 0, this.dimCardinality, 0, dimCardinality.length);
    }

    private void prepareComplexDimensions(List<Dimension> currentDimTables)
    {
        Map<String, ArrayList<Dimension>> complexDimensions = new HashMap<String, ArrayList<Dimension>>();
        for(int i = 0;i < currentDimTables.size();i++)
        {
            ArrayList<Dimension> dimensions = complexDimensions.get(currentDimTables.get(i).getHierName());
            if(dimensions != null)
            {
                dimensions.add(currentDimTables.get(i));
            }
            else
            {
                dimensions = new ArrayList<Dimension>();
                dimensions.add(currentDimTables.get(i));
            }
            complexDimensions.put(currentDimTables.get(i).getHierName(), dimensions);
        }
        
        for (Map.Entry<String, ArrayList<Dimension>> entry : complexDimensions.entrySet())
        {
            int[] blockIndexsForEachComplexType = new int[entry.getValue().size()];
            for(int i=0;i<entry.getValue().size();i++)
            {
                blockIndexsForEachComplexType[i] = entry.getValue().get(i).getDataBlockIndex();
            }
            entry.getValue().get(0).setAllApplicableDataBlockIndexs(blockIndexsForEachComplexType);
        }
    }
    
    /**
     * Gets the DataStore
     * @param keyGen
     * @param msrCount
     * @return
     */
    protected DataStore getDataStoreDS(KeyGenerator keyGen, int msrCount, int [] keyblockSize, boolean [] aggKeyBlock, boolean isColumnar)
    {
        boolean isFileStore = false;
        // Get the mode from cube
        // This will either be file or in-memory
        // Cube logic ensures that only these two will come here.
        String schemaAndcubeName = metaCube.getCubeName();
        String schemaName = metaCube.getSchemaName();
        String cubeName = schemaAndcubeName.substring(schemaAndcubeName.indexOf(schemaName +'_')+schemaName.length()+1,
                schemaAndcubeName.length());
        String modeValue = metaCube.getMode();
        if(modeValue.equalsIgnoreCase(MolapCommonConstants.MOLAP_MODE_DEFAULT_VAL))
        {
            isFileStore = true;
        }
        boolean isForcedInMemoryCube = Boolean.parseBoolean(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.IS_FORCED_IN_MEMORY_CUBE,
                MolapCommonConstants.IS_FORCED_IN_MEMORY_CUBE_DEFAULT_VALUE));
        if(isForcedInMemoryCube)
        {
            isFileStore=false;
        }
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Mode set for cube " + schemaName +':'+cubeName
                + "as mode=" + (isFileStore?"file":"In-Memory"));
        if(isColumnar)
        {
            return new CSBTree(this.hybridStoreModel,keyGen, msrCount, tableName,isFileStore,keyblockSize,aggKeyBlock);
        }
        else
        {
            return new CSBTree(keyGen, msrCount, tableName,isFileStore);
        }
    }

    public boolean loadDataFromFile(String filesLocaton, int startAndEndKeySize)
    {
        // added for get the MDKey size by liupeng 00204190.
        MolapFile file = FileFactory.getMolapFile(filesLocaton, FileFactory.getFileType(filesLocaton));
        boolean hasFactCount = hasFactCount();
        int numberOfValues = metaCube.getMeasures(tableName).size() + (hasFactCount ? 1 : 0);
        StandardLogService.setThreadName(StandardLogService.getPartitionID(metaCube.getOnlyCubeName()), null);
        checkIsColumnar(numberOfValues);
//        int keySize = keyGenerator.getKeySizeInBytes();
        int keySize = startAndEndKeySize;
        int msrCount =  smd.getMeasures().length;
        List<DataInputStream> streams = new ArrayList<DataInputStream>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        if(file.isDirectory())
        {
            //Verify any update status fact file is present so that the original fact will be ignored since
            //updation has happened as per retention policy.
            MolapFile[] files = getMolapFactFilesList(file);
            
            if(files.length == 0)
            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"@@@@ Fact file is missing for the table :"+tableName+" @@@@");
                return false;
            }
            files=removeFactFileWithDeleteStatus(files);
            files = getMolapFactFilesWithUpdateStatus(files);
            if(files.length==0)
            {
            	return false;
            }
            for(MolapFile aFile : files)
            {
                streams.add(MolapDataInputStreamFactory.getDataInputStream(aFile.getAbsolutePath(), keySize, msrCount, hasFactCount(), filesLocaton, tableName,FileFactory.getFileType(filesLocaton)));
            }
        }

        // Initialize the stream readers
        int streamCount = streams.size();
        for(int streamCounter = 0;streamCounter < streamCount;streamCounter++)
        {
            streams.get(streamCounter).initInput();
        }
        //Coverity Fix add null check
        ValueCompressionModel valueCompressionMode = streams.get(0).getValueCompressionMode();
        if(null != valueCompressionMode)
        {
            this.uniqueValue= valueCompressionMode.getUniqueValue();
            this.minValue= valueCompressionMode.getMinValue();
            this.minValueFactForAgg = valueCompressionMode.getMinValueFactForAgg();
            this.type=valueCompressionMode.getType();
        }

        // Build tree from streams
        try
        {
            long t1 = System.currentTimeMillis();
//            if(false)
//            {
//                data.build(streams.get(0), hasFactCount());
//            }
//            else
//            {
                // data.build(streams, aggregateNames, hasFactCount());
            if(!isColumnar)
            {
                data.build(streams, hasFactCount());
            }
            else
            {
                data.buildColumnar(streams, hasFactCount(),metaCube);
            }
//            }
            
            LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Fact increamental load build time is: " + 
                    (System.currentTimeMillis() - t1));
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e,e.getMessage());
        }

        // Close the readers
        for(int streamCounter = 0;streamCounter < streamCount;streamCounter++)
        {
            streams.get(streamCounter).closeInput();
        }
        
        startKey = streams.get(0).getStartKey();
        return true;
    }

    private MolapFile[] removeFactFileWithDeleteStatus(MolapFile[] files)
    {
    	List<MolapFile> listOfFactFileWithDelStatus=new ArrayList<MolapFile>(files.length);
        Collections.addAll(listOfFactFileWithDelStatus, files);
        for(MolapFile molapFile:files)
        {
        	if(molapFile.getName().endsWith(MolapCommonConstants.FACT_DELETE_EXTENSION))
            {
                for(MolapFile molapArrayFiles:files)
                {
                	String factFileNametoRemove=molapArrayFiles.getName().substring(0,molapFile.getName().indexOf(MolapCommonConstants.FACT_DELETE_EXTENSION));
                	if(molapArrayFiles.getName().equals(factFileNametoRemove))
                	{
                		listOfFactFileWithDelStatus.remove(molapArrayFiles);
                		listOfFactFileWithDelStatus.remove(molapFile);
                	}
                }
            }
        }
        MolapFile[] fileModified=new  MolapFile[listOfFactFileWithDelStatus.size()];
        return listOfFactFileWithDelStatus.toArray(fileModified);
    }
    private MolapFile[] getMolapFactFilesWithUpdateStatus(MolapFile[] files)
    {
        List<MolapFile> molapFileList=new ArrayList<MolapFile>(files.length);
        
        for(MolapFile molapFactFile:files)
        {
            if(molapFactFile.getName().endsWith(MolapCommonConstants.FACT_UPDATE_EXTENSION))
            {
                molapFileList.add(molapFactFile);
            }
        }
        if(molapFileList.size()>0)
        {
            files=molapFileList.toArray(new MolapFile[molapFileList.size()]);
             
        }
        return files;
    }


    /**
     * @param file
     * @return
     */
    private MolapFile[] getMolapFactFilesList(MolapFile file)
    {
        MolapFile[] files = file.listFiles(new MolapFileFilter()
        {
            public boolean accept(MolapFile pathname)
            {
                //verifying whether any fact file has been in update status as per retention policy. 
                boolean status=(!pathname.isDirectory()) && pathname.getName().startsWith(tableName)
                        && pathname.getName().endsWith(MolapCommonConstants.FACT_UPDATE_EXTENSION);
                if(status)
                {
                    return true;
                }
                status=(!pathname.isDirectory()) && pathname.getName().startsWith(tableName)
                        && pathname.getName().endsWith(MolapCommonConstants.FACT_DELETE_EXTENSION);
                if(status)
                {
                	return true;
                }
                return  (!pathname.isDirectory()) && pathname.getName().startsWith(tableName)
                        && pathname.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT);
            }

        });
        


//             Sort the fact files as per index number. (Expected names
//             filename_1,filename_2)
        Arrays.sort(files, new Comparator<MolapFile>()
        {
            public int compare(MolapFile o1, MolapFile o2)
            {
                try
                {
                    int f1 = Integer.parseInt(o1.getName().substring(tableName.length()+1).split("\\.")[0]);
                    int f2 = Integer.parseInt(o2.getName().substring(tableName.length()+1).split("\\.")[0]);
                    return (f1 < f2) ? -1 : (f1 == f2 ? 0 : 1);
                }
                catch(Exception e)
                {
                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e.getMessage());
                    return o1.getName().compareTo(o2.getName());
                }
            }
        });
        return files;
    }

    /**
     * @param numberOfValues
     */
    private void checkIsColumnar(int numberOfValues)
    {
        isColumnar=Boolean.parseBoolean(MolapCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE);
        
        if(isColumnar)
        {
            int dimSet = Integer.parseInt(MolapCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);

            if(!isColumnar)
            {
                dimSet = keyGenerator.getDimCount();
            }
            int[] keyBlockSize = null;
            
            // if there is no single dims present (i.e only high card dims is present.)
            if(this.dimCardinality.length > 0)
            {
                keyBlockSize = new MultiDimKeyVarLengthVariableSplitGenerator(MolapUtil.getDimensionBitLength(this.hybridStoreModel.getHybridCardinality(),this.hybridStoreModel.getDimensionPartitioner()),this.hybridStoreModel.getColumnSplit())
                        .getBlockKeySize();

           // aggKeyBlock = new boolean[dimCardinality.length];
                boolean isAggKeyBlock = Boolean
                        .parseBoolean(MolapCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE);
                if(isAggKeyBlock)
                {
                    int highCardinalityValue = Integer.parseInt(MolapProperties.getInstance().getProperty(
                        MolapCommonConstants.HIGH_CARDINALITY_VALUE,
                        MolapCommonConstants.HIGH_CARDINALITY_VALUE_DEFAULTVALUE));
                    int aggIndex=0;
                    if(this.hybridStoreModel.isHybridStore())
                    {
                        this.aggKeyBlock=new boolean[this.hybridStoreModel.getColumnStoreOrdinals().length+1];
                        this.aggKeyBlock[aggIndex++]=false;
                    }
                    else
                    {
                        this.aggKeyBlock=new boolean[this.hybridStoreModel.getColumnStoreOrdinals().length]; 
                    }
                    
                    for(int i=hybridStoreModel.getRowStoreOrdinals().length;i<dimCardinality.length;i++)
                    {
                        if(dimCardinality[i]<highCardinalityValue)
                        {
                            this.aggKeyBlock[aggIndex++]=true;
                            continue;
                        }
                        aggIndex++;
                    }
                }

            }
            else
            {
                keyBlockSize = new int[0];
                aggKeyBlock = new boolean[0];
            }
            data = getDataStoreDS(keyGenerator, numberOfValues, keyBlockSize, aggKeyBlock,true);
        }
        else
        {
        
            data = getDataStoreDS(keyGenerator, numberOfValues,null, null,false);
        }
    }

    private int[] getKeyBlockSizeWithComplexTypes(int[] dimCardinality)
    {
        int[] keyBlockSize = new int[dimCardinality.length];
        for(int i=0;i<dimCardinality.length;i++)
        {
            if(dimCardinality[i] == 0)
                keyBlockSize[i] = 8;
            else
                keyBlockSize[i] = new MultiDimKeyVarLengthEquiSplitGenerator(new int[]{dimCardinality[i]}, (byte)1)
                  .getBlockKeySize()[0];
        }
        return keyBlockSize;
    }
//    public void loadDataFromSlices(List<CubeDataStore> dataStores, String fileStore)
//    {
//        List<Scanner> scanners = new ArrayList<Scanner>(MolapCommonConstants.CONSTANT_SIZE_TEN);
//        // Make scanners from old slices
//        CubeDataStore cubeDataStore = dataStores.get(0);
//        int[] msrOrdinal2 = cubeDataStore.getMsrOrdinal();
//        ValueCompressionModel compressionModel = cubeDataStore.getData().getCompressionModel();
//        FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(fileStore));
//        setScannerForSlices(scanners, cubeDataStore, msrOrdinal2,fileHolder);
//        double[] maxValue = compressionModel.getMaxValue();
//
//        int[] decimalLength = compressionModel.getDecimal();
//
//        double[] minValue = compressionModel.getMinValue();
//
//        for(int i = 1;i < dataStores.size();i++)
//        {
//            CubeDataStore cubeDataStore2 = dataStores.get(i);
//            ValueCompressionModel compressionModel1 = cubeDataStore.getData().getCompressionModel();
//            compare(maxValue, minValue, decimalLength, compressionModel1);
//            setScannerForSlices(scanners, cubeDataStore2, msrOrdinal2,fileHolder);
//        }
//        // Make scanner from file store
//        if(fileStore != null)
//        {
//            // TODO avoid create new tree and merge here. Try loading directly
//            // from files
//            CubeDataStore dataStore = new CubeDataStore(tableName, metaCube, smd);
//            dataStore.loadDataFromFile(fileStore);
//            // setScannerForSlices(scanners, dataStore, msrOrdinal2);
//            KeyValue keyValue = new KeyValue();
//            Scanner scanner = new NonFilterTreeScanner(new byte[0], null, keyGenerator, keyValue, msrOrdinal2, fileHolder);
//            dataStore.data.getNext(new byte[0], scanner);
//            scanners.add(scanner);
//            ValueCompressionModel compressionModel1= cubeDataStore.getData().getCompressionModel();
//            if (null != compressionModel1)
//            {
//                compare(maxValue, minValue, decimalLength, compressionModel1);
//            }
//        }
//
//        ScannersInputCombiner inputStream = new ScannersInputCombiner(scanners, keyGenerator, aggregateNames,
//                hasFactCount());
//        inputStream.initInput();
//        // TODO need to call build method with only onse stream
//        List<DataInputStream> list = new ArrayList<DataInputStream>(MolapCommonConstants.CONSTANT_SIZE_TEN);
//        list.add(inputStream);
//
//        if(isColumnar)
//        {
//            data.build(list, hasFactCount());
//        }
//        else
//        {
//            data.buildColumnar(list, hasFactCount());
//        }
//        fileHolder.finish();
//    }

   /* private void compare(double[] maxValue, double[] minValue, int[] decimal,
            ValueCompressionModel valueCompressionModel)
    {
        double[] maxValue2 = valueCompressionModel.getMaxValue();

        double[] minValue2 = valueCompressionModel.getMinValue();
        int[] decimal2 = valueCompressionModel.getDecimal();
        for(int j = 0;j < maxValue.length;j++)
        {
            maxValue[j] = maxValue[j] > (maxValue2[j]) ? maxValue[j] : (maxValue2[j]);
            minValue[j] = minValue[j] < (minValue2[j]) ? minValue[j] : (minValue2[j]);
            decimal[j] = decimal[j] > (decimal2[j]) ? decimal[j] : (decimal2[j]);
        }
    }*/

    /*private void setScannerForSlices(List<Scanner> scanners, CubeDataStore store, int[] msrOrdinal, FileHolder fileHolder)
    {
        KeyValue keyValue = new KeyValue();
        Scanner scanner = new NonFilterTreeScanner(new byte[0], null, keyGenerator, keyValue, msrOrdinal, fileHolder);
        store.data.getNext(new byte[0], scanner);
        scanners.add(scanner);
    }*/

    public KeyValue getData(byte[] key, Scanner scanner)
    {
        return data.get(key, scanner);
    }

    public void initializeScanner(byte[] key, Scanner scanner)
    {
        data.getNext(key, scanner);
    }

    public KeyValue getNextAvailableData(byte[] key, Scanner scanner)
    {
        return data.getNext(key, scanner);
    }

    public DataStoreBlock getDataStoreBlock(byte[] key, FileHolder fileHolder, boolean isFirst)
    {
        return data.getBlock(key, fileHolder, isFirst);
    }
    /*
     * public Scanner<byte[], double[]> getScanner(byte[] startKey, byte[]
     * endKey) { return factData.getScanner(startKey, endKey); }
     */

    public long getSize()
    {
        return data.size();
    }

    public void clear()
    {
        data = null;
    }

    public long[][] getDataStoreRange()
    {
        return data.getRanges();
    }

    /**
     * @return the data
     */
    public DataStore getData()
    {
        return data;
    }

    public int[] getMsrOrdinal()
    {
        return msrOrdinal;
    }

    public byte[] getStartKey()
    {
        return startKey;
    }

    public KeyGenerator getKeyGenerator()
    {
        return keyGenerator;
    }

    public double[] getUniqueValue()
    {
        return uniqueValue;
    }
    
    public double[] getMinValue()
    {
        return minValue;
    }

    /**
     * @return the type
     */
    public char[] getType()
    {
        return type;
    }

    /**
     * @return the minValueFactForAgg
     */
    public double[] getMinValueFactForAgg()
    {
        return minValueFactForAgg;
    }
    /**
     * @return the aggKeyBlock
     */
    public boolean[] getAggKeyBlock()
    {
        return aggKeyBlock;
    }

    /**
     * @param aggKeyBlock the aggKeyBlock to set
     */
    public void setAggKeyBlock(boolean[] aggKeyBlock)
    {
        this.aggKeyBlock = aggKeyBlock;
    }

    public int[] getDimCardinality()
    {
        return dimCardinality;
    }

    public void setDimCardinality(int[] dimCardinality)
    {
        this.dimCardinality = dimCardinality;
    }
}
