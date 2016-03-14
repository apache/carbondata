package com.huawei.unibi.molap.merger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.olap.MolapDef.Schema;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.exception.MolapDataProcessorException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.merger.columnar.ColumnarFactFileMerger;
import com.huawei.unibi.molap.merger.columnar.impl.NonTimeBasedMergerColumnar;
import com.huawei.unibi.molap.merger.columnar.impl.TimeBasedMergerColumnar;
import com.huawei.unibi.molap.merger.exeception.SliceMergerException;
import com.huawei.unibi.molap.merger.sliceMerger.DimesionMappingFileMerger;
import com.huawei.unibi.molap.merger.sliceMerger.HierarchyFileMerger;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.metadata.SliceMetaData;


import com.huawei.unibi.molap.schema.metadata.AggregateTable;
import com.huawei.unibi.molap.schema.metadata.MolapColumnarFactMergerInfo;
import com.huawei.unibi.molap.util.ByteUtil;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapDataProcessorUtil;
import com.huawei.unibi.molap.util.MolapMergerUtil;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapSliceAndFiles;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.ValueCompressionUtil;

public class MolapColumnarSliceMerger implements MolapSliceMerger
{
    /**
     * molap schema object
     */
    private Schema schema;

    /**
     * molap cube object
     */
    private Cube cube;

    /**
     * table name to be merged
     */
    private String tableName;
    
    private List<String> loadsToBeMerged;
    
    private String mergedLoadName;

    /**
     * logger
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(MolapColumnarSliceMerger.class.getName());

//    public static void main(String[] args) throws SliceMergerException
//    {
//        String schemaPath = "D:/Network_Audit_Gn.xml";
//        String storeLocation = "D:/molap";
//        Schema loadXML = MolapSchemaParser.loadXML(schemaPath);
//        Cube localCube = MolapSchemaParser.getMondrianCubes(loadXML)[0];
//
//        MolapSliceMergerInfo molapSliceMergerInfo = new MolapSliceMergerInfo();
//        molapSliceMergerInfo.setCubeName(localCube.name);
//        molapSliceMergerInfo.setSchemaName(loadXML.name);
////        molapSliceMergerInfo.setTableName(MolapSchemaParser
////                .getFactTableName(localCube));
//        molapSliceMergerInfo.setTableName("agg_1_FACT_IU_DATA_INFO");
//        molapSliceMergerInfo.setSchemaPath(schemaPath);
//        MolapProperties.getInstance().addProperty("store_output_location",
//                storeLocation);
//        MolapProperties.getInstance().addProperty(
//                MolapCommonConstants.STORE_LOCATION, storeLocation);
//        MolapProperties.getInstance().addProperty(
//                MolapCommonConstants.STORE_LOCATION_HDFS, storeLocation);
//        MolapProperties.getInstance().addProperty("send.signal.load", "false");
//        MolapProperties.getInstance().addProperty(
//                "molap.dimension.split.value.in.columnar", "1");
//        MolapProperties.getInstance().addProperty("molap.is.fullyfilled.bits",
//                "true");
//        MolapProperties.getInstance().addProperty("is.int.based.indexer",
//                "true");
//        MolapProperties.getInstance().addProperty(
//                "aggregate.columnar.keyblock", "true");
//        MolapProperties.getInstance().addProperty("high.cardinality.value",
//                "100000");
//        MolapProperties.getInstance().addProperty("molap.is.columnar.storage",
//                "true");
//        MolapProperties.getInstance()
//                .addProperty("molap.leaf.node.size", "299");
//
//        MolapColumnarSliceMerger columnarSliceMerger = new MolapColumnarSliceMerger(
//                molapSliceMergerInfo);
//
//        columnarSliceMerger.fullMerge();
//
//    }

    public MolapColumnarSliceMerger(MolapSliceMergerInfo molapSliceMergerInfo)
    {
        // if schema object is null, then get the schema object after parsing
        // the schema object and update the schema based on partition id
        if(null == molapSliceMergerInfo.getSchema())
        {
            schema = MolapSchemaParser.loadXML(molapSliceMergerInfo
                    .getSchemaPath());
        }
        else
        {
            schema = molapSliceMergerInfo.getSchema();
        }
        cube = MolapSchemaParser.getMondrianCube(schema,
                molapSliceMergerInfo.getCubeName());
        if(molapSliceMergerInfo.getPartitionID() != null && null==molapSliceMergerInfo.getSchema())
        {
            String originalSchemaName = schema.name;
            schema.name = originalSchemaName + '_'
                    + molapSliceMergerInfo.getPartitionID();
            cube.name = cube.name + '_' + molapSliceMergerInfo.getPartitionID();
        }
        this.tableName = molapSliceMergerInfo.getTableName();
        
        this.loadsToBeMerged = molapSliceMergerInfo.getLoadsToBeMerged();
        
        this.mergedLoadName = molapSliceMergerInfo.getMergedLoadName();
    }

    @Override
    public boolean fullMerge(int currentRestructNumber) throws SliceMergerException
    {
        
        String hdfsLocation = MolapProperties.getInstance().getProperty(
                MolapCommonConstants.STORE_LOCATION_HDFS)
                + '/' + schema.name + '/' + cube.name;

        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,"HDFS Location: "+ hdfsLocation);
//        String tempLocationKey = schema.name + '/' + cube.name;
        String localStore = MolapProperties.getInstance().getProperty(
                MolapCommonConstants.STORE_LOCATION,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
                + '/' + schema.name + '/' + cube.name;

        int restrctFolderCount = currentRestructNumber/*MolapUtil
                .checkAndReturnNextRestructFolderNumber(hdfsLocation,"RS_")*/;
        if(restrctFolderCount == -1)
        {
            restrctFolderCount = 0;
        }
        hdfsLocation = hdfsLocation + '/'
                + MolapCommonConstants.RESTRUCTRE_FOLDER + restrctFolderCount
                + '/' + tableName;
        
        
        
        try
        {
            if(!FileFactory.isFileExist(hdfsLocation, FileFactory.getFileType(hdfsLocation)))
            {
                return false;
            }
        }
        catch(IOException e)
        {
            LOGGER.error(
                    MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG ,
                    "Error occurred :: " + e.getMessage());
        }
        
        
        
        List<MolapSliceAndFiles> slicesFromHDFS = MolapMergerUtil.getSliceAndFilesList(
                hdfsLocation, this.tableName,FileFactory.getFileType(hdfsLocation),loadsToBeMerged);
        
        if(slicesFromHDFS.isEmpty())
        {
            return false;
        }
        
        localStore = localStore
                + '/'
                + MolapCommonConstants.RESTRUCTRE_FOLDER
                + restrctFolderCount
                + '/'
                + tableName
                +'/'
                + MolapCommonConstants.LOAD_FOLDER+mergedLoadName;

        String destinationLocation = localStore
                + MolapCommonConstants.FILE_INPROGRESS_STATUS;
      File file = new File(destinationLocation);
      if(!file.mkdirs())
      {
          throw new SliceMergerException("Problem while creating the destination location for slicemerging");
          
      }
      startMerge(slicesFromHDFS,
              MolapUtil.readSliceMetaDataFile(hdfsLocation, restrctFolderCount),file.getAbsolutePath(), restrctFolderCount);
      
      if(!file.renameTo(new File(localStore)))
      {
          throw new SliceMergerException("Problem while renaming the destination location for slicemerging");
      }
      return true;
    }


    /**
     * startMerge
     * @throws SliceMergerException
     * @throws MolapDataProcessorException
     */
    public void startMerge(List<MolapSliceAndFiles> slicesFromHDFS,
            SliceMetaData sliceMetaData, String destinationLocation,
            int currentRestructNumber)
            throws SliceMergerException
    {
        String factTableName = MolapSchemaParser.getFactTableName(this.cube);
            if(factTableName.equals(this.tableName))
            {
                mergerSlice(slicesFromHDFS, sliceMetaData, null, null,
                        destinationLocation, currentRestructNumber);
            }
                    }

    private MolapColumnarFactMergerInfo getMolapColumnarFactMergerInfo(
            List<MolapSliceAndFiles> slicesFromHDFS, String[] aggType,
            String[] aggClass, SliceMetaData readSliceMetaDataFile,
            String destinationLocation, KeyGenerator globalKeyGen)
    {
        MolapColumnarFactMergerInfo columnarFactMergerInfo = new MolapColumnarFactMergerInfo();
        columnarFactMergerInfo.setAggregatorClass(aggClass);
        columnarFactMergerInfo.setAggregators(aggType);
        columnarFactMergerInfo.setCubeName(this.cube.name);
        columnarFactMergerInfo
                .setGroupByEnabled(null != aggType ? true : false);
        if(null != aggType)
        {
            for(int i = 0;i < aggType.length;i++)
            {
                if(aggType[i].equals(MolapCommonConstants.DISTINCT_COUNT)
                        || aggType[i].equals(MolapCommonConstants.CUSTOM))
                {
                    columnarFactMergerInfo.setMergingRequestForCustomAgg(true);
                    break;
                }
            }
        }
        columnarFactMergerInfo.setDestinationLocation(destinationLocation);
        columnarFactMergerInfo.setMdkeyLength(readSliceMetaDataFile
                .getKeyGenerator().getKeySizeInBytes());
        columnarFactMergerInfo.setMeasureCount(readSliceMetaDataFile
                .getMeasures().length);
        columnarFactMergerInfo.setSchemaName(this.schema.name);
        columnarFactMergerInfo.setTableName(tableName);
        columnarFactMergerInfo.setDimLens(readSliceMetaDataFile.getDimLens());
        columnarFactMergerInfo.setSlicesFromHDFS(slicesFromHDFS);
        
        columnarFactMergerInfo.setGlobalKeyGen(globalKeyGen);
        
        char[] type = new char[readSliceMetaDataFile.getMeasures().length];
        Arrays.fill(type, 'n');
        if(null != aggType)
        {
            for(int i = 0;i < type.length;i++)
            {
                if(aggType[i].equals(MolapCommonConstants.DISTINCT_COUNT)
                        || aggType[i].equals(MolapCommonConstants.CUSTOM))
                {
                    type[i] = 'c';
                }
                else
                {
                    type[i] = 'n';
                }
            }
        }
        columnarFactMergerInfo.setType(type);
        return columnarFactMergerInfo;
    }

    /**
     * Below method will be used for merging the slice All the concrete classes
     * will override this method and will implements its own type of merging
     * method
     * @throws SliceMergerException
     *             will throw slice merger exception if any problem occurs
     *             during merging the slice
     * 
     */
    public void mergerSlice(List<MolapSliceAndFiles> slicesFromHDFS,
            SliceMetaData sliceMetaData, String[] aggType, String[] aggClass,
            String destinationLocation, int currentRestructNumber) throws SliceMergerException
    {
        List<List<LeafNodeInfoColumnar>> leafNodeInfoList = new ArrayList<List<LeafNodeInfoColumnar>>(
                slicesFromHDFS.size());
        List<LeafNodeInfoColumnar> sliceLeafNodeInfo = null;
        List<ValueCompressionModel> existingSliceCompressionModel = new ArrayList<ValueCompressionModel>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        String[] sliceLocation = new String[slicesFromHDFS.size()];
        int index = 0;
        for(int i = 0;i < sliceLocation.length;i++)
        {
            sliceLeafNodeInfo = new ArrayList<LeafNodeInfoColumnar>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            MolapSliceAndFiles sliceAndFiles = slicesFromHDFS.get(i);
            sliceLocation[index++] = sliceAndFiles.getPath();
            MolapFile[] factFiles = sliceAndFiles.getSliceFactFilesList();
            if(null == factFiles || factFiles.length < 1)
            {
                continue;
            }
            for(int j = 0;j < factFiles.length;j++)
            {
                sliceLeafNodeInfo.addAll(MolapUtil.getLeafNodeInfoColumnar(
                        factFiles[j], sliceMetaData.getMeasures().length,
                        sliceMetaData.getKeyGenerator().getKeySizeInBytes()));
            }

            int [] cardinality = MolapMergerUtil.getCardinalityFromLevelMetadata(sliceAndFiles.getPath(),tableName);
            KeyGenerator localKeyGen = KeyGeneratorFactory.getKeyGenerator(cardinality);
            sliceAndFiles.setKeyGen(localKeyGen);
            
            
            leafNodeInfoList.add(sliceLeafNodeInfo);
            existingSliceCompressionModel
                    .add(getCompressionModel(sliceAndFiles.getPath(),
                            sliceMetaData.getMeasures().length));
            
            
        }
        for(int i = 0;i < sliceLocation.length;i++)
        {
            LOGGER.info(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Slice Merger Start Merging for slice: " + sliceLocation[i]);
        }
        double[] uniqueValue = new double[sliceMetaData.getMeasures().length];
        double[] maxValue = new double[sliceMetaData.getMeasures().length];
        double[] minValue = new double[sliceMetaData.getMeasures().length];
        int[] decimalLength = new int[sliceMetaData.getMeasures().length];
        if(existingSliceCompressionModel.size() > 0)
        {
            System.arraycopy(existingSliceCompressionModel.get(0)
                    .getUniqueValue(), 0, uniqueValue, 0, sliceMetaData
                    .getMeasures().length);
            System.arraycopy(
                    existingSliceCompressionModel.get(0).getMaxValue(), 0,
                    maxValue, 0, sliceMetaData.getMeasures().length);
            System.arraycopy(
                    existingSliceCompressionModel.get(0).getMinValue(), 0,
                    minValue, 0, sliceMetaData.getMeasures().length);
            System.arraycopy(existingSliceCompressionModel.get(0).getDecimal(),
                    0, decimalLength, 0, sliceMetaData.getMeasures().length);
            for(int i = 1;i < existingSliceCompressionModel.size();i++)
            {
                updateUniqueValue(existingSliceCompressionModel.get(i)
                        .getUniqueValue(), uniqueValue);
                calculateMax(
                        existingSliceCompressionModel.get(i).getMaxValue(),
                        maxValue);
                calculateMin(
                        existingSliceCompressionModel.get(i).getMinValue(),
                        minValue);
                calculateDecimalLength(existingSliceCompressionModel.get(i)
                        .getDecimal(), decimalLength);
            }
            writeMeasureMetaFile(maxValue, minValue, uniqueValue,
                    decimalLength, existingSliceCompressionModel.get(0)
                            .getType(), destinationLocation);
            
            // write level metadata
            
            int [] maxCardinality = MolapMergerUtil.mergeLevelMetadata(sliceLocation,tableName,destinationLocation);
            
            KeyGenerator globalKeyGen=KeyGeneratorFactory.getKeyGenerator(maxCardinality);
            
            
            
            ColumnarFactFileMerger factMerger = null;
            
            // pass global key generator;
                factMerger = new NonTimeBasedMergerColumnar(
                        getMolapColumnarFactMergerInfo(slicesFromHDFS, aggType,
                                aggClass, sliceMetaData, destinationLocation,globalKeyGen), currentRestructNumber);
            LOGGER.info(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Starting fact file merging: ");
            factMerger.mergerSlice();
        }
    }

    /**
     * Below method will be used to write the measure files
     * 
     * @throws SliceMergerException
     *             if any problem while writing the measure meta file
     */
    private void writeMeasureMetaFile(double[] maxValue, double[] minValue,
            double[] uniqueValue, int[] decimalLength, char[] type,
            String destinationLocation) throws SliceMergerException
    {
        String msrMetaDataFile = destinationLocation + File.separator
                + MolapCommonConstants.MEASURE_METADATA_FILE_NAME
                + this.tableName
                + MolapCommonConstants.MEASUREMETADATA_FILE_EXT
                + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        try
        {
            MolapDataProcessorUtil.writeMeasureMetaDataToFile(maxValue,
                    minValue, decimalLength, uniqueValue, type,
                    new byte[maxValue.length], msrMetaDataFile);
            String changedFileName = msrMetaDataFile.substring(0,
                    msrMetaDataFile.lastIndexOf('.'));
            File currentFile = new File(msrMetaDataFile);
            File destFile = new File(changedFileName);
            currentFile.renameTo(destFile);
        }
        catch(MolapDataProcessorException e)
        {
            throw new SliceMergerException(
                    "Problem While Writing the measure meta file" + e);
        }
    }

    /**
     * This method will be used to get the compression model for slice
     * 
     * @param path
     *            slice path
     * @param measureCount
     *            measure count
     * @return compression model
     * 
     */
    private ValueCompressionModel getCompressionModel(String path,
            int measureCount)
    {
        ValueCompressionModel compressionModel = ValueCompressionUtil
                .getValueCompressionModel(path
                        + MolapCommonConstants.MEASURE_METADATA_FILE_NAME
                        + this.tableName
                        + MolapCommonConstants.MEASUREMETADATA_FILE_EXT,
                        measureCount);
        return compressionModel;
    }

    /**
     * This method will be used to update the max value for each measure
     * 
     * @param currentMeasures
     * 
     */
    private double[] calculateMax(double[] currentMeasures, double[] maxValue)
    {
        int arrayIndex = 0;
        for(double value : currentMeasures)
        {

            maxValue[arrayIndex] = (maxValue[arrayIndex] > value ? maxValue[arrayIndex]
                    : value);
            arrayIndex++;
        }
        return maxValue;
    }

    /**
     * This method will be used to update the min value for each measure
     * 
     * @param currentMeasures
     * 
     */
    private double[] calculateMin(double[] currentMeasures, double[] minValue)
    {
        int arrayIndex = 0;
        for(double value : currentMeasures)
        {

            minValue[arrayIndex] = (minValue[arrayIndex] < value ? minValue[arrayIndex]
                    : value);
            arrayIndex++;
        }
        return minValue;
    }

    /**
     * This method will be used to update the measures decimal length If current
     * measure length is more then decimalLength then it will update the decimal
     * length for that measure
     * 
     * @param currentMeasure
     *            measures array
     * 
     */
    private int[] calculateDecimalLength(int[] currentMeasure,
            int[] decimalLength)
    {
        int arrayIndex = 0;
        for(int value : currentMeasure)
        {
            decimalLength[arrayIndex] = (decimalLength[arrayIndex] > value ? decimalLength[arrayIndex]
                    : value);
            arrayIndex++;
        }
        return decimalLength;
    }

    /**
     * 
     * @param uniqueValue2
     * 
     */
    private double[] updateUniqueValue(double[] currentUniqueValue,
            double[] uniqueValue)
    {
        for(int i = 0;i < currentUniqueValue.length;i++)
        {
            if(uniqueValue[i] > currentUniqueValue[i])
            {
                // CHECKSTYLE:OFF Approval No:Approval-352
                uniqueValue[i] = currentUniqueValue[i];
                // CHECKSTYLE:ON Approval No:Approval-352
            }
        }
        return uniqueValue;
    }

}
