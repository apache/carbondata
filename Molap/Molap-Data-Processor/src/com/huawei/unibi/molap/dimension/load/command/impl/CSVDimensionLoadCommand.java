/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnKYzQS9qvG3N523RBPdX9jbaVUnaNBjuGYlTNt4ILkAPw1mmgFsuJw6yle2p0frZ0yzF
2vAC5fZNmWENjmhCvlGRsDUxmxxULj2er0qX0sEgB1hs9opb65TIGN34z6xvEw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.dimension.load.command.impl;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.csvreader.checkpoint.CheckPointHanlder;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.dimension.load.command.DimensionLoadCommand;
import com.huawei.unibi.molap.dimension.load.info.DimensionLoadInfo;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.schema.metadata.HierarchiesInfo;
import com.huawei.unibi.molap.surrogatekeysgenerator.csvbased.MolapCSVBasedDimSurrogateKeyGen;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.writer.LevelValueWriter;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.Set;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :14-Nov-2013 6:07:43 PM
 * FileName : CSVDimensionLoadCommand.java
 * Class Description :
 * Version 1.0
 */
public class CSVDimensionLoadCommand implements DimensionLoadCommand
{
    /**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(CSVDimensionLoadCommand.class.getName());

    /**
     * Dimension Load Info
     */
    private DimensionLoadInfo dimensionLoadInfo;
    
    private int currentRestructNumber;
    
    private LevelValueWriter[] dimensionWriter;
    
    public CSVDimensionLoadCommand(DimensionLoadInfo loadInfo, int currentRestructNum, LevelValueWriter[] dimensionWriter)
    {
        this.dimensionLoadInfo = loadInfo;
        this.currentRestructNumber = currentRestructNum;
        this.dimensionWriter = dimensionWriter;
    }
    
    /**
     * 
     * @throws KettleException 
     * @see com.huawei.unibi.molap.dimension.load.command.DimensionLoadCommand#execute()
     * 
     */
    @Override
    public void execute() throws KettleException
    {
        loadData(dimensionLoadInfo);
    }

    /**
     * 
     * @param dimensionLoadInfo
     * @throws KettleException 
     * 
     */
    private void loadData(DimensionLoadInfo dimensionLoadInfo) throws KettleException
    {
        List<HierarchiesInfo> metahierVoList = dimensionLoadInfo.getHierVOlist();
        
        try
        {
        	String dimFileMapping = dimensionLoadInfo.getDimFileLocDir();
        	Map<String, String> fileMaps = new HashMap<String, String>();
        	
        	if(null != dimFileMapping && dimFileMapping.length() > 0)
        	{
        		String[] fileMapsArray = dimFileMapping.split(",");
            	
            	
            	for(String entry : fileMapsArray)
            	{
            		String tableName = entry.split(":")[0];
                	String dimCSVFileLoc = entry.substring(tableName.length() + 1);
            		fileMaps.put(tableName, dimCSVFileLoc);
            	}
        	}
        	
            for(int i = 0;i < metahierVoList.size();i++)
            {
                HierarchiesInfo hierarchyInfo = metahierVoList.get(i);
                String hierarichiesName = hierarchyInfo.getHierarichieName();
                int[] columnIndex = hierarchyInfo.getColumnIndex();
                String[] columnNames = hierarchyInfo.getColumnNames();
                String query = hierarchyInfo.getQuery();
                boolean isTimeDim = hierarchyInfo.isTimeDimension();
                Map<String, String> levelTypeColumnMap = hierarchyInfo
                        .getLevelTypeColumnMap();
                if(null == query) // table will be denormalized so no foreign
                                  // key , primary key for this hierarchy
                { // Direct column names will be present in the csv file. in
                  // that case continue.
                    continue;
                } 
                boolean loadToHierarichiTable = hierarchyInfo
                        .isLoadToHierarichiTable();
                Map<String, String[]> columnPropMap = hierarchyInfo
                        .getColumnPropMap();

                updateHierarichiesFromCSVFiles(columnNames, columnPropMap,
                        columnIndex, hierarichiesName, loadToHierarichiTable,
                        query, isTimeDim, levelTypeColumnMap, currentRestructNumber, fileMaps);

            }
        }
        catch(Exception e)
        {
            throw new KettleException(e.getMessage(), e);
        }
        
        if(CheckPointHanlder.IS_CHECK_POINT_NEEDED)
        {
            // close the streams
            try
            {
                dimensionLoadInfo.getSurrogateKeyGen().writeHeirDataToFileAndCloseStreams();

                // Merge the dimension and hierarchy files
//                MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen = dimensionLoadInfo.getSurrogateKeyGen();
//                String baseStoreFolder = surrogateKeyGen.getStoreFolderWithLoadNumber();
//                Map<String, KeyGenerator> keyGenerator = dimensionLoadInfo.getKeyGenerator();
                
//                Map<String, Integer> hierKeyMap = DimenionLoadCommandHelper.getHierKeyMap(keyGenerator);
                
//                DimenionLoadCommandHelper.mergeFiles(baseStoreFolder, hierKeyMap);
            }
            catch(KeyGenException e)
            {
                LOGGER.error(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Not able to close the stream for level value and hierarchy files.");
            }
//         catch(IOException e)
//         {
//             LOGGER.error(
//                     MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
//                     "Not able to merge the level and hierarchy files.");
//         }
            
        }
    }
    
    private void updateHierarichiesFromCSVFiles(String[] columnNames,
            Map<String, String[]> columnPropMap, int[] columnIndex,
            String hierarichiesName, boolean loadToHier, String query,
            boolean isTimeDim, Map<String, String> levelTypeColumnMap, int currentRestructNumber, Map<String, String> fileMaps) throws KettleException, IOException
             
    {
        
        DimenionLoadCommandHelper dimenionLoadCommandHelper = DimenionLoadCommandHelper.getInstance();
        
        String modifiedDimesions = dimensionLoadInfo.getModifiedDimesions();
        
        String substring = query.substring(query.indexOf("SELECT") + 6,
                query.indexOf("FROM"));
        String[] actualColumnsIncludingPrimaryKey = substring.split(",");
        
        for(int colIndex = 0; colIndex < actualColumnsIncludingPrimaryKey.length; colIndex++)
        {
            if(actualColumnsIncludingPrimaryKey[colIndex].contains("\""))
            {
                actualColumnsIncludingPrimaryKey[colIndex] = actualColumnsIncludingPrimaryKey[colIndex]
                        .replaceAll("\"", "");
            }
        }
        
        String tblName = query.substring(query.indexOf("FROM") + 4).trim();
        if(tblName.contains("."))
        {
            tblName = tblName.split("\\.")[1];
        }
        if(tblName.contains("\""))
        {
            tblName = tblName.replaceAll("\"", "");
        }
        //trim to remove any spaces
        tblName = tblName.trim();
        
        //First we need to check whether modified dimensions is null and this is first call for data loading,
        // In that case we need to load the data for all the dimension table.
        // If modifeied dimensions is not null then we will update only the dimension table data 
        // which is mensioned in the modified dimension table list.
        
        // In case of restructuring we are adding one member by default in the level mapping file, so for 
        // Incremental load we we need to check if restructure happened then for that table added newly 
        // we have to load data. So added method checkModifiedTableInSliceMetaData().
        if(null == modifiedDimesions
                && dimenionLoadCommandHelper.isDimCacheExist(
                        actualColumnsIncludingPrimaryKey, tblName,
                        columnPropMap, dimensionLoadInfo)
                && dimenionLoadCommandHelper.isHierCacheExist(hierarichiesName,
                        dimensionLoadInfo)
                && dimenionLoadCommandHelper.checkModifiedTableInSliceMetaData(
                        tblName, dimensionLoadInfo, currentRestructNumber))
        {
            return;
        }
        else if(null != modifiedDimesions
                && dimenionLoadCommandHelper.isDimCacheExist(
                        actualColumnsIncludingPrimaryKey, tblName,
                        columnPropMap, dimensionLoadInfo)
                && dimenionLoadCommandHelper.isHierCacheExist(hierarichiesName,
                        dimensionLoadInfo)
                && dimenionLoadCommandHelper.checkModifiedTableInSliceMetaData(
                        tblName, dimensionLoadInfo, currentRestructNumber))
        {
            String[] dimTables = modifiedDimesions.split(",");
            int count = 0;
            for(String dimTable : dimTables)
            {
                if(dimTable.equalsIgnoreCase(tblName))
                {
                    break;
                }
                count++;
            }

            // table doesnot exist in the modified dimention list then no need
            // to load
            // this dimension table.
            if(count == dimTables.length)
            {
                return;
            }
        }

        MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen = dimensionLoadInfo.getSurrogateKeyGen();
        
        // If Dimension table has to load then first check whether it is time Dimension,
        //if yes then check the mappings specified in the realtimedata.properties.
        // If nothing is specified their also then go through the normal flow.
        String primaryKeyColumnName = query.substring(
                query.indexOf("SELECT") + 6, query.indexOf(","));

        primaryKeyColumnName = tblName + '_' + primaryKeyColumnName.replace("\"", "").trim();
        isTimeDim=false;
       /* if(isTimeDim)
        {
            if(!loadTimeDimensionFromRealTimeDataMappingFile(columnNames,
                    columnIndex, levelTypeColumnMap, dimFileLocDir,
                    dimenionLoadCommandHelper, tblName, primaryKeyColumnName))
            {
                return;
            }
        
        }*/
        
        LevelValueWriter primaryKeyValueWriter = null;
        boolean fileAlreadyCreated = false;
        DataInputStream fileReader = null;
        BufferedReader bufferedReader = null;
        try
        {
            String dimCsvFile = fileMaps.get(tblName);
            
            if(null == dimCsvFile)
            {
            	LOGGER.error(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "For Dimension table : \"" + tblName
                                + " \" CSV file path is NULL.");
                throw new RuntimeException("For Dimension table : \""
                        + dimCsvFile + " \" , CSV file path is NULL.");
            }
            
            FileType fileType = FileFactory.getFileType(dimCsvFile);
            
            if(!FileFactory.isFileExist(dimCsvFile, fileType))
            {
                LOGGER.error(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "For Dimension table : \"" + tblName
                                + " \" CSV file not presnt.");
                throw new RuntimeException("For Dimension table : \""
                        + dimCsvFile + " \" ,CSV file not presnt.");
            }
            
            fileReader = FileFactory.getDataInputStream(dimCsvFile, fileType);
            bufferedReader = new BufferedReader(new InputStreamReader(fileReader,Charset.defaultCharset()));
            
            String header = bufferedReader.readLine();
            if(null == header)
            {
                return;
            }
            
            int[] dimColmapping = getDimColumnNameMapping(tblName,
                    columnNames, header, dimenionLoadCommandHelper);
            int primaryColumnIndex = getPrimaryColumnMap(tblName,
                    primaryKeyColumnName, header, dimenionLoadCommandHelper);
            
            if(primaryColumnIndex == -1)
            {
                return;
            }

//            String[] origunalColumnName = new String[actualColumnsIncludingPrimaryKey.length];
            String[] columnNameArray = dimenionLoadCommandHelper.checkQuotesAndAddTableNameForCSV(
                    dimenionLoadCommandHelper.getRowData(header), tblName);
            int primaryIndexInLevel = dimenionLoadCommandHelper.getRepeatedPrimaryFromLevels(tblName, columnNames,actualColumnsIncludingPrimaryKey[0]);
            if(primaryIndexInLevel == -1)
            {
                if(primaryKeyColumnName.contains("\""))
                {
                    primaryKeyColumnName = primaryKeyColumnName.replaceAll("\"", "");
                }
                String dimFileName = primaryKeyColumnName
                        + MolapCommonConstants.LEVEL_FILE_EXTENSION;
//                        + MolapCommonConstants.FILE_INPROGRESS_STATUS;
                for(int i = 0;i < dimensionWriter.length;i++)
                {
                    if(dimFileName.equals(dimensionWriter[i]
                            .getMemberFileName()))
                    {
                        primaryKeyValueWriter = dimensionWriter[i];
                        fileAlreadyCreated = true;
                        break;
                    }
                }
                if(null == primaryKeyValueWriter)
                {
                    primaryKeyValueWriter = new LevelValueWriter(dimFileName,
                            surrogateKeyGen.getStoreFolderWithLoadNumber());
                }
            }
            
//            columnNameFileIndex = getIndex(columnNameArray, columnNames);
            int[] outputVal = new int[columnNames.length];
            int[][] propertyIndex = null;
            int primaryKeySurrogate = -1;
//            String []columnNameWithoutPrimaryKey = new String[columnNameArray.length -1];
//            System.arraycopy(columnNameArray, 1, columnNameWithoutPrimaryKey, 0, columnNameArray.length-1);
            propertyIndex = new int[columnNames.length][];
            for(int i = 0;i < columnNames.length;i++)
            {
                String[] property = columnPropMap.get(columnNames[i]);
                propertyIndex[i] = dimenionLoadCommandHelper.getIndex(columnNameArray, property);
            }
            outputVal = new int[columnNames.length];

            boolean isKeyExceeded=false;
            int recordCnt = 0; 
            String dataline = null;
            while((dataline = bufferedReader.readLine()) != null)
            {
                if(dataline.isEmpty())
                {
                    continue;
                }
                String[] data = dimenionLoadCommandHelper.getRowData(dataline);

                recordCnt++;
                outputVal = new int[columnIndex.length];
                String primaryKey = data[primaryColumnIndex];
                if(null==primaryKey)
                {
                    continue;
                }

                // String[] columnNameArrayNew = new String[length-1];
                isKeyExceeded = processCSVRows(columnNames, columnIndex,
                        isTimeDim, surrogateKeyGen, dimColmapping, outputVal,
                        propertyIndex,data);

                if(!isKeyExceeded)
                {
                    if(primaryIndexInLevel >= 0)
                    {
                        primaryKeySurrogate = outputVal[primaryIndexInLevel];
                    }
                    else
                    {
                        primaryKeySurrogate = surrogateKeyGen.getSurrogateKeyForPrimaryKey(primaryKey,
                                primaryKeyColumnName, primaryKeyValueWriter);
                    }

                    if(loadToHier)
                    {
                        surrogateKeyGen.checkHierExists(outputVal, hierarichiesName, primaryKeySurrogate);
                    }
                }
//                isKeyExceeded= false;// Not required to assign, as it is not used further.in the loop again it is initialized.
            }

            if(0==recordCnt)
            {
                Map<String, Map<String, Integer>> memberCache = surrogateKeyGen.getMemberCache();
                Map<String, Integer> primaryKeyColName = memberCache.get(primaryKeyColumnName);
                if(null == primaryKeyColName) 
                {
                    memberCache.put(primaryKeyColumnName, new HashMap<String, Integer>(0));
                }
            }
            
         /*   if(isTimeDim)
            {
                surrogateKeyGen.setTimeDimCache(null);
                dimenionLoadCommandHelper.dataPropertyReader = null;
            }
            */
        }
        catch(KettleException e)
        {
            throw new KettleException(e.getMessage(), e);
        }
        finally
        {
            if(null != primaryKeyValueWriter && !fileAlreadyCreated)
            {
                try
                {
                    primaryKeyValueWriter.writeMaxValue();
                }
                catch(IOException e)
                {
                    throw new KettleException(e.getMessage(), e);
                }
                MolapUtil.closeStreams(primaryKeyValueWriter
                        .getBufferedOutputStream());

                String storePath = surrogateKeyGen
                        .getStoreFolderWithLoadNumber();
                String levelFileName = primaryKeyValueWriter
                        .getMemberFileName();
                int counter = primaryKeyValueWriter.getCounter();

                String changedFileName = levelFileName + (counter - 1);
                // String changedFileName = inProgFileName.substring(0,
                // inProgFileName.lastIndexOf('.'));
                String inProgFileName = changedFileName
                        + MolapCommonConstants.FILE_INPROGRESS_STATUS;
                File inProgress = new File(storePath + File.separator
                        + inProgFileName);
                File destFile = new File(storePath + File.separator
                        + changedFileName);

                if(!inProgress.renameTo(destFile))
                {
                    LOGGER.error(
                            MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Renaming of file is not successfull : "
                                    + inProgress.getAbsolutePath());
                }
            }
            if(null!=bufferedReader)
            {
                MolapUtil.closeStreams(bufferedReader);
            }
        }
    }

    /**
     * Below method will be used to load the the time dimension data which is present in real time data mapping file and dimension table 
     * @param columnNames
     * @param columnIndex
     * @param levelTypeColumnMap
     * @param dimFileLocDir
     * @param dimenionLoadCommandHelper
     * @param tableName
     * @param primaryKeyColumnName
     */
    /*private boolean loadTimeDimensionFromRealTimeDataMappingFile(
            String[] columnNames, int[] columnIndex,
            Map<String, String> levelTypeColumnMap, String dimFileLocDir,
            DimenionLoadCommandHelper dimenionLoadCommandHelper,
            String tableName, String primaryKeyColumnName) throws Exception
    {
        DataInputStream fileReader = null;
        BufferedReader bufferedReader = null;
        try
        {
            int[] dimCardinality = dimensionLoadInfo.getDimCardinality();
            // get the column name only year, month and day is supported in real time mapping file  
            Iterator<String> iterator = levelTypeColumnMap.values()
                    .iterator();
            // create map for each level and it data present in dimension table in csv files
            Map<String, Set<String>> columnAndMemberListaMap = new HashMap<String, Set<String>>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            String[] columnNameInDBTable = new String[levelTypeColumnMap.size()];
            int counter = 0;
            while(iterator.hasNext())
            {
                // create map with column and its data 
                Set<String> list = new HashSet<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
                columnNameInDBTable[counter] = iterator.next();
                columnAndMemberListaMap.put(columnNameInDBTable[counter], list);
                counter++;
            }

            // create dimension table file 
            FileType fileType = FileFactory.getFileType(dimFileLocDir);
            
            String dimCsvFile = dimFileLocDir + '/'
                    + tableName + ".csv";
            if(!FileFactory.isFileExist(dimCsvFile, fileType))
            {
                LOGGER.error(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "For Dimension table : \"" + dimCsvFile
                                + " \" CSV file not presnt.");
                throw new RuntimeException("For Dimension table : \""
                        + dimCsvFile + " \" ,CSV file not presnt.");
            }
            
            
            fileReader = FileFactory.getDataInputStream(dimCsvFile, fileType);
            // create reader 
            bufferedReader = new BufferedReader(new InputStreamReader(fileReader,Charset.defaultCharset()));

            // if header is not present in csv then return 
            String header = bufferedReader.readLine();
            if(null == header)
            {
                return false;
            }
            String[] headerValues = dimenionLoadCommandHelper
                    .getRowData(header);
            int[] dimColmapping = new int[columnNameInDBTable.length];
            int index = 0;
            // map the header 
            for(int i = 0;i < columnNameInDBTable.length;i++)
            {
                for(int j = 0;j < headerValues.length;j++)
                {
                    if(columnNameInDBTable[i].equals(headerValues[j]))
                    {
                        dimColmapping[index++] = j;
                    }
                }
            }
            // get the primary key column if its index is -1 then return 
            int primaryColumnIndex = getPrimaryColumnMap(tableName,
                    primaryKeyColumnName, header, dimenionLoadCommandHelper);
            if(primaryColumnIndex == -1)
            {
                return false;
            }
            counter=0;
            int []cardinalityIndex = new int[columnNameInDBTable.length];
            StringBuilder builder= null;
            for(int i = 0;i < columnNameInDBTable.length;i++)
            {
                builder= new StringBuilder();
                builder.append(tableName);
                builder.append('_');
                builder.append(columnNameInDBTable[i]);
                for(int j = 0;j < columnNames.length;j++)
                {
                    if(builder.toString().equals(columnNames[j]))
                    {
                        cardinalityIndex[counter++]=j;
                    }
                }
            }
            Map<String,Integer> levelAndCardinalityMap = new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            for(int i = 0;i < columnNameInDBTable.length;i++)
            {
                levelAndCardinalityMap.put(columnNameInDBTable[i],dimCardinality[cardinalityIndex[i]]);
            }
            String dimRow = "";
            // read row for year month and day and fill the map 
            while(null != (dimRow = bufferedReader.readLine()))
            {
                String[] columnValues = dimenionLoadCommandHelper
                        .getRowData(dimRow);
                for(int i = 0;i < dimColmapping.length;i++)
                {
                    if(columnValues[dimColmapping[i]]!=null)
                    {
                        columnAndMemberListaMap.get(columnNameInDBTable[i]).add(
                                columnValues[dimColmapping[i]]);
                    }
                }
            }
            // load the data and create the catch 
            dimenionLoadCommandHelper.updateMemberCache(dimensionLoadInfo,
                    levelTypeColumnMap, tableName, columnNames,
                    columnIndex, columnAndMemberListaMap, levelAndCardinalityMap);
        }
        finally
        {
            MolapUtil.closeStreams(bufferedReader);
        }
        return true;
    }*/

    /**
     * processCSVRows
     * @param columnNames
     * @param columnIndex
     * @param isTimeDim
     * @param surrogateKeyGen
     * @param dimColmapping
     * @param output
     * @param propertyIndex
     * @param isKeyExceeded
     * @param data
     * @return
     * @throws KettleException
     */
    private boolean processCSVRows(String[] columnNames, int[] columnIndex,
            boolean isTimeDim, MolapCSVBasedDimSurrogateKeyGen surrogateKeyGen,
            int[] dimColmapping, int[] output, int[][] propertyIndex,String[] data) throws KettleException 
    {
        boolean isKeyExceeded=false;
        for(int i = 0;i < columnNames.length;i++)
        {
                String columnName= null;
                columnName = columnNames[i];
                String tuple = data[dimColmapping[i]];
                        
                Object[] propertyvalue = new Object[propertyIndex[i].length];

                for(int k = 0;k < propertyIndex[i].length;k++)
                {
                    String value = data[propertyIndex[i][k]];
                    
//                            Object value = resultSet
//                                    .getObject(origunalColumnName[propertyIndex[dimColmapping[i]][k]]);
                    if(null == value)
                    {
                        value = MolapCommonConstants.MEMBER_DEFAULT_VAL;
                    }
                    propertyvalue[k] = value;
                }
                if(null == tuple)
                {
                    tuple = MolapCommonConstants.MEMBER_DEFAULT_VAL;
                }
                if(isTimeDim)
                {
                    output[i] = surrogateKeyGen
                            .generateSurrogateKeysForTimeDims(tuple,
                                    columnName, columnIndex[i],
                                    propertyvalue);
                }
                else
                {
                    output[i] = surrogateKeyGen
                            .generateSurrogateKeys(tuple,
                                    columnName, columnIndex[i],
                                    propertyvalue);
                    
                }
                if(output[i]==-1)
                {
                    isKeyExceeded= true;
                }
        }
        return isKeyExceeded;
    }

    /**
     * 
     * @param tableName
     * @param primaryKeyColumnName
     * @param header
     * @param dimenionLoadCommandHelper
     * @return
     * 
     */
    private int getPrimaryColumnMap(String tableName,
            String primaryKeyColumnName, String header,
            DimenionLoadCommandHelper dimenionLoadCommandHelper)
    {
        int index = -1;

        String[] headerColumn = dimenionLoadCommandHelper.getRowData(header);

        for(int j = 0;j < headerColumn.length;j++)
        {
            if(primaryKeyColumnName.equalsIgnoreCase(tableName + '_'
                    + headerColumn[j]))
            {
                return j;
            }

        }
        return index;
    }

    /**
     * Return the dimension column mapping.
     * 
     * @param tableName
     * @param columnNames
     * @param header
     * @return
     * 
     */
    private int[] getDimColumnNameMapping(String tableName,
            String[] columnNames, String header,DimenionLoadCommandHelper commandHelper)
    {
        int []index = new int[columnNames.length];

        String[] headerColumn = commandHelper.getRowData(header);
  
        for(int i=0; i < columnNames.length; i++)
        {
            for(int j=0; j < headerColumn.length; j++)
            {
                if(columnNames[i].equalsIgnoreCase(tableName + '_' + headerColumn[j]))
                {
                    index[i] = j;
                    break;
                }
                
            }
        }
        return index;
    }
    
}

