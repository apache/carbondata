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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfnVI3c/udSMK6An9Lipq6FjccIMKj41/T4EBXl
K2tBNz3wq1nUUJcMDF2YAdLEUNsJiYYt4L9TGbeXoGvPWVTL48oxhn9sNcA10nOj6MT4TNUn
gmcisYFFIf0yLhLz5KIP8VFSxDDpHVQG3PMd6WC8ZlvYkCGXZ1zJDdXY2mSGTQ==*/
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

package com.huawei.unibi.molap.csvload;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.etl.DataLoadingException;
import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.olap.MolapDef.Hierarchy;
import com.huawei.unibi.molap.olap.MolapDef.Level;
import com.huawei.unibi.molap.olap.MolapDef.Measure;
import com.huawei.unibi.molap.olap.MolapDef.RelationOrJoin;
import com.huawei.unibi.molap.olap.MolapDef.Schema;
import com.huawei.unibi.molap.olap.MolapDef.Table;
import com.huawei.unibi.molap.schema.metadata.AggregateTable;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :18-Jul-2013 1:08:01 PM
 * FileName : GraphExecutionUtil.java
 * Class Description :
 * Version 1.0
 */
public final class GraphExecutionUtil
{
    private GraphExecutionUtil()
    {
    	
    }
    /**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(GraphExecutionUtil.class.getName());
    
    /**
     * getCsvFileToRead
     * @param csvFilePath
     * @return File
     */
    public static MolapFile getCsvFileToRead(String csvFilePath)
    {
        MolapFile csvFile = FileFactory.getMolapFile(csvFilePath, FileFactory.getFileType(csvFilePath));
        
        MolapFile[] listFiles = null;
        if(csvFile.isDirectory())
        {
            listFiles = csvFile.listFiles(new MolapFileFilter()
            {
                @Override
                public boolean accept(MolapFile pathname)
                {
                    if(!pathname.isDirectory())
                    {
                        if(pathname.getName().endsWith(
                                MolapCommonConstants.CSV_FILE_EXTENSION)
                                || pathname
                                        .getName()
                                        .endsWith(
                                                MolapCommonConstants.CSV_FILE_EXTENSION
                                                        + MolapCommonConstants.FILE_INPROGRESS_STATUS))
                        {
                            return true;
                        }
                    }
                    
                    return false;
                }
            });
        }
        else
        {
            listFiles = new MolapFile[1];
            listFiles[0] = csvFile;
            
        }
        
        return listFiles[0];
    }
    /**
     * 
     * @param measuresInCSVFile 
     * @param csvFilePath
     * @return
     * @throws DataLoadingException 
     * 
     */
    public static TextFileInputField[] getTextInputFiles(MolapFile csvFile,List<String> measureColumns, StringBuilder builder, 
    		StringBuilder measuresInCSVFile, String delimiter) throws DataLoadingException
    {
        DataInputStream fileReader = null;
        BufferedReader bufferedReader = null;
        String readLine = null;
        
        FileType fileType = FileFactory.getFileType(csvFile.getAbsolutePath());
        
        if(!csvFile.exists())
        {
            csvFile = FileFactory.getMolapFile(csvFile.getAbsolutePath() + MolapCommonConstants.FILE_INPROGRESS_STATUS,fileType);
        }

        try
        {
            fileReader = FileFactory.getDataInputStream(csvFile.getAbsolutePath(), fileType);
            bufferedReader = new BufferedReader(new InputStreamReader(fileReader, Charset.defaultCharset()));
            readLine = bufferedReader.readLine();
        }
        catch(FileNotFoundException e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, 
                    e, "CSV Input File not found  " + e.getMessage());
            throw new DataLoadingException(
                    "CSV Input File not found ", e);
        }
        catch(IOException e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, 
                    e, "Not able to read CSV input File  " + e.getMessage());
            throw new DataLoadingException(
                    "Not able to read CSV input File ", e);
        }
        finally
        {
            MolapUtil.closeStreams(fileReader, bufferedReader);
        }
        
        if(null != readLine)
        {
            String[] columnNames = readLine.split(delimiter);
            TextFileInputField []textFileInputFields = new TextFileInputField[columnNames.length];
            
            int i=0;
            String tmpCol;
            for(String column : columnNames)
            {
                tmpCol = column.replaceAll("\"", "");
                builder.append(tmpCol);
                builder.append(";");
                textFileInputFields[i]= new TextFileInputField();
                textFileInputFields[i].setName(tmpCol.trim());
//                if(contains(measureColumns,tmpCol.trim()))
//                {
//                    textFileInputFields[i].setType(1);
                    textFileInputFields[i].setType(2);
                    measuresInCSVFile.append(tmpCol);
                    measuresInCSVFile.append(";");
//                }
//                else
//                {
//                    textFileInputFields[i].setType(2);
//                }
                i++;
            }
            
            return textFileInputFields;
        }

        return null;
    }
    
    
    /**
     * 
     * @param measuresInCSVFile 
     * @param csvFilePath
     * @return
     * @throws DataLoadingException 
     * 
     */
    public static TextFileInputField[] getTextInputFiles(String header, StringBuilder builder, 
    		StringBuilder measuresInCSVFile, String delimiter) throws DataLoadingException
    {

        String[] columnNames = header.split(delimiter);
        TextFileInputField []textFileInputFields = new TextFileInputField[columnNames.length];
        
        int i=0;
        String tmpCol; 
        for(String columnName : columnNames)
        {
            tmpCol = columnName.replaceAll("\"", "");
            builder.append(tmpCol);
            builder.append(";");
            textFileInputFields[i]= new TextFileInputField();
            textFileInputFields[i].setName(tmpCol.trim());
            textFileInputFields[i].setType(2);
            measuresInCSVFile.append(tmpCol);
            measuresInCSVFile.append(";");
            i++;
        }
        
        return textFileInputFields;
    
    }
    
//    private static boolean contains(List<String> coList, String cols)
//    {
//        for(String col : coList)
//        {
//            if(col.equalsIgnoreCase(cols))
//            {
//                return true;
//            }
//        }
//        return false;
//    }
    
    /**
     * 
     * 
     * @param csvFilePath
     * @return
     *
     */
    public static boolean checkIsFolder(String csvFilePath)
    {
        try
        {
            if(FileFactory.isFileExist(csvFilePath, FileFactory.getFileType(csvFilePath), false))
            {
                MolapFile molapFile = FileFactory.getMolapFile(csvFilePath, FileFactory.getFileType(csvFilePath));
                return molapFile.isDirectory();
            }
        }
        catch(IOException e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, 
                    e, "Not able check path exists or not  " + e.getMessage() + "path: " + csvFilePath);
        } 
        
        return false;
    }
    
    /**
     * This method reads the csv file and checks the header count length and 
     * data length is same or not, may be the data is corrupted.
     * 
     * @param csvFile
     * @return
     * @throws DataLoadingException 
     *
     */
//    public static boolean checkDataColumnCountMatch(File csvFile) throws DataLoadingException
//    {
//
//        FileReader fileReader = null;
//        BufferedReader bufferedReader = null;
//        String headerLine = null;
//        List<String> data = new ArrayList<String>(2);
//        boolean isHeader = true;
//        int count = 0;
//
//        try
//        {
//            fileReader = new FileReader(csvFile);
//            bufferedReader = new BufferedReader(fileReader);
//            String line = bufferedReader.readLine();
//            while(null != line && (count < 2))
//            {
//                if(isHeader)
//                {
//                    headerLine = line;
//                    isHeader = false;
//                }
//                else
//                {
//                    if(!line.isEmpty())
//                    {
//                        data.add(line);
//                        count++;
//                    }
//                }
//
//                line = bufferedReader.readLine();
//
//            }
//        }
//        catch(FileNotFoundException e)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, 
//                    e, "CSV Input File not found  " + e.getMessage());
//            throw new DataLoadingException(
//                    "CSV Input File not found ", e);
//        }
//        catch(IOException e)
//        {
//            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, 
//                    e, "Not able to read CSV input File  " + e.getMessage());
//            throw new DataLoadingException(
//                    "Not able to read CSV input File ", e);
//        }
//        finally
//        {
//            MolapUtil.closeStreams(fileReader, bufferedReader);
//        }
//
//        String[] headerColumns = null;
//        
//        if (null != headerLine)
//        {
//            headerColumns = getRowData(headerLine);
//        }
//        else
//        {
//            headerColumns = new String[0];
//        }
//        
//        int size = data.size();
//        switch(size)
//        {
//        case 1 : 
//            String rowData = data.get(0);
//            
//            String[] splittedData = getRowData(rowData);
//            
//            if(splittedData.length == headerColumns.length)
//            {
//                return true;
//            }
//            else
//            {
//                int length = splittedData.length;
//                if(length == 1)
//                {
//                    if(splittedData[0].isEmpty())
//                    {
//                        return true;
//                    }
//                }
//                else
//                {
//                    return false;
//                }
//                    
//            }
//                
//            break;
//        case 2: 
//            String rowData1 = data.get(1);
//            
//            String[] splittedData1 = getRowData(rowData1);
//            
//            if(splittedData1.length == headerColumns.length)
//            {
//                return true;
//            }
//            break;
//            
//         default :
//             
//             return true;
//            
//        }
//        
//        return false;
//    }
    
    /**
     * Return the data row
     * 
     * @param data
     * @return
     *
     */
//    private static String[] getRowData(String data)
//    {
//        if(data.indexOf("\"") != 0 && data.lastIndexOf("\"") != data.length())
//        {
//            return data.split(",");
//        }
//        
//        List<String> records = new ArrayList<String>();
//        
//        addRecordsRecursively(data, records);
//        
//        return records.toArray(new String[records.size()]);
//    }
    
    /**
     * Recursively split the records and return the data as comes 
     * under quotes.
     * 
     * @param data
     * @param records
     * 
     * For Example : 
     * If data comes like:
     * String s = ""13569,69600000","SN=66167523766568","NE=66167522854161""
     * then it will return {"13569,69600000","SN=66167523766568","NE=66167522854161"}
     * 
     * and If String is without quotes:
     * String s = "13569,69600000,SN=66167523766568,NE=66167522854161"
     * then it will return {"13569","69600000","SN=66167523766568","NE=66167522854161"}
     */
//    private static void addRecordsRecursively(String data, List<String> records)
//    {
//       if(data.length() == 0)
//       {
//           return;
//       }
//       
//       int secondIndexOfQuotes = data.indexOf("\"" , 1);
//       records.add(data.substring(1, secondIndexOfQuotes));
//
//       if(data.length()-1 != secondIndexOfQuotes)
//       {
//           data = data.substring(secondIndexOfQuotes + 2);
//       }
//       else
//       {
//           data = data.substring(secondIndexOfQuotes + 1);
//       }
//       
//       //call recursively
//       addRecordsRecursively(data, records);
//    }
    
    /**
     * This method update the column Name 
     * @param columnNames
     * @param cube
     * @param tableName 
     * @param schema 
     * 
     */
    public static Set<String> getSchemaColumnNames(Cube cube, String tableName, Schema schema)
    {
        Set<String> columnNames  = new HashSet<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        String factTableName = MolapSchemaParser.getFactTableName(cube);
        if(tableName.equals(factTableName))
        {
            CubeDimension[] dimensions = cube.dimensions;

            for(CubeDimension dimension : dimensions)
            {
                String foreignKey = dimension.foreignKey;
                if(null == foreignKey)
                {
                    Hierarchy[] extractHierarchies = MolapSchemaParser
                            .extractHierarchies(schema, dimension);

                    for(Hierarchy hier : extractHierarchies)
                    {
                        Level[] levels = hier.levels;

                        for(Level level : levels)
                        {
                            if (level.visible && null == level.parentname)
                            {
                                columnNames.add(level.column.trim());
                            }
                        }
                    }
                }
                else
                {
                	if(dimension.visible)
                	{
                		columnNames.add(foreignKey);
                	}
                }
            }
            
            Measure[] measures = cube.measures;
            for(Measure msr : measures)
            {
                /*if (false == msr.visible)
                {
                    continue;
                }*/
            	if (!msr.visible)
                {
                    continue;
                }
            		
                columnNames.add(msr.column);
            }
        }
        else
        {
            AggregateTable[] aggregateTable = MolapSchemaParser
                    .getAggregateTable(cube, schema);

            for(AggregateTable aggTable : aggregateTable)
            {
                if(tableName.equals(aggTable.getAggregateTableName()))
                {
                    String[] aggLevels = aggTable.getAggLevels();
                    for(String aggLevel : aggLevels)
                    {
                        columnNames.add(aggLevel);
                    }

                    String[] aggMeasure = aggTable.getAggMeasure();
                    for(String aggMsr : aggMeasure)
                    {
                        columnNames.add(aggMsr);
                    }
                }
            }

        }
        
        return columnNames;

    }
    /**
     * 
     * @param csvFilePath
     * @param columnNames
     * 
     */
    public static boolean checkHeaderExist(String csvFilePath, String[] columnNames, String delimiter)
    {

        String readLine = readCSVFile(csvFilePath);

        if(null != readLine)
        {
            String[] columnFromCSV = readLine.split(delimiter);
            
           List<String> csvColumnsList = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
           
           for(String column : columnFromCSV)
           {
               csvColumnsList.add(column.replaceAll("\"", ""));
           }
           
           
           for(String columns : columnNames)
           {
               if(csvColumnsList.contains(columns))
               {
                   return true;
               }
           }
        }

        return false;
    }
	/**
	 * @param csvFilePath
	 * @return
	 */
	private static String readCSVFile(String csvFilePath) {
		
		DataInputStream fileReader = null;
        BufferedReader bufferedReader = null;
        String readLine = null;

        try
        {
            fileReader = FileFactory.getDataInputStream(csvFilePath, FileFactory.getFileType(csvFilePath));
            bufferedReader = new BufferedReader(new InputStreamReader(fileReader,Charset.defaultCharset()));
            readLine = bufferedReader.readLine();
            
        }
        catch(FileNotFoundException e)
        {
            LOGGER.error(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "CSV Input File not found  " + e.getMessage());
        }
        catch(IOException e)
        {
            LOGGER.error(
                    MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "Not able to read CSV input File  " + e.getMessage());
        }
        finally
        {
            MolapUtil.closeStreams(fileReader, bufferedReader);
        }
		return readLine;
	}
    /**
     * 
     * @param csvFilePath
     * @param columnNames
     * @return
     * 
     */
    public static boolean checkCSVAndRequestedTableColumns(String csvFilePath,
            String[] columnNames, String delimiter)
    {

        String readLine = readCSVFile(csvFilePath);

        if(null != readLine)
        {
            String[] columnFromCSV = readLine.split(delimiter);

            List<String> csvColumnsList = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);

            for(String column : columnFromCSV)
            {
                csvColumnsList.add(column.replaceAll("\"", "").trim());
            }

            int count = 0;

            for(String columns : columnNames)
            {
                if(csvColumnsList.contains(columns))
                {
                    count++;
                }
            }

            return (count == columnNames.length);
          /*  if(count == columnNames.length)
            {
                return true;
            }
            else
            {
                return false;
            }*/
        }

        return false;
    }
    /**
     * 
     * @param cube
     * @param schema 
     * @return
     * 
     */
    public static boolean checkLevelCardinalityExists(Cube cube, Schema schema)
    {
        CubeDimension[] dimensions = cube.dimensions;

        for(CubeDimension dimension : dimensions)
        {
            Hierarchy[] extractHierarchies = MolapSchemaParser
                    .extractHierarchies(schema, dimension);

            for(Hierarchy hier : extractHierarchies)
            {
                Level[] levels = hier.levels;

                for(Level level : levels)
                {
                    if(-1 == level.levelCardinality)
                    {
                        return false;
                    }
                }
            }
        }

        return true;
    }
    /**
     * 
     * @param schemaInfo
     * @param cubeName
     * @param tableName
     * 
     */
//    public static boolean checkKeyOrdinalDefined(SchemaInfo schemaInfo,
//            String cubeName, String tableName)
//    {
//        
//        Schema schema = MolapSchemaParser.loadXML(schemaInfo.getSchemaPath());
//        Cube cube = MolapSchemaParser.getMondrianCube(schema,
//                schemaInfo.getCubeName());
//        Map<Integer, Boolean> levelsKeyOrdinal = new HashMap<Integer, Boolean>();
//        
//        String factTableName = MolapSchemaParser.getFactTableName(cube);
//        int keyOrdinalNOtDefined = 0;
//        
//        if(!tableName.equals(factTableName))
//        {
//            return true;
//        }
//        
//        CubeDimension[] dimensions = cube.dimensions;
//        
//        int counter = 0;
//        for(CubeDimension cDimension : dimensions)
//        {
//            Hierarchy[] hierarchies =  null;
//            hierarchies = MolapSchemaParser.extractHierarchies(schema, cDimension);
//            for(Hierarchy hierarchy : hierarchies)
//            {
//                for(Level level : hierarchy.levels)
//                {
//                    counter++;
//
//                    Integer keyOrdinal = level.keyOrdinal;
//
//                    if(-1 == keyOrdinal)
//                    {
//                        keyOrdinalNOtDefined++;
//                        break;
//                    }
//
//                    if(null == levelsKeyOrdinal.get(keyOrdinal))
//                    {
//                        levelsKeyOrdinal.put(keyOrdinal, true);
//                    }
//                }
//
//            }
//        }
//        
//        if(counter == keyOrdinalNOtDefined)
//        {
//            return true;
//        }
//        if(levelsKeyOrdinal.size() < counter)
//        {
//            return false;
//        }
//
//        return true;
//        
//    }
    /**
     * 
     * @param cube
     * @param factTableName
     * @param dimTableName
     * @param schema
     * @return
     * 
     */
    public static Set<String> getDimensionColumnNames(Cube cube,
            String factTableName, String dimTableName, Schema schema)
    {
        Set<String> columnNames  = new HashSet<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        
        String factTableNameLocal = MolapSchemaParser.getFactTableName(cube);
        if(factTableName.equals(factTableNameLocal))
        {
            CubeDimension[] dimensions = cube.dimensions;

            for(CubeDimension dimension : dimensions)
            {
                Hierarchy[] extractHierarchies = MolapSchemaParser
                        .extractHierarchies(schema, dimension);

                for(Hierarchy hier : extractHierarchies)
                {
                    RelationOrJoin relation = hier.relation;
                    String tableName = relation==null?null:((Table)hier.relation).name;
                    if(null != tableName && tableName.equalsIgnoreCase(dimTableName))
                    {
                        Level[] levels = hier.levels;
                        
                        for(Level level : levels)
                        {
                            columnNames.add(level.column.trim());
                        }
                    }
                    
                }
            }
        }
        
        return columnNames;
    }
}
