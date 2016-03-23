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

package com.huawei.unibi.molap.csvload;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.etl.DataLoadingException;
import com.huawei.unibi.molap.olap.MolapDef.*;
import com.huawei.unibi.molap.schema.metadata.AggregateTable;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapUtil;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;

public final class GraphExecutionUtil {
    /**
     * Comment for <code>LOGGER</code>
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(GraphExecutionUtil.class.getName());

    private GraphExecutionUtil() {

    }

    /**
     * getCsvFileToRead
     *
     * @param csvFilePath
     * @return File
     */
    public static MolapFile getCsvFileToRead(String csvFilePath) {
        MolapFile csvFile =
                FileFactory.getMolapFile(csvFilePath, FileFactory.getFileType(csvFilePath));

        MolapFile[] listFiles = null;
        if (csvFile.isDirectory()) {
            listFiles = csvFile.listFiles(new MolapFileFilter() {
                @Override public boolean accept(MolapFile pathname) {
                    if (!pathname.isDirectory()) {
                        if (pathname.getName().endsWith(MolapCommonConstants.CSV_FILE_EXTENSION)
                                || pathname.getName().endsWith(
                                MolapCommonConstants.CSV_FILE_EXTENSION
                                        + MolapCommonConstants.FILE_INPROGRESS_STATUS)) {
                            return true;
                        }
                    }

                    return false;
                }
            });
        } else {
            listFiles = new MolapFile[1];
            listFiles[0] = csvFile;

        }

        return listFiles[0];
    }

    /**
     * @param measuresInCSVFile
     * @throws DataLoadingException
     */
    public static TextFileInputField[] getTextInputFiles(MolapFile csvFile,
            List<String> measureColumns, StringBuilder builder, StringBuilder measuresInCSVFile,
            String delimiter) throws DataLoadingException {
        DataInputStream fileReader = null;
        BufferedReader bufferedReader = null;
        String readLine = null;

        FileType fileType = FileFactory.getFileType(csvFile.getAbsolutePath());

        if (!csvFile.exists()) {
            csvFile = FileFactory.getMolapFile(
                    csvFile.getAbsolutePath() + MolapCommonConstants.FILE_INPROGRESS_STATUS,
                    fileType);
        }

        try {
            fileReader = FileFactory.getDataInputStream(csvFile.getAbsolutePath(), fileType);
            bufferedReader =
                    new BufferedReader(new InputStreamReader(fileReader, Charset.defaultCharset()));
            readLine = bufferedReader.readLine();
        } catch (FileNotFoundException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "CSV Input File not found  " + e.getMessage());
            throw new DataLoadingException("CSV Input File not found ", e);
        } catch (IOException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "Not able to read CSV input File  " + e.getMessage());
            throw new DataLoadingException("Not able to read CSV input File ", e);
        } finally {
            MolapUtil.closeStreams(fileReader, bufferedReader);
        }

        if (null != readLine) {
            String[] columnNames = readLine.split(delimiter);
            TextFileInputField[] textFileInputFields = new TextFileInputField[columnNames.length];

            int i = 0;
            String tmpCol;
            for (String column : columnNames) {
                tmpCol = column.replaceAll("\"", "");
                builder.append(tmpCol);
                builder.append(";");
                textFileInputFields[i] = new TextFileInputField();
                textFileInputFields[i].setName(tmpCol.trim());
                textFileInputFields[i].setType(2);
                measuresInCSVFile.append(tmpCol);
                measuresInCSVFile.append(";");
                i++;
            }

            return textFileInputFields;
        }

        return null;
    }

    /**
     * @param measuresInCSVFile
     * @throws DataLoadingException
     */
    public static TextFileInputField[] getTextInputFiles(String header, StringBuilder builder,
            StringBuilder measuresInCSVFile, String delimiter) throws DataLoadingException {

        String[] columnNames = header.split(delimiter);
        TextFileInputField[] textFileInputFields = new TextFileInputField[columnNames.length];

        int i = 0;
        String tmpCol;
        for (String columnName : columnNames) {
            tmpCol = columnName.replaceAll("\"", "");
            builder.append(tmpCol);
            builder.append(";");
            textFileInputFields[i] = new TextFileInputField();
            textFileInputFields[i].setName(tmpCol.trim());
            textFileInputFields[i].setType(2);
            measuresInCSVFile.append(tmpCol);
            measuresInCSVFile.append(";");
            i++;
        }

        return textFileInputFields;

    }

    public static boolean checkIsFolder(String csvFilePath) {
        try {
            if (FileFactory.isFileExist(csvFilePath, FileFactory.getFileType(csvFilePath), false)) {
                MolapFile molapFile =
                        FileFactory.getMolapFile(csvFilePath, FileFactory.getFileType(csvFilePath));
                return molapFile.isDirectory();
            }
        } catch (IOException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "Not able check path exists or not  " + e.getMessage() + "path: "
                            + csvFilePath);
        }

        return false;
    }

    /**
     * This method update the column Name
     *
     * @param cube
     * @param tableName
     * @param schema
     */
    public static Set<String> getSchemaColumnNames(Cube cube, String tableName, Schema schema) {
        Set<String> columnNames = new HashSet<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        String factTableName = MolapSchemaParser.getFactTableName(cube);
        if (tableName.equals(factTableName)) {
            CubeDimension[] dimensions = cube.dimensions;

            for (CubeDimension dimension : dimensions) {
                String foreignKey = dimension.foreignKey;
                if (null == foreignKey) {
                    Hierarchy[] extractHierarchies =
                            MolapSchemaParser.extractHierarchies(schema, dimension);

                    for (Hierarchy hier : extractHierarchies) {
                        Level[] levels = hier.levels;

                        for (Level level : levels) {
                            if (level.visible && null == level.parentname) {
                                columnNames.add(level.column.trim());
                            }
                        }
                    }
                } else {
                    if (dimension.visible) {
                        columnNames.add(foreignKey);
                    }
                }
            }

            Measure[] measures = cube.measures;
            for (Measure msr : measures) {
                /*if (false == msr.visible)
                {
                    continue;
                }*/
                if (!msr.visible) {
                    continue;
                }

                columnNames.add(msr.column);
            }
        } else {
            AggregateTable[] aggregateTable = MolapSchemaParser.getAggregateTable(cube, schema);

            for (AggregateTable aggTable : aggregateTable) {
                if (tableName.equals(aggTable.getAggregateTableName())) {
                    String[] aggLevels = aggTable.getAggLevels();
                    for (String aggLevel : aggLevels) {
                        columnNames.add(aggLevel);
                    }

                    String[] aggMeasure = aggTable.getAggMeasure();
                    for (String aggMsr : aggMeasure) {
                        columnNames.add(aggMsr);
                    }
                }
            }

        }

        return columnNames;

    }

    /**
     * @param csvFilePath
     * @param columnNames
     */
    public static boolean checkHeaderExist(String csvFilePath, String[] columnNames,
            String delimiter) {

        String readLine = readCSVFile(csvFilePath);

        if (null != readLine) {
            String[] columnFromCSV = readLine.split(delimiter);

            List<String> csvColumnsList =
                    new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);

            for (String column : columnFromCSV) {
                csvColumnsList.add(column.replaceAll("\"", ""));
            }

            for (String columns : columnNames) {
                if (csvColumnsList.contains(columns)) {
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

        try {
            fileReader = FileFactory
                    .getDataInputStream(csvFilePath, FileFactory.getFileType(csvFilePath));
            bufferedReader =
                    new BufferedReader(new InputStreamReader(fileReader, Charset.defaultCharset()));
            readLine = bufferedReader.readLine();

        } catch (FileNotFoundException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "CSV Input File not found  " + e.getMessage());
        } catch (IOException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    "Not able to read CSV input File  " + e.getMessage());
        } finally {
            MolapUtil.closeStreams(fileReader, bufferedReader);
        }
        return readLine;
    }

    /**
     * @param csvFilePath
     * @param columnNames
     * @return
     */
    public static boolean checkCSVAndRequestedTableColumns(String csvFilePath, String[] columnNames,
            String delimiter) {

        String readLine = readCSVFile(csvFilePath);

        if (null != readLine) {
            String[] columnFromCSV = readLine.split(delimiter);

            List<String> csvColumnsList =
                    new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);

            for (String column : columnFromCSV) {
                csvColumnsList.add(column.replaceAll("\"", "").trim());
            }

            int count = 0;

            for (String columns : columnNames) {
                if (csvColumnsList.contains(columns)) {
                    count++;
                }
            }

            return (count == columnNames.length);
        }

        return false;
    }

    /**
     * @param cube
     * @param schema
     * @return
     */
    public static boolean checkLevelCardinalityExists(Cube cube, Schema schema) {
        CubeDimension[] dimensions = cube.dimensions;

        for (CubeDimension dimension : dimensions) {
            Hierarchy[] extractHierarchies =
                    MolapSchemaParser.extractHierarchies(schema, dimension);

            for (Hierarchy hier : extractHierarchies) {
                Level[] levels = hier.levels;

                for (Level level : levels) {
                    if (-1 == level.levelCardinality) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    public static Set<String> getDimensionColumnNames(Cube cube, String factTableName,
            String dimTableName, Schema schema) {
        Set<String> columnNames = new HashSet<String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        String factTableNameLocal = MolapSchemaParser.getFactTableName(cube);
        if (factTableName.equals(factTableNameLocal)) {
            CubeDimension[] dimensions = cube.dimensions;

            for (CubeDimension dimension : dimensions) {
                Hierarchy[] extractHierarchies =
                        MolapSchemaParser.extractHierarchies(schema, dimension);

                for (Hierarchy hier : extractHierarchies) {
                    RelationOrJoin relation = hier.relation;
                    String tableName = relation == null ? null : ((Table) hier.relation).name;
                    if (null != tableName && tableName.equalsIgnoreCase(dimTableName)) {
                        Level[] levels = hier.levels;

                        for (Level level : levels) {
                            columnNames.add(level.column.trim());
                        }
                    }

                }
            }
        }

        return columnNames;
    }
}
