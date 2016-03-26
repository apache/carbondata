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

package org.carbondata.processing.surrogatekeysgenerator.dbbased;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.processing.schema.metadata.HierarchiesInfo;
import org.carbondata.processing.util.MolapDataProcessorUtil;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.w3c.dom.Node;

public class MolapSeqGenStepMeta extends BaseStepMeta implements StepMetaInterface, Cloneable {

    /**
     * pkg
     */
    private static final Class<?> PKG = MolapSeqGenStepMeta.class; // for i18n
    // purposes
    /**
     * ROW_COUNT_INFO
     */
    private static final String ROW_COUNT_INFO = "rowcount";
    /**
     * hier name
     */
    protected String[] hierNames;
    /**
     * dims
     */
    protected int[] dims;
    /**
     * dimLens
     */
    protected int[] dimLens;
    /**
     * dims
     */
    protected boolean[] dimPresent;
    protected int normLength;
    /**
     * msrs
     */
    protected int[] msrs;
    /**
     * timehierName
     */
    protected String timehierName;
    /**
     * hirches
     */
    protected Map<String, int[]> hirches;
    /**
     * timeFormat
     */
    protected SimpleDateFormat timeFormat;
    /**
     * timeIndex
     */
    protected int timeIndex = -1;
    /**
     * timeDimeIndex
     */
    protected int timeDimeIndex = -1;
    /**
     * timeLevels
     */
    protected String[] timeLevels = new String[0];
    /**
     * timeOrdinalCols
     */
    protected String[] timeOrdinalCols = new String[0];
    /**
     * timeOrdinalIndices
     */
    protected int[] timeOrdinalIndices = new int[0];
    /**
     * dimColNames
     */
    protected String[] dimColNames;
    /**
     * measureColumn
     */
    protected String[] measureColumn;
    /**
     * measureNames
     */
    protected String[] measureNames;
    /**
     * Mrs Aggregator.
     */
    protected String[] msrAggregators;
    /**
     * Primary key containing columns
     */
    protected Map<String, String[]> primaryKeyColumnMap;
    /**
     * Foreign key and respective hierarchy Map
     */
    protected Map<String, String> foreignKeyHierarchyMap;
    /**
     * molapdim
     */
    private String molapdim;
    /**
     * molapProps
     */
    private String molapProps;
    /**
     * molapmsr
     */
    private String molapmsr;
    /**
     * molaphier
     */
    private String molaphier;
    /**
     * molapMeasureNames
     */
    private String molapMeasureNames;
    /**
     * molapTime
     */
    private String molapTime;
    /**
     * storeLocation
     */
    private String storeLocation;
    /**
     * molapJNDI
     */
    private String molapJNDI;
    /**
     * molapSchema
     */
    private String molapSchema;
    /**
     * batchSize
     */
    private int batchSize = 10000;
    /**
     * isAggregate
     */
    private boolean isAggregate;
    /**
     * generateDimFiles
     */
    private boolean generateDimFiles;
    /**
     * storeType
     */
    private String storeType;
    /**
     * metaHeirSQLQuery
     */
    private String metaHeirSQLQuery;
    /**
     * molapMetaHier
     */
    private String molapMetaHier;
    /**
     * propColumns
     */
    private List<String>[] propColumns;
    /**
     * propTypes
     */
    private List<String>[] propTypes;
    /**
     * propIndxs
     */
    private int[][] propIndxs;
    /**
     * metahierVoList
     */
    private List<HierarchiesInfo> metahierVoList;
    /**
     * rowCountMap
     */
    private Map<String, Integer> rowCountMap;
    /**
     * dimesionTableNames
     */
    private String dimesionTableNames;
    /**
     * tableName
     */
    private String tableName;
    /**
     * msrAggregatorString
     */
    private String msrAggregatorString;
    /**
     * MOdified Dimension
     */
    private String[] modifiedDimension;
    /**
     * heirKeySize
     */
    private String heirKeySize;
    /**
     * primary key containing column names string
     */
    private String primaryKeyColumnNamesString;

    /**
     * Foreign key and respective hierarchy String
     */
    private String foreignKeyHierarchyString;

    private int currentRestructNumber;

    public MolapSeqGenStepMeta() {
        super();
    }

    /**
     * @return Returns the storeType.
     */
    public String getStoreType() {
        return storeType;
    }

    /**
     * @param storeType The storeType to set.
     */
    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    /**
     * @return the molapSchema
     */
    public String getMolapSchema() {
        return molapSchema;
    }

    /**
     * @param molapSchema the molapSchema to set
     */
    public void setMolapSchema(String molapSchema) {
        this.molapSchema = molapSchema;
    }

    /**
     * @return the molapCon
     */
    public String getMolapJNDIName() {
        return molapJNDI;
    }

    public void setMolapJNDIName(String jndiName) {
        this.molapJNDI = jndiName;
    }

    public List<HierarchiesInfo> getMetahierVoList() {
        return metahierVoList;
    }

    public void setMetahierVoList(List<HierarchiesInfo> metahierVoList) {
        this.metahierVoList = metahierVoList;
    }

    /**
     * @return the molapLocation
     */
    public String getStoreLocation() {
        return storeLocation;
    }

    public void setStoreLocation(String molapLocation) {
        this.storeLocation = molapLocation;
    }

    public String getMolapMetaHier() {
        return molapMetaHier;
    }

    public void setMolapMetaHier(String molapMetaHier) {
        this.molapMetaHier = molapMetaHier;
    }

    public String getMetaHeirSQLQueries() {
        return metaHeirSQLQuery;
    }

    public void setMetaMetaHeirSQLQueries(String metaHeirSQLQuery) {
        this.metaHeirSQLQuery = metaHeirSQLQuery;
    }

    /**
     * @return Returns the isInitialLoad.
     */
    public boolean isAggregate() {
        return isAggregate;
    }

    public void setAggregate(boolean isAggregate) {
        this.isAggregate = isAggregate;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getMolapTime() {
        return molapTime;
    }

    public void setMolapTime(String molapTime) {
        this.molapTime = molapTime;
    }

    // getters and setters for the step settings

    public String getMolapProps() {
        return molapProps;
    }

    public void setMolapProps(String molapProps) {
        this.molapProps = molapProps;
    }

    public String getMolapdim() {
        return molapdim;
    }

    public void setMolapdim(String molapdim) {
        this.molapdim = molapdim;
    }

    public String getMolapmsr() {
        return molapmsr;
    }

    public void setMolapmsr(String molapmsr) {

        this.molapmsr = molapmsr;
    }

    public String getMolapHier() {
        return molaphier;
    }

    public void setMolaphier(String molaphier) {
        this.molaphier = molaphier;
    }

    /**
     * @return Returns the generateDimFiles.
     */
    public boolean isGenerateDimFiles() {
        return generateDimFiles;
    }

    /**
     * @param generateDimFiles The generateDimFiles to set.
     */
    public void setGenerateDimFiles(boolean generateDimFiles) {
        this.generateDimFiles = generateDimFiles;
    }

    /**
     * set sensible defaults for a new step
     */
    public void setDefault() {
        molapProps = "";
        molapdim = "";
        molapmsr = "";
        molaphier = "";
        molapTime = "";
        storeLocation = "";
        //
        molapJNDI = "";
        molapSchema = "";
        storeType = "";
        isAggregate = false;
        metaHeirSQLQuery = "";
        molapMetaHier = "";
        dimesionTableNames = "";
        tableName = "";
        molapMeasureNames = "";
        msrAggregatorString = "";
        heirKeySize = "";
        primaryKeyColumnNamesString = "";
        foreignKeyHierarchyString = "";
        currentRestructNumber = -1;
        //
    }

    // helper method to allocate the arrays
    public void allocate(int nrkeys) {

    }

    public Object clone() {

        // field by field copy is default
        MolapSeqGenStepMeta retval = (MolapSeqGenStepMeta) super.clone();

        return retval;
    }

    /**
     * @see BaseStepMeta#getXML()
     */
    public String getXML() throws KettleValueException {
        //
        StringBuffer retval = new StringBuffer(150);
        //
        retval.append("    ").append(XMLHandler.addTagValue("molapProps", molapProps));
        retval.append("    ").append(XMLHandler.addTagValue("dim", molapdim));
        retval.append("    ").append(XMLHandler.addTagValue("msr", molapmsr));
        retval.append("    ").append(XMLHandler.addTagValue("hier", molaphier));
        retval.append("    ").append(XMLHandler.addTagValue("time", molapTime));
        retval.append("    ").append(XMLHandler.addTagValue("loc", storeLocation));
        retval.append("    ").append(XMLHandler.addTagValue("con", molapJNDI));
        //
        retval.append("    ").append(XMLHandler.addTagValue("batchSize", batchSize));
        retval.append("    ").append(XMLHandler.addTagValue("genDimFiles", generateDimFiles));
        retval.append("    ").append(XMLHandler.addTagValue("isAggregate", isAggregate));
        retval.append("    ").append(XMLHandler.addTagValue("storeType", storeType));
        retval.append("    ").append(XMLHandler.addTagValue("metadataFilePath", metaHeirSQLQuery));
        retval.append("    ").append(XMLHandler.addTagValue("molapMetaHier", molapMetaHier));
        retval.append("    ")
                .append(XMLHandler.addTagValue("molapMeasureNames", molapMeasureNames));
        retval.append("    ")
                .append(XMLHandler.addTagValue("dimHierReleation", dimesionTableNames));
        retval.append("    ").append(XMLHandler.addTagValue("factOrAggTable", tableName));
        retval.append("    ")
                .append(XMLHandler.addTagValue("msrAggregatorString", msrAggregatorString));
        retval.append("    ").append(XMLHandler.addTagValue("heirKeySize", heirKeySize));
        retval.append("    ").append(XMLHandler
                .addTagValue("primaryKeyColumnNamesString", primaryKeyColumnNamesString));
        retval.append("    ").append(XMLHandler
                .addTagValue("foreignKeyHierarchyString", foreignKeyHierarchyString));
        retval.append("    ")
                .append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        //
        return retval.toString();
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleXMLException {

        try {

            molapProps = XMLHandler.getTagValue(stepnode, "molapProps");
            molapdim = XMLHandler.getTagValue(stepnode, "dim");
            molapmsr = XMLHandler.getTagValue(stepnode, "msr");
            molaphier = XMLHandler.getTagValue(stepnode, "hier");
            molapTime = XMLHandler.getTagValue(stepnode, "time");
            storeLocation = XMLHandler.getTagValue(stepnode, "loc");
            molapJNDI = XMLHandler.getTagValue(stepnode, "con");
            molapMetaHier = XMLHandler.getTagValue(stepnode, "molapMetaHier");
            molapMeasureNames = XMLHandler.getTagValue(stepnode, "molapMeasureNames");
            dimesionTableNames = XMLHandler.getTagValue(stepnode, "dimHierReleation");
            tableName = XMLHandler.getTagValue(stepnode, "factOrAggTable");
            msrAggregatorString = XMLHandler.getTagValue(stepnode, "msrAggregatorString");
            heirKeySize = XMLHandler.getTagValue(stepnode, "heirKeySize");
            primaryKeyColumnNamesString =
                    XMLHandler.getTagValue(stepnode, "primaryKeyColumnNamesString");
            foreignKeyHierarchyString =
                    XMLHandler.getTagValue(stepnode, "foreignKeyHierarchyString");
            String batchConfig = XMLHandler.getTagValue(stepnode, "batchSize");
            String dimeFileConfig = XMLHandler.getTagValue(stepnode, "genDimFiles");
            currentRestructNumber =
                    Integer.parseInt(XMLHandler.getTagValue(stepnode, "currentRestructNumber"));

            if (batchConfig != null) {
                batchSize = Integer.parseInt(batchConfig);
            }

            if (dimeFileConfig != null) {
                generateDimFiles = Boolean.parseBoolean(dimeFileConfig);
            }

            metaHeirSQLQuery = XMLHandler.getTagValue(stepnode, "metadataFilePath");
            storeType = XMLHandler.getTagValue(stepnode, "storeType");

            isAggregate = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "isAggregate"));

            int nrKeys = XMLHandler.countNodes(stepnode, "lookup");
            allocate(nrKeys);

        } catch (Exception e) {
            throw new KettleXMLException("Template Plugin Unable to read step info from XML node",
                    e);
        }

    }

    public void initialize() throws KettleException {
        try {
            updateDimensions(molapdim, molapmsr);

            hirches = getHierarichies(molaphier);

            measureNames = getMeasureNamesArray(molapMeasureNames);

            updateMeasureAggregator(msrAggregatorString);

            primaryKeyColumnMap = getPrimaryKeyColumnMap(primaryKeyColumnNamesString);

            foreignKeyHierarchyMap = getForeignKeyHierMap(foreignKeyHierarchyString);

            getTimeHierarichies(molapTime);

            if (timeIndex >= 0) {
                for (int j = 0; j < msrs.length; j++) {
                    if (msrs[j] >= timeIndex) {
                        if (timeLevels != null) {
                            msrs[j] = msrs[j] + timeLevels.length - 1;
                        }
                    }
                }
            }

            //update non time dimension properties
            updateDimProperties();

            if (timeIndex >= 0) {
                //update time dimension properties
                updateTimeDimProps();
            }
            //update the meta Hierarichies list
            getMetaHierarichies(molapMetaHier);

            //intitalize rowCountMap
            rowCountMap =
                    new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

            updateMetaHierarichiesWithQueries(metaHeirSQLQuery);

            updateRowCountMap();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    private Map<String, String> getForeignKeyHierMap(String foreignKeyHierarchyString) {
        if (foreignKeyHierarchyString == null || "".equals(foreignKeyHierarchyString)) {
            return new HashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        }
        Map<String, String> map =
                new HashMap<String, String>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        String[] hies = foreignKeyHierarchyString.split("&");

        for (int i = 0; i < hies.length; i++) {
            String[] foreignHierArray = hies[i].split(":");

            map.put(foreignHierArray[0], foreignHierArray[1]);
        }
        return map;
    }

    private Map<String, String[]> getPrimaryKeyColumnMap(String primaryKeyColumnNamesString) {
        if (primaryKeyColumnNamesString == null || "".equals(primaryKeyColumnNamesString)) {
            return new LinkedHashMap<String, String[]>();
        }
        Map<String, String[]> map = new LinkedHashMap<String, String[]>();

        String[] hies = primaryKeyColumnNamesString.split("&");

        for (int i = 0; i < hies.length; i++) {
            String hie = hies[i];

            String hierName = hie.substring(0, hie.indexOf(":"));

            String[] columnArray =
                    getStringArray(hie.substring(hie.indexOf(":") + 1, hie.length()));
            map.put(hierName, columnArray);
        }
        return map;
    }

    private String[] getStringArray(String columnNames) {
        String[] splitedColumnNames = columnNames.split(",");
        String[] columns = new String[splitedColumnNames.length];

        System.arraycopy(splitedColumnNames, 0, columns, 0, columns.length);
        return columns;
    }

    public void updateHierMappings(RowMetaInterface metaInterface) {
        hierNames = new String[primaryKeyColumnMap.size()];
        Set<String> primKey = primaryKeyColumnMap.keySet();
        String[] primary = primKey.toArray(new String[primKey.size()]);
        int k = 0;
        for (int j = 0; j < primaryKeyColumnMap.size(); j++) {
            String foreignKey = primary[j];
            String actualHier = foreignKeyHierarchyMap.get(foreignKey);
            if (null != actualHier && foreignKey != null) {
                hierNames[k++] = actualHier;
            }
        }
    }

    private void updateMeasureAggregator(String msrAggregatorString) {
        String[] split = msrAggregatorString.split(";");
        msrAggregators = new String[split.length];
        System.arraycopy(split, 0, msrAggregators, 0, split.length);
    }

    private String[] getMeasureNamesArray(String molapMeasureNames) {
        if (molapMeasureNames == null || "".equals(molapMeasureNames)) {
            return new String[0];
        }

        String[] foreignKeys = molapMeasureNames.split("&");

        String[] measureName = new String[foreignKeys.length];

        System.arraycopy(foreignKeys, 0, measureName, 0, foreignKeys.length);
        return measureName;
    }

    private void updateRowCountMap() throws KettleException {
        // check first the rowCounter file Exists
        String storeLocation = MolapUtil.getCarbonStorePath(null, null);
        storeLocation = storeLocation + File.separator + getStoreLocation();

        int restructFolderNumber = currentRestructNumber;

        storeLocation = storeLocation + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                + restructFolderNumber + File.separator + ROW_COUNT_INFO;
        File rowCountFile = new File(storeLocation);

        if (!rowCountFile.exists()) {
            return;
        }
        FileInputStream fileInputStream = null;
        FileChannel fileChannel = null;

        try {

            fileInputStream = new FileInputStream(rowCountFile);
            fileChannel = fileInputStream.getChannel();

            long size = fileChannel.size();

            String tableName = "";

            while (fileChannel.position() < size) {
                ByteBuffer totoalLength = ByteBuffer.allocate(4);

                fileChannel.read(totoalLength);
                totoalLength.rewind();
                int tablelen = totoalLength.getInt();
                //
                ByteBuffer tableInfo = ByteBuffer.allocate(tablelen);
                fileChannel.read(tableInfo);
                tableInfo.rewind();
                int toread = tableInfo.getInt();
                byte[] bb = new byte[toread];
                tableInfo.get(bb);
                tableName = new String(bb, Charset.defaultCharset());
                int rowCoutValue = tableInfo.getInt();
                rowCountMap.put(tableName, rowCoutValue);
            }
        } catch (IOException ioException) {
            throw new KettleException("Not able to read file ", ioException);
        } finally {
            MolapUtil.closeStreams(fileInputStream, fileChannel);
        }

    }

    private void getMetaHierarichies(String molapMetaHier) {
        //
        if (null == molapMetaHier || null == metaHeirSQLQuery) {
            return;
        }
        String[] metaHier = molapMetaHier.split("&");
        metahierVoList = new ArrayList<HierarchiesInfo>(metaHier.length);
        Map<String, String[]> columnPropsMap =
                new HashMap<String, String[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int i = 0; i < metaHier.length; i++) {
            //
            HierarchiesInfo hierarichiesVo = new HierarchiesInfo();
            String[] split = metaHier[i].split(":");
            String[] columnNames = new String[split.length - 1];
            int[] columnIndex = new int[split.length - 1];
            hierarichiesVo.setHierarichieName(split[0]);
            if (null != hirches.get(split[0])) {
                hierarichiesVo.setLoadToHierarichiTable(true);
            }
            int index = 0;
            for (int j = 1; j < split.length; j++) {
                //
                String[] columnAndPropertyNames = split[j].split(",");
                columnNames[index] = columnAndPropertyNames[0];
                columnIndex[index] = getColumnIndex(columnNames[index]);
                String[] properties = new String[columnAndPropertyNames.length - 1];
                System.arraycopy(columnAndPropertyNames, 1, properties, 0,
                        columnAndPropertyNames.length - 1);
                columnPropsMap.put(columnNames[index], properties);
                index++;
            }
            hierarichiesVo.setColumnIndex(columnIndex);
            hierarichiesVo.setColumnNames(columnNames);
            hierarichiesVo.setColumnPropMap(columnPropsMap);
            metahierVoList.add(hierarichiesVo);
        }
    }

    private void updateMetaHierarichiesWithQueries(String molapLocation) {
        if (null == molapLocation) {
            return;
        }
        String[] hierWithQueries = molapLocation.split("#");
        //
        for (String hierarchyWithQuery : hierWithQueries) {
            String[] hierQueryStrings = hierarchyWithQuery.split(":");

            Iterator<HierarchiesInfo> iterator = metahierVoList.iterator();
            while (iterator.hasNext()) {
                //
                HierarchiesInfo next = iterator.next();
                if (hierQueryStrings[0].equalsIgnoreCase(next.getHierarichieName())) {
                    next.setQuery(hierQueryStrings[1]);
                    break;
                }

            }
        }

    }

    private int getColumnIndex(String columnNames) {
        for (int j = 0; j < dimColNames.length; j++) {
            if (dimColNames[j].equalsIgnoreCase(columnNames)) {
                return j;
            }
        }
        return -1;
    }

    /**
     * For now only ordinal columns are considered for time levels.
     */
    private void updateTimeDimProps() {
        timeOrdinalIndices = new int[timeOrdinalCols.length];
        int counter = 0;
        for (int i = 0; i < timeOrdinalCols.length; i++) {
            if (timeOrdinalCols[i] == null) {
                timeOrdinalIndices[i] = -1;
            } else {
                timeOrdinalIndices[i] = counter;
                counter++;
            }
        }
    }

    /**
     * Parse the properties string.
     * Level Entries separated by '&'
     * Level and prop details separated by ':'
     * Property column name and index separated by ','
     * Level:p1,index1:p2,index2&Level2....
     */
    private void updateDimProperties() {
        int incment = 0;
        if (timeIndex >= 0 && null != timeLevels) {
            incment = timeLevels.length - 1;
        }

        Map<String, int[]> indices =
                new HashMap<String, int[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        Map<String, String[]> columns =
                new HashMap<String, String[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        Map<String, String[]> dbTypes =
                new HashMap<String, String[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        if (molapProps != null && !"".equals(molapProps)) {
            String[] entries = molapProps.split("&");
            for (int i = 0; i < entries.length; i++) {
                String[] levelEntry = entries[i].split(":");
                String dimColName = levelEntry[0];
                int[] pIndices = new int[levelEntry.length - 1];
                String[] cols = new String[levelEntry.length - 1];
                String[] dbType = new String[levelEntry.length - 1];
                for (int j = 1; j < levelEntry.length; j++) {
                    String[] propEntry = levelEntry[j].split(",");
                    pIndices[j - 1] = Integer.parseInt(propEntry[1]);

                    //Shift the property index as per time levels inserted
                    if (timeIndex != -1 && pIndices[j - 1] > timeIndex) {
                        pIndices[j - 1] += incment;
                    }
                    cols[j - 1] = propEntry[0];
                    dbType[j - 1] = propEntry[2];
                }

                indices.put(dimColName, pIndices);
                columns.put(dimColName, cols);
                dbTypes.put(dimColName, dbType);
            }
        }

        if (indices.isEmpty()) {
            return;
        }

        propColumns = new List[dimColNames.length];
        propTypes = new List[dimColNames.length];
        propIndxs = new int[dimColNames.length][];

        //Fill the property details based on the map created
        for (int k = 0; k < dimColNames.length; k++) {
            //Properties present or not
            if (indices.containsKey(dimColNames[k])) {
                propColumns[k] = Arrays.asList(columns.get(dimColNames[k]));
                propTypes[k] = Arrays.asList(dbTypes.get(dimColNames[k]));
                propIndxs[k] = indices.get(dimColNames[k]);
            } else {
                propColumns[k] = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                propTypes[k] = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                propIndxs[k] = new int[0];
            }
        }
    }

    private void getTimeHierarichies(String ds) {
        if (ds == null || "".equals(ds)) {
            return;
        }

        String[] hies = ds.split(":");
        timeIndex = Integer.parseInt(hies[0]);
        timehierName = hies[1];

        //Find out the position of time hierarchy based on index matching
        for (int i = 0; i < dims.length; i++) {
            if (timeIndex == dims[i]) {
                timeDimeIndex = i;
                break;
            }
        }

        String[] names = hies[2].split(",");
        timeLevels = new String[names.length];
        int[] timeCardinalities = new int[names.length];
        timeOrdinalCols = new String[names.length];
        timeFormat = new SimpleDateFormat(hies[3]);
        String[] colNames = new String[names.length];

        for (int i = 0; i < names.length; i++) {
            String[] name = names[i].split("&");
            timeLevels[i] = name[0];
            colNames[i] = name[1];
            timeCardinalities[i] = Integer.parseInt(name[2]);

            //next is Ordinal column
            if (name.length > 3) {
                timeOrdinalCols[i] = name[3];
            }
        }

        int[] uDims = new int[dims.length + timeLevels.length - 1];
        int[] uDimLens = new int[dims.length + timeLevels.length - 1];
        String[] cols = new String[dims.length + timeLevels.length - 1];

        System.arraycopy(dims, 0, uDims, 0, timeDimeIndex);
        System.arraycopy(dimColNames, 0, cols, 0, timeDimeIndex);
        System.arraycopy(dimLens, 0, uDimLens, 0, timeDimeIndex);

        int j = 0;
        int[] tim = new int[colNames.length];
        for (int i = timeDimeIndex; i < timeLevels.length + timeDimeIndex; i++) {
            uDims[i] = timeIndex + j;
            tim[j] = i;
            j++;
        }

        System.arraycopy(colNames, 0, cols, timeDimeIndex, timeLevels.length);
        System.arraycopy(timeCardinalities, 0, uDimLens, timeDimeIndex, timeLevels.length);

        for (int i = timeDimeIndex + 1; i < dims.length; i++) {
            uDims[i + timeLevels.length - 1] = dims[i] + timeLevels.length - 1;
        }

        System.arraycopy(dimColNames, timeDimeIndex + 1, cols, timeDimeIndex + timeLevels.length,
                dims.length - (timeDimeIndex + 1));
        System.arraycopy(dimLens, timeDimeIndex + 1, uDimLens, timeDimeIndex + timeLevels.length,
                dims.length - (timeDimeIndex + 1));

        hirches.put(timehierName, tim);
        dims = uDims;
        dimColNames = cols;
        dimLens = uDimLens;
    }

    private Map<String, int[]> getHierarichies(String ds) {
        if (ds == null || "".equals(ds)) {
            return new HashMap<String, int[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        }
        Map<String, int[]> map =
                new HashMap<String, int[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        String[] hies = ds.split("&");

        for (int i = 0; i < hies.length; i++) {
            String hie = hies[i];

            String name = hie.substring(0, hie.indexOf(":"));

            int[] a = getIntArray(hie.substring(hie.indexOf(":") + 1, hie.length()));
            map.put(name, a);
        }
        return map;
    }

    private int[] getIntArray(String ds) {

        String[] sp = ds.split(",");
        int[] a = new int[sp.length];

        for (int i = 0; i < a.length; i++) {
            a[i] = Integer.parseInt(sp[i]);
        }
        return a;

    }

    private void updateDimensions(String ds, String msr) {
        String[] sp = ds.split(",");
        int[] dimsLocal = new int[sp.length];
        int[] lens = new int[sp.length];
        List<String> list = new ArrayList<String>();
        dimPresent = new boolean[sp.length];

        for (int i = 0; i < dimsLocal.length; i++) {
            String[] dim = sp[i].split(":");
            list.add(dim[0]);
            dimsLocal[i] = Integer.parseInt(dim[1]);
            lens[i] = Integer.parseInt(dim[2]);

            if ("Y".equals(dim[3])) {
                dimPresent[i] = true;
                normLength++;
            }
        }
        dims = dimsLocal;
        dimLens = lens;
        //		columns.put(DIMENSIONS, list);
        dimColNames = list.toArray(new String[list.size()]);

        String[] sm = msr.split(",");
        int[] m = new int[sm.length];
        List<String> mlist = new ArrayList<String>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for (int i = 0; i < m.length; i++) {
            String[] ms = sm[i].split(":");
            mlist.add(ms[0]);
            m[i] = Integer.parseInt(ms[1]);
        }
        msrs = m;
        measureColumn = mlist.toArray(new String[mlist.size()]);
    }

    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
            Map<String, Counter> counters) throws KettleException {
        try {
            molapProps = rep.getStepAttributeString(idStep, "molapProps");
            molapdim = rep.getStepAttributeString(idStep, "dim");
            molapmsr = rep.getStepAttributeString(idStep, "msr");
            molaphier = rep.getStepAttributeString(idStep, "hier");
            molapTime = rep.getStepAttributeString(idStep, "time");
            storeLocation = rep.getStepAttributeString(idStep, "loc");
            //
            molapJNDI = rep.getStepAttributeString(idStep, "con");
            isAggregate = rep.getStepAttributeBoolean(idStep, "isAggregate");
            metaHeirSQLQuery = rep.getStepAttributeString(idStep, "metadataFilePath");
            molapMetaHier = rep.getStepAttributeString(idStep, "molapMetaHier");
            molapMeasureNames = rep.getStepAttributeString(idStep, "molapMeasureNames");
            dimesionTableNames = rep.getStepAttributeString(idStep, "dimHierReleation");
            tableName = rep.getStepAttributeString(idStep, "factOrAggTable");
            msrAggregatorString = rep.getStepAttributeString(idStep, "msrAggregatorString");
            heirKeySize = rep.getStepAttributeString(idStep, "heirKeySize");
            primaryKeyColumnNamesString =
                    rep.getStepAttributeString(idStep, "primaryKeyColumnNamesString");
            foreignKeyHierarchyString =
                    rep.getStepAttributeString(idStep, "foreignKeyHierarchyString");
            batchSize = Integer.parseInt(rep.getStepAttributeString(idStep, "batchSize"));
            currentRestructNumber =
                    (int) rep.getStepAttributeInteger(idStep, "currentRestructNumber");
            //
            int nrKeys = rep.countNrStepAttributes(idStep, "lookup_keyfield");
            allocate(nrKeys);
            //
        } catch (Exception e) {
            throw new KettleException(BaseMessages
                    .getString(PKG, "MolapStep.Exception.UnexpectedErrorInReadingStepInfo"), e);
        }
    }

    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
            throws KettleException {
        try {
            rep.saveStepAttribute(idTransformation, idStep, "molapProps", molapProps);
            rep.saveStepAttribute(idTransformation, idStep, "dim", molapdim);
            rep.saveStepAttribute(idTransformation, idStep, "msr", molapmsr);
            rep.saveStepAttribute(idTransformation, idStep, "hier", molaphier);
            rep.saveStepAttribute(idTransformation, idStep, "time", molapTime);
            //
            rep.saveStepAttribute(idTransformation, idStep, "loc", storeLocation);
            rep.saveStepAttribute(idTransformation, idStep, "con", molapJNDI);
            rep.saveStepAttribute(idTransformation, idStep, "isInitialLoad", isAggregate);
            rep.saveStepAttribute(idTransformation, idStep, "metadataFilePath", metaHeirSQLQuery);
            rep.saveStepAttribute(idTransformation, idStep, "molapMetaHier", molapMetaHier);
            rep.saveStepAttribute(idTransformation, idStep, "batchSize", batchSize);
            rep.saveStepAttribute(idTransformation, idStep, "dimHierReleation", dimesionTableNames);
            rep.saveStepAttribute(idTransformation, idStep, "molapMeasureNames", molapMeasureNames);
            rep.saveStepAttribute(idTransformation, idStep, "msrAggregatorString",
                    msrAggregatorString);
            rep.saveStepAttribute(idTransformation, idStep, "heirKeySize", heirKeySize);
            rep.saveStepAttribute(idTransformation, idStep, "primaryKeyColumnNamesString",
                    primaryKeyColumnNamesString);
            rep.saveStepAttribute(idTransformation, idStep, "foreignKeyHierarchyString",
                    foreignKeyHierarchyString);
            //
            rep.saveStepAttribute(idTransformation, idStep, "factOrAggTable", tableName);
            rep.saveStepAttribute(idTransformation, idStep, "currentRestructNumber",
                    currentRestructNumber);

        } catch (Exception ex) {
            throw new KettleException(BaseMessages
                    .getString(PKG, "MolapStep.Exception.UnableToSaveStepInfoToRepository")
                    + idStep, ex);
        }
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
            TransMeta transMeta, Trans disp) {
        return new MolapSeqGenStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
    }

    public StepDataInterface getStepData() {
        return new MolapSeqGenData();
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta,
            RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
        MolapDataProcessorUtil.check(PKG, remarks, stepMeta, prev, input);

    }

    public List<String>[] getPropertiesColumns() {
        return propColumns;
    }

    public int[][] getPropertiesIndices() {
        return propIndxs;
    }

    /**
     * @return Returns the propTypes.
     */
    public List<String>[] getPropTypes() {
        return propTypes;
    }

    /**
     * @return Returns the rowCountMap.
     */
    public Map<String, Integer> getRowCountMap() {
        return rowCountMap;
    }

    /**
     * @param rowCountMap The rowCountMap to set.
     */
    public void setRowCountMap(Map<String, Integer> rowCountMap) {
        this.rowCountMap = rowCountMap;
    }

    /**
     * @return Returns the dimHierReleation.
     */
    public String getTableNames() {
        return dimesionTableNames;
    }

    /**
     * @param dimHierReleation The dimHierReleation to set.
     */
    public void setTableNames(String dimHierReleation) {
        this.dimesionTableNames = dimHierReleation;
    }

    /**
     * @return Returns the modifiedDimension.
     */
    public String[] getModifiedDimension() {
        return modifiedDimension;
    }

    /**
     * @param modifiedDimension The modifiedDimension to set.
     */
    public void setModifiedDimension(String[] modifiedDimension) {
        this.modifiedDimension = modifiedDimension;
    }

    /**
     * @return the molapMeasureNames
     */
    public String getMolapMeasureNames() {
        return molapMeasureNames;
    }

    /**
     * @param molapMeasureNames the molapMeasureNames to set
     */
    public void setMolapMeasureNames(String molapMeasureNames) {
        this.molapMeasureNames = molapMeasureNames;
    }

    /**
     * @return Returns the msrAggregatorString.
     */
    public String getMsrAggregatorString() {
        return msrAggregatorString;
    }

    /**
     * @param msrAggregatorString The msrAggregatorString to set.
     */
    public void setMsrAggregatorString(String msrAggregatorString) {
        this.msrAggregatorString = msrAggregatorString;
    }

    /**
     * @return Returns the tableName.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName The tableName to set.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getHeirKeySize() {
        return heirKeySize;
    }

    public void setHeirKeySize(String heirKeySize) {
        this.heirKeySize = heirKeySize;
    }

    /**
     * @return Returns the primaryKeyColumnNamesString.
     */
    public String getPrimaryKeyColumnNamesString() {
        return primaryKeyColumnNamesString;
    }

    /**
     * @param primaryKeyColumnNamesString The primaryKeyColumnNamesString to set.
     */
    public void setPrimaryKeyColumnNamesString(String primaryKeyColumnNamesString) {
        this.primaryKeyColumnNamesString = primaryKeyColumnNamesString;
    }

    /**
     * @return Returns the foreignKeyHierarchyString.
     */
    public String getForeignKeyHierarchyString() {
        return foreignKeyHierarchyString;
    }

    /**
     * @param foreignKeyHierarchyString The foreignKeyHierarchyString to set.
     */
    public void setForeignKeyHierarchyString(String foreignKeyHierarchyString) {
        this.foreignKeyHierarchyString = foreignKeyHierarchyString;
    }

    /**
     * @return the currentRestructNumber
     */
    public int getCurrentRestructNumber() {
        return currentRestructNumber;
    }

    /**
     * @param currentRestructNum the currentRestructNumber to set
     */
    public void setCurrentRestructNumber(int currentRestructNum) {
        this.currentRestructNumber = currentRestructNum;
    }
}
