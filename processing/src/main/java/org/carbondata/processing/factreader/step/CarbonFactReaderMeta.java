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

package org.carbondata.processing.factreader.step;

import java.util.List;
import java.util.Map;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
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

public class CarbonFactReaderMeta extends BaseStepMeta implements StepMetaInterface {
    /**
     * for i18n purposes
     */
    private static Class<?> pkg = CarbonFactReaderMeta.class; // for i18n

    /**
     * measureCount
     */
    private int measureCount;

    /**
     * measureCountString
     */
    private String measureCountString;

    /**
     * dimLensString
     */
    private String dimLensString;

    /**
     * dimLens
     */
    private long[] dimLens;

    /**
     * global cardinality
     */
    private String globalDimLensString;

    /**
     * global cardinality
     */
    private int[] globalDimLens;

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * tableName
     */
    private String tableName;

    /**
     * readOnlyInProgress
     */
    private String readOnlyInProgress;

    /**
     * aggTypeString
     */
    private String aggTypeString;

    /**
     * aggType
     */
    private String[] aggType;

    /**
     * blockIndexString
     */
    private String blockIndexString;

    /**
     * blockIndex
     */
    private int[] blockIndex;

    /**
     * Fact store location
     */
    private String factStoreLocation;

    /**
     * schema
     */
    private String schema;

    /**
     * partitionID
     */
    private String partitionID;

    /**
     * partitionID
     */
    private String aggregateTableName;
    /**
     * load names
     */
    private String loadNames;

    /**
     * Cube modification or Deletion time stamp
     */
    private String modificationOrDeletionTime;

    /**
     *  map having segment name  as key and segment Modification time stamp as value
     */
    private Map<String, Long> loadNameAndModificationTimeMap;

    /**
     * set the default value for all the properties
     */
    @Override
    public void setDefault() {
        tableName = "";
        schemaName = "";
        cubeName = "";
        measureCountString = "";
        dimLensString = "";
        readOnlyInProgress = "";
        aggTypeString = "";
        blockIndexString = "";
        factStoreLocation = "";
        schema = "";
        partitionID = "";
        aggregateTableName = "";
        globalDimLensString = "";
        loadNames = "";
        modificationOrDeletionTime = "";
    }

    /**
     * below method will be used initialise the meta
     */
    public void initialize() {
        String[] dimLensStringArray = dimLensString.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
        dimLens = new long[dimLensStringArray.length];
        for (int i = 0; i < dimLensStringArray.length; i++) {
            dimLens[i] = Integer.parseInt(dimLensStringArray[i]);
        }
        String[] globalDimLensStringArray =
                globalDimLensString.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
        globalDimLens = new int[globalDimLensStringArray.length];
        for (int i = 0; i < dimLensStringArray.length; i++) {
            globalDimLens[i] = Integer.parseInt(globalDimLensStringArray[i]);
        }
        measureCount = Integer.parseInt(measureCountString);
        this.aggType = aggTypeString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
        String[] split = blockIndexString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
        this.blockIndex = new int[split.length];
        for (int i = 0; i < split.length; i++) {
            this.blockIndex[i] = Integer.parseInt(split[i]);
        }
        loadNameAndModificationTimeMap = CarbonDataProcessorUtil.getLoadNameAndModificationTimeMap(
                loadNames.split(CarbonCommonConstants.HASH_SPC_CHARACTER),
                modificationOrDeletionTime.split(CarbonCommonConstants.HASH_SPC_CHARACTER));
    }

    /**
     * Checks the settings of this step and puts the findings in a remarks List.
     *
     * @param remarks  The list to put the remarks in @see
     *                 org.pentaho.di.core.CheckResult
     * @param stepMeta The stepMeta to help checking
     * @param prev     The fields coming from the previous step
     * @param input    The input step names
     * @param output   The output step names
     * @param info     The fields that are used as information by the step
     */
    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
            RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
        CheckResult cr = null;
        if (input.length > 0) {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
                    "Step is receiving info from other steps.", stepMeta);
            remarks.add(cr);
        } else {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
                    "No input received from other steps!", stepMeta);
            remarks.add(cr);
        }
    }

    /**
     * Get the executing step, needed by Trans to launch a step.
     *
     * @param stepMeta          The step info
     * @param stepDataInterface the step data interface linked to this step. Here the step can
     *                          store temporary data, database connections, etc.
     * @param copyNr            The copy nr to get
     * @param transMeta         The transformation info
     * @param trans             The launching transformation
     */

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans) {
        return new CarbonFactReaderStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    /**
     * Get a new instance of the appropriate data class. This data class
     * implements the StepDataInterface. It basically contains the persisting
     * data that needs to live on, even if a worker thread is terminated.
     *
     * @return The appropriate StepDataInterface class.
     */

    @Override
    public StepDataInterface getStepData() {
        return new CarbonFactReaderData();
    }

    /**
     * Get the XML that represents the values in this step
     *
     * @return the XML that represents the metadata in this step
     * @throws KettleException in case there is a conversion or XML encoding error
     */
    public String getXML() {
        StringBuffer retval = new StringBuffer(150);
        retval.append("    ").append(XMLHandler.addTagValue("TableName", tableName));
        retval.append("    ").append(XMLHandler.addTagValue("cubeName", cubeName));
        retval.append("    ").append(XMLHandler.addTagValue("schemaName", schemaName));
        retval.append("    ")
                .append(XMLHandler.addTagValue("measureCountString", measureCountString));
        retval.append("    ").append(XMLHandler.addTagValue("dimLensString", dimLensString));
        retval.append("    ")
                .append(XMLHandler.addTagValue("readOnlyInProgress", readOnlyInProgress));
        retval.append("    ").append(XMLHandler.addTagValue("aggTypeString", aggTypeString));
        retval.append("    ").append(XMLHandler.addTagValue("blockIndexString", blockIndexString));
        retval.append("    ")
                .append(XMLHandler.addTagValue("factStoreLocation", factStoreLocation));
        retval.append("    ").append(XMLHandler.addTagValue("schema", schema));
        retval.append("    ").append(XMLHandler.addTagValue("partitionID", partitionID));
        retval.append("    ")
                .append(XMLHandler.addTagValue("aggregateTableName", aggregateTableName));
        retval.append("    ")
                .append(XMLHandler.addTagValue("globalDimLensString", globalDimLensString));
        retval.append("    ").append(XMLHandler.addTagValue("loadNames", loadNames));
        retval.append("    ").append(XMLHandler
                .addTagValue("modificationOrDeletionTime", modificationOrDeletionTime));
        return retval.toString();
    }

    /**
     * Load the values for this step from an XML Node
     *
     * @param stepnode  the Node to get the info from
     * @param databases The available list of databases to reference to
     * @param counters  Counters to reference.
     * @throws KettleXMLException When an unexpected XML error occurred. (malformed etc.)
     */
    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleXMLException {
        try {
            tableName = XMLHandler.getTagValue(stepnode, "TableName");
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            measureCountString = XMLHandler.getTagValue(stepnode, "measureCountString");
            dimLensString = XMLHandler.getTagValue(stepnode, "dimLensString");
            readOnlyInProgress = XMLHandler.getTagValue(stepnode, "readOnlyInProgress");
            aggTypeString = XMLHandler.getTagValue(stepnode, "aggTypeString");
            blockIndexString = XMLHandler.getTagValue(stepnode, "blockIndexString");
            factStoreLocation = XMLHandler.getTagValue(stepnode, "factStoreLocation");
            schema = XMLHandler.getTagValue(stepnode, "schema");
            partitionID = XMLHandler.getTagValue(stepnode, "partitionID");
            aggregateTableName = XMLHandler.getTagValue(stepnode, "aggregateTableName");
            globalDimLensString = XMLHandler.getTagValue(stepnode, "globalDimLensString");
            loadNames = XMLHandler.getTagValue(stepnode, "loadNames");
            modificationOrDeletionTime = XMLHandler.getTagValue(stepnode, "loadNames");


        } catch (Exception e) {
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

    /**
     * Read the steps information from a Kettle repository
     *
     * @param rep       The repository to read from
     * @param idStep    The step ID
     * @param databases The databases to reference
     * @param counters  The counters to reference
     * @throws KettleException When an unexpected error occurred (database, network, etc)
     */
    @Override
    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
            Map<String, Counter> counters) throws KettleException {
        try {
            tableName = rep.getStepAttributeString(idStep, "TableName");
            schemaName = rep.getStepAttributeString(idStep, "schemaName");
            cubeName = rep.getStepAttributeString(idStep, "cubeName");
            measureCountString = rep.getStepAttributeString(idStep, "measureCountString");
            dimLensString = rep.getStepAttributeString(idStep, "dimLensString");
            readOnlyInProgress = rep.getStepAttributeString(idStep, "readOnlyInProgress");
            aggTypeString = rep.getStepAttributeString(idStep, "aggTypeString");
            blockIndexString = rep.getStepAttributeString(idStep, "blockIndexString");
            factStoreLocation = rep.getStepAttributeString(idStep, "factStoreLocation");
            schema = rep.getStepAttributeString(idStep, "schema");
            partitionID = rep.getStepAttributeString(idStep, "partitionID");
            aggregateTableName = rep.getStepAttributeString(idStep, "aggregateTableName");
            globalDimLensString = rep.getStepAttributeString(idStep, "globalDimLensString");
            loadNames = rep.getStepAttributeString(idStep, "loadNames");
            modificationOrDeletionTime =
                    rep.getStepAttributeString(idStep, "modificationOrDeletionTime");
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(pkg,
                    "CarbonMDKeyStepMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
        }
    }

    /**
     * Save the steps data into a Kettle repository
     *
     * @param rep              The Kettle repository to save to
     * @param idTransformation The transformation ID
     * @param idStep           The step ID
     * @throws KettleException When an unexpected error occurred (database, network, etc)
     */
    @Override
    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
            throws KettleException {

        try {
            rep.saveStepAttribute(idTransformation, idStep, "TableName", tableName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "schemaName", schemaName);
            rep.saveStepAttribute(idTransformation, idStep, "cubeName", cubeName);
            rep.saveStepAttribute(idTransformation, idStep, "measureCountString",
                    measureCountString);
            rep.saveStepAttribute(idTransformation, idStep, "dimLensString", dimLensString);
            rep.saveStepAttribute(idTransformation, idStep, "readOnlyInProgress",
                    readOnlyInProgress);
            rep.saveStepAttribute(idTransformation, idStep, "aggTypeString", aggTypeString);
            rep.saveStepAttribute(idTransformation, idStep, "blockIndexString", blockIndexString);
            rep.saveStepAttribute(idTransformation, idStep, "factStoreLocation", factStoreLocation);
            rep.saveStepAttribute(idTransformation, idStep, "schema", schema);
            rep.saveStepAttribute(idTransformation, idStep, "partitionID", partitionID);
            rep.saveStepAttribute(idTransformation, idStep, "aggregateTableName",
                    aggregateTableName);
            rep.saveStepAttribute(idTransformation, idStep, "globalDimLensString",
                    globalDimLensString);
            rep.saveStepAttribute(idTransformation, idStep, "loadNames",
                    loadNames);
            rep.saveStepAttribute(idTransformation, idStep, "modificationOrDeletionTime",
                    modificationOrDeletionTime);
        } catch (Exception e) {
            throw new KettleException(BaseMessages
                    .getString(pkg, "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
                    + idStep, e);
        }
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName() {
        return cubeName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    /**
     * @return the measureCountString
     */
    public String getMeasureCountString() {
        return measureCountString;
    }

    /**
     * @param measureCountString the measureCountString to set
     */
    public void setMeasureCountString(String measureCountString) {
        this.measureCountString = measureCountString;
    }

    /**
     * @return the dimLens
     */
    public long[] getDimLens() {
        return dimLens;
    }

    /**
     * @param dimLens the dimLens to set
     */
    public void setDimLens(long[] dimLens) {
        this.dimLens = dimLens;
    }

    /**
     * @return the measureCount
     */
    public int getMeasureCount() {
        return measureCount;
    }

    /**
     * @param measureCount the measureCount to set
     */
    public void setMeasureCount(int measureCount) {
        this.measureCount = measureCount;
    }

    /**
     * @return the dimLensString
     */
    public String getDimLensString() {
        return dimLensString;
    }

    /**
     * @param dimLensString the dimLensString to set
     */
    public void setDimLensString(String dimLensString) {
        this.dimLensString = dimLensString;
    }

    /**
     * @return the readOnlyInProgress
     */
    public boolean isReadOnlyInProgress() {
        return Boolean.parseBoolean(readOnlyInProgress);
    }

    /**
     * @param readOnlyInProgress the readOnlyInProgress to set
     */
    public void setReadOnlyInProgress(String readOnlyInProgress) {
        this.readOnlyInProgress = readOnlyInProgress;
    }

    /**
     * @param aggTypeString the aggTypeString to set
     */
    public void setAggTypeString(String aggTypeString) {
        this.aggTypeString = aggTypeString;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @return the aggType
     */
    public String[] getAggType() {
        return aggType;
    }

    public void setBlockIndexString(String blockIndexString) {
        this.blockIndexString = blockIndexString;
    }

    public int[] getBlockIndex() {
        return blockIndex;
    }

    public void setBlockIndex(int[] blockIndex) {
        this.blockIndex = blockIndex;
    }

    public String getFactStoreLocation() {
        return factStoreLocation;
    }

    public void setFactStoreLocation(String factStoreLocation) {
        this.factStoreLocation = factStoreLocation;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(String partitionID) {
        this.partitionID = partitionID;
    }

    /**
     * @return the aggregateTableName
     */
    public String getAggregateTableName() {
        return aggregateTableName;
    }

    /**
     * @param aggregateTableName the aggregateTableName to set
     */
    public void setAggregateTableName(String aggregateTableName) {
        this.aggregateTableName = aggregateTableName;
    }

    /**
     * @return Returns the globalDimLensString.
     */
    public String getGlobalDimLensString() {
        return globalDimLensString;
    }

    /**
     * @param globalDimLensString The globalDimLensString to set.
     */
    public void setGlobalDimLensString(String globalDimLensString) {
        this.globalDimLensString = globalDimLensString;
    }

    /**
     * @return Returns the globalDimLens.
     */
    public int[] getGlobalDimLens() {
        return globalDimLens;
    }

    /**
     * @param globalDimLens The globalDimLens to set.
     */
    public void setGlobalDimLens(int[] globalDimLens) {
        this.globalDimLens = globalDimLens;
    }

    public void setModificationOrDeletionTime(String modificationOrDeletionTime) {
        this.modificationOrDeletionTime = modificationOrDeletionTime;
    }

    /**
     * set the load name
     * @param loadNames
     */
    public void setLoadNames(String loadNames) {
        this.loadNames = loadNames;
    }

    /**
     * Returns the map having segment name as key & modification time as value.
     * @return
     */
    public Map<String, Long> getLoadNameAndModificationTimeMap() {
        return loadNameAndModificationTimeMap;
    }
}
