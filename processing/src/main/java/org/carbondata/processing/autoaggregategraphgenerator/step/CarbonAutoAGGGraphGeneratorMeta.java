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

package org.carbondata.processing.autoaggregategraphgenerator.step;

import java.util.List;
import java.util.Map;

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

public class CarbonAutoAGGGraphGeneratorMeta extends BaseStepMeta implements StepMetaInterface {
    /**
     * for i18n purposes
     */
    private static Class<?> pkg = CarbonAutoAGGGraphGeneratorMeta.class; // for i18n

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * aggTables
     */
    private String aggTables;

    /**
     * schemaPath
     */
    private String schema;

    /**
     * factTableName
     */
    private String factTableName;

    /**
     * isHDFSMode
     */
    private String hdfMode;

    /**
     * partitionId
     */
    private String partitionId;

    private String autoMode;

    /**
     * factStoreLocation
     */
    private String factStoreLocation;

    private int currentRestructNumber;
    /**
     * load names separated by HASH_SPC_CHARACTER
     */
    private String loadNames;
    /**
     * Segment modification Or DeletionTime separated by HASH_SPC_CHARACTER
     */
    private String modificationOrDeletionTime;

    /**
     * set the default value for all the properties
     */
    @Override
    public void setDefault() {
        aggTables = "";
        schemaName = "";
        cubeName = "";
        schema = "";
        factTableName = "";
        hdfMode = "";
        partitionId = "";
        autoMode = "";
        factStoreLocation = "";
        currentRestructNumber = -1;
        loadNames = "";
        modificationOrDeletionTime = "";
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
        CheckResult chkRes = null;
        if (input.length > 0) {
            chkRes = new CheckResult(CheckResult.TYPE_RESULT_OK,
                    "Step is receiving info from other steps.", stepMeta);
            remarks.add(chkRes);
        } else {
            chkRes = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
                    "No input received from other steps!", stepMeta);
            remarks.add(chkRes);
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
        return new CarbonAutoAGGGraphGeneratorStep(stepMeta, stepDataInterface, copyNr, transMeta,
                trans);
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
        return new CarbonAutoAGGGraphGeneratorData();
    }

    /**
     * Get the XML that represents the values in this step
     *
     * @return the XML that represents the metadata in this step
     * @throws KettleException in case there is a conversion or XML encoding error
     */
    public String getXML() {
        StringBuffer retval = new StringBuffer(150);
        retval.append("    ").append(XMLHandler.addTagValue("aggTables", aggTables));
        retval.append("    ").append(XMLHandler.addTagValue("cubeName", cubeName));
        retval.append("    ").append(XMLHandler.addTagValue("schemaName", schemaName));
        retval.append("    ").append(XMLHandler.addTagValue("schema", schema));
        retval.append("    ").append(XMLHandler.addTagValue("factTableName", factTableName));
        retval.append("    ").append(XMLHandler.addTagValue("isHDFSMode", hdfMode));
        retval.append("    ").append(XMLHandler.addTagValue("partitionId", partitionId));
        retval.append("    ").append(XMLHandler.addTagValue("isAutoMode", autoMode));
        retval.append("    ")
                .append(XMLHandler.addTagValue("factStoreLocation", factStoreLocation));
        retval.append("    ")
                .append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
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
            aggTables = XMLHandler.getTagValue(stepnode, "aggTables");
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            schema = XMLHandler.getTagValue(stepnode, "schema");
            factTableName = XMLHandler.getTagValue(stepnode, "factTableName");
            hdfMode = XMLHandler.getTagValue(stepnode, "isHDFSMode");
            partitionId = XMLHandler.getTagValue(stepnode, "partitionId");
            autoMode = XMLHandler.getTagValue(stepnode, "isAutoMode");
            factStoreLocation = XMLHandler.getTagValue(stepnode, "factStoreLocation");
            currentRestructNumber =
                    Integer.parseInt(XMLHandler.getTagValue(stepnode, "currentRestructNumber"));
            loadNames = XMLHandler.getTagValue(stepnode, "loadNames");
            modificationOrDeletionTime =
                    XMLHandler.getTagValue(stepnode, "modificationOrDeletionTime");
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
            aggTables = rep.getStepAttributeString(idStep, "aggTables");
            schemaName = rep.getStepAttributeString(idStep, "schemaName");
            cubeName = rep.getStepAttributeString(idStep, "cubeName");
            schema = rep.getStepAttributeString(idStep, "schema");
            factTableName = rep.getStepAttributeString(idStep, "factTableName");
            hdfMode = rep.getStepAttributeString(idStep, "isHDFSMode");
            partitionId = rep.getStepAttributeString(idStep, "partitionId");
            autoMode = rep.getStepAttributeString(idStep, "isAutoMode");
            factStoreLocation = rep.getStepAttributeString(idStep, "factStoreLocation");
            currentRestructNumber =
                    (int) rep.getStepAttributeInteger(idStep, "currentRestructNumber");
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
     * @param rep               The Kettle repository to save to
     * @param idtTransformation The transformation ID
     * @param idsStep           The step ID
     * @throws KettleException When an unexpected error occurred (database, network, etc)
     */
    @Override
    public void saveRep(Repository rep, ObjectId idtTransformation, ObjectId idsStep)
            throws KettleException {
        try {
            rep.saveStepAttribute(idtTransformation, idsStep, "aggTables", aggTables); //$NON-NLS-1$
            rep.saveStepAttribute(idtTransformation, idsStep, "schemaName", schemaName);
            rep.saveStepAttribute(idtTransformation, idsStep, "cubeName", cubeName);
            rep.saveStepAttribute(idtTransformation, idsStep, "schema", schema);
            rep.saveStepAttribute(idtTransformation, idsStep, "factTableName", factTableName);
            rep.saveStepAttribute(idtTransformation, idsStep, "isHDFSMode", hdfMode);
            rep.saveStepAttribute(idtTransformation, idsStep, "partitionId", partitionId);
            rep.saveStepAttribute(idtTransformation, idsStep, "isAutoMode", autoMode);
            rep.saveStepAttribute(idtTransformation, idsStep, "factStoreLocation",
                    factStoreLocation);
            rep.saveStepAttribute(idtTransformation, idsStep, "currentRestructNumber",
                    currentRestructNumber);
            rep.saveStepAttribute(idtTransformation, idsStep, "loadNames", loadNames);
            rep.saveStepAttribute(idtTransformation, idsStep, "modificationOrDeletionTime",
                    modificationOrDeletionTime);
        } catch (Exception e) {
            throw new KettleException(BaseMessages
                    .getString(pkg, "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
                    + idsStep, e);
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
     * @return the aggTables
     */
    public String getAggTables() {
        return aggTables;
    }

    /**
     * @param aggTables the aggTables to set
     */
    public void setAggTables(String aggTables) {
        this.aggTables = aggTables;
    }

    /**
     * @return the factTableName
     */
    public String getFactTableName() {
        return factTableName;
    }

    /**
     * @param factTableName the factTableName to set
     */
    public void setFactTableName(String factTableName) {
        this.factTableName = factTableName;
    }

    /**
     * @return the schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * @param schema the schema to set
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * @return the isHDFSMode
     */
    public boolean isHDFSMode() {
        return Boolean.parseBoolean(hdfMode);
    }

    /**
     * @return the partitionId
     */
    public String getPartitionId() {
        return partitionId;
    }

    /**
     * @param partitionId the partitionId to set
     */
    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    /**
     * @param isHDFSMode the isHDFSMode to set
     */
    public void setIsHDFSMode(String isHDFSMode) {
        this.hdfMode = isHDFSMode;
    }

    public String isAutoMode() {
        return autoMode;
    }

    public void setAutoMode(String isAutoMode) {
        this.autoMode = isAutoMode;
    }

    /**
     * @return Returns the factStoreLocation.
     */
    public String getFactStoreLocation() {
        return factStoreLocation;
    }

    /**
     * @param factStoreLocation The factStoreLocation to set.
     */
    public void setFactStoreLocation(String factStoreLocation) {
        this.factStoreLocation = factStoreLocation;
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

    /**
     * return's loadNames separated by HASH_SPC_CHARACTER
     */
    public String getLoadNames() {
        return loadNames;
    }

    /**
     * set loadNames separated by HASH_SPC_CHARACTER
     */
    public void setLoadNames(String loadNames) {
        this.loadNames = loadNames;
    }

    /**
     * return modificationOrDeletionTime separated by HASH_SPC_CHARACTER
     */
    public String getModificationOrDeletionTime() {
        return modificationOrDeletionTime;
    }

    /**
     * set modificationOrDeletionTime separated by HASH_SPC_CHARACTER
     */
    public void setModificationOrDeletionTime(String modificationOrDeletionTime) {
        this.modificationOrDeletionTime = modificationOrDeletionTime;
    }
}
