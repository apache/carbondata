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
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 */
package org.carbondata.processing.merger.step.autoaggregate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.constants.MolapCommonConstants;
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

public class MolapAutoAggregateSliceMergerMeta extends BaseStepMeta implements StepMetaInterface {

    /**
     * for i18n purposes
     */
    private static final Class<?> PKG = MolapAutoAggregateSliceMergerMeta.class;

    /**
     * table name
     */
    private String tabelName;

    /**
     * mdkey size
     */
    private String mdkeySize;

    /**
     * measureCount
     */
    private String measureCount;

    /**
     * heirAndKeySize
     */
    private String heirAndKeySize;

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * aggregatorString
     */
    private String aggregatorString;

    /**
     * aggregatorClassString
     */
    private String aggregatorClassString;

    /**
     * tableNames
     */
    private String[] tableNames;

    /**
     * mapOfTableAndMdkeySize
     */
    private Map<String, Integer> mapOfTableAndMdkeySize;

    /**
     * mapOfTableAndMeasureCount
     */
    private Map<String, Integer> mapOfTableAndMeasureCount;

    /**
     * mapOfAggTableAndAggClass
     */
    private Map<String, String[]> mapOfAggTableAndAggClass;

    /**
     * mapOfAggTableAndAgg
     */
    private Map<String, String[]> mapOfAggTableAndAgg;

    /**
     * factDimLensString
     */
    private String factDimLensString;

    private int currentRestructNumber;

    /**
     *
     * MolapDataWriterStepMeta constructor to initialize this class
     *
     */
    public MolapAutoAggregateSliceMergerMeta() {
        super();
    }

    /**
     * set the default value for all the properties
     *
     */
    @Override public void setDefault() {
        tabelName = "";
        mdkeySize = "";
        measureCount = "";
        heirAndKeySize = "";
        cubeName = "";
        schemaName = "";
        aggregatorClassString = "";
        aggregatorString = "";
        factDimLensString = "";
        currentRestructNumber = -1;
    }

    /**
     * Below method will be used to initialise the meta
     */
    public void initialise() {
        tableNames = tabelName.split(MolapCommonConstants.HASH_SPC_CHARACTER);
        mapOfTableAndMdkeySize =
                new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        mapOfTableAndMeasureCount =
                new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        mapOfAggTableAndAggClass =
                new HashMap<String, String[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        mapOfAggTableAndAgg =
                new HashMap<String, String[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        String[] split = mdkeySize.split(MolapCommonConstants.HASH_SPC_CHARACTER);
        String[] split2 = null;
        for (int i = 0; i < split.length; i++) {
            split2 = split[i].split(MolapCommonConstants.COMA_SPC_CHARACTER);
            mapOfTableAndMdkeySize.put(split2[0], Integer.parseInt(split2[1]));
        }

        split = measureCount.split(MolapCommonConstants.HASH_SPC_CHARACTER);
        for (int i = 0; i < split.length; i++) {
            split2 = split[i].split(MolapCommonConstants.COMA_SPC_CHARACTER);
            mapOfTableAndMeasureCount.put(split2[0], Integer.parseInt(split2[1]));
        }
        split = aggregatorString.split(MolapCommonConstants.COLON_SPC_CHARACTER);
        for (int i = 0; i < split.length; i++) {
            split2 = split[i].split(MolapCommonConstants.COMA_SPC_CHARACTER);
            mapOfAggTableAndAgg
                    .put(split2[0], split2[1].split(MolapCommonConstants.HASH_SPC_CHARACTER));
        }

        split = aggregatorClassString.split(MolapCommonConstants.COLON_SPC_CHARACTER);
        for (int i = 0; i < split.length; i++) {
            split2 = split[i].split(MolapCommonConstants.COMA_SPC_CHARACTER);
            mapOfAggTableAndAggClass
                    .put(split2[0], split2[1].split(MolapCommonConstants.HASH_SPC_CHARACTER));
        }

    }

    /**
     * Get the XML that represents the values in this step
     *
     * @return the XML that represents the metadata in this step
     * @throws KettleException
     *             in case there is a conversion or XML encoding error
     */
    public String getXML() {
        StringBuffer strBuff = new StringBuffer(150);
        strBuff.append("    ").append(XMLHandler.addTagValue("TableName", tabelName));
        strBuff.append("    ").append(XMLHandler.addTagValue("MDKeySize", mdkeySize));
        strBuff.append("    ").append(XMLHandler.addTagValue("Measurecount", measureCount));
        strBuff.append("    ").append(XMLHandler.addTagValue("HeirAndKeySize", heirAndKeySize));
        strBuff.append("    ").append(XMLHandler.addTagValue("cubeName", cubeName));
        strBuff.append("    ").append(XMLHandler.addTagValue("schemaName", schemaName));
        strBuff.append("    ")
                .append(XMLHandler.addTagValue("aggregatorClassString", aggregatorClassString));
        strBuff.append("    ").append(XMLHandler.addTagValue("aggregatorString", aggregatorString));
        strBuff.append("    ")
                .append(XMLHandler.addTagValue("factDimLensString", factDimLensString));
        strBuff.append("    ")
                .append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        return strBuff.toString();
    }

    /**
     * Load the values for this step from an XML Node
     *
     * @param stepnode
     *            the Node to get the info from
     * @param databases
     *            The available list of databases to reference to
     * @param counters
     *            Counters to reference.
     * @throws KettleXMLException
     *             When an unexpected XML error occurred. (malformed etc.)
     */
    @Override public void loadXML(Node stepnode, List<DatabaseMeta> databases,
            Map<String, Counter> counters) throws KettleXMLException {
        try {
            tabelName = XMLHandler.getTagValue(stepnode, "TableName");
            mdkeySize = XMLHandler.getTagValue(stepnode, "MDKeySize");
            measureCount = XMLHandler.getTagValue(stepnode, "Measurecount");
            heirAndKeySize = XMLHandler.getTagValue(stepnode, "HeirAndKeySize");

            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            aggregatorClassString = XMLHandler.getTagValue(stepnode, "aggregatorClassString");
            aggregatorString = XMLHandler.getTagValue(stepnode, "aggregatorString");
            factDimLensString = XMLHandler.getTagValue(stepnode, "factDimLensString");
            currentRestructNumber =
                    Integer.parseInt(XMLHandler.getTagValue(stepnode, "currentRestructNumber"));
        } catch (Exception ex) {
            throw new KettleXMLException("Unable to read step info from XML node", ex);
        }
    }

    /**
     * Save the steps data into a Kettle repository
     *
     * @param repository
     *            The Kettle repository to save to
     * @param idTransformation
     *            The transformation ID
     * @param idStep
     *            The step ID
     * @throws KettleException
     *             When an unexpected error occurred (database, network, etc)
     */
    @Override public void saveRep(Repository repository, ObjectId idTransformation, ObjectId idStep)
            throws KettleException {
        try {
            repository.saveStepAttribute(idTransformation, idStep, "TableName",
                    tabelName); //$NON-NLS-1$
            repository.saveStepAttribute(idTransformation, idStep, "MDKeySize",
                    mdkeySize); //$NON-NLS-1$
            repository.saveStepAttribute(idTransformation, idStep, "Measurecount", measureCount);
            repository.saveStepAttribute(idTransformation, idStep, "HeirAndKeySize",
                    heirAndKeySize); //$NON-NLS-1$
            repository.saveStepAttribute(idTransformation, idStep, "cubeName",
                    cubeName); //$NON-NLS-1$
            repository.saveStepAttribute(idTransformation, idStep, "schemaName",
                    schemaName); //$NON-NLS-1$
            repository.saveStepAttribute(idTransformation, idStep, "aggregatorClassString",
                    aggregatorClassString);
            repository.saveStepAttribute(idTransformation, idStep, "aggregatorString",
                    aggregatorString);
            repository.saveStepAttribute(idTransformation, idStep, "factDimLensString",
                    factDimLensString);
            repository.saveStepAttribute(idTransformation, idStep, "currentRestructNumber",
                    currentRestructNumber);

        } catch (Exception e) {
            throw new KettleException(BaseMessages
                    .getString(PKG, "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
                    + idStep, e);
        }
    }

    /**
     * Read the steps information from a Kettle repository
     *
     * @param rep
     *            The repository to read from
     * @param idStep
     *            The step ID
     * @param databases
     *            The databases to reference
     * @param counters
     *            The counters to reference
     * @throws KettleException
     *             When an unexpected error occurred (database, network, etc)
     */
    @Override public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
            Map<String, Counter> counters) throws KettleException {
        try {
            tabelName = rep.getStepAttributeString(idStep, "TableName");
            mdkeySize = rep.getStepAttributeString(idStep, "MDKeySize");
            measureCount = rep.getStepAttributeString(idStep, "Measurecount");
            heirAndKeySize = rep.getStepAttributeString(idStep, "HeirAndKeySize");
            schemaName = rep.getStepAttributeString(idStep, "schemaName");
            cubeName = rep.getStepAttributeString(idStep, "cubeName");
            aggregatorClassString = rep.getStepAttributeString(idStep, "aggregatorClassString");
            aggregatorString = rep.getStepAttributeString(idStep, "aggregatorString");
            factDimLensString = rep.getStepAttributeString(idStep, "factDimLensString");
            currentRestructNumber =
                    (int) rep.getStepAttributeInteger(idStep, "currentRestructNumber");
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG,
                    "MolapDataWriterStepMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
        }

    }

    /**
     * Checks the settings of this step and puts the findings in a remarks List.
     *
     * @param remarks
     *            The list to put the remarks in @see
     *            org.pentaho.di.core.CheckResult
     * @param stepMeta
     *            The stepMeta to help checking
     * @param prev
     *            The fields coming from the previous step
     * @param input
     *            The input step names
     * @param output
     *            The output step names
     * @param info
     *            The fields that are used as information by the step
     */
    @Override public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
            StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
            RowMetaInterface info) {

        CheckResult chkRes = null;

        // See if we have input streams leading to this step!
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
     * @param stepMeta
     *            The step info
     * @param stepDataInterface
     *            the step data interface linked to this step. Here the step can
     *            store temporary data, database connections, etc.
     * @param copyNr
     *            The copy nr to get
     * @param transMeta
     *            The transformation info
     * @param trans
     *            The launching transformation
     */
    @Override public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
            int copyNr, TransMeta transMeta, Trans trans) {
        return new MolapAutoAggregateSliceMergerStep(stepMeta, stepDataInterface, copyNr, transMeta,
                trans);
    }

    /**
     * Get a new instance of the appropriate data class. This data class
     * implements the StepDataInterface. It basically contains the persisting
     * data that needs to live on, even if a worker thread is terminated.
     *
     * @return The appropriate StepDataInterface class.
     */
    @Override public StepDataInterface getStepData() {
        return new MolapAutoAggregateSliceMergerData();
    }

    /**
     * This method will be used to get the heir and its key suze string
     *
     * @return heirAndKeySize
     *
     */
    public String getHeirAndKeySize() {
        return heirAndKeySize;
    }

    /**
     * This method will be used to set the heir and key size string
     *
     * @param heirAndKeySize
     *
     */
    public void setHeirAndKeySize(String heirAndKeySize) {
        this.heirAndKeySize = heirAndKeySize;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName() {
        return cubeName;
    }

    /**
     * @param cubeName
     *            the cubeName to set
     */
    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    /**
     * @return the tableNames
     */
    public String[] getTableNames() {
        return tableNames;
    }

    /**
     * @param tableNames
     *            the tableNames to set
     */
    public void setTableNames(String[] tableNames) {
        this.tableNames = tableNames;
    }

    /**
     * @return the mapOfTableAndMdkeySize
     */
    public Map<String, Integer> getMapOfTableAndMdkeySize() {
        return mapOfTableAndMdkeySize;
    }

    /**
     * @param mapOfTableAndMdkeySize
     *            the mapOfTableAndMdkeySize to set
     */
    public void setMapOfTableAndMdkeySize(Map<String, Integer> mapOfTableAndMdkeySize) {
        this.mapOfTableAndMdkeySize = mapOfTableAndMdkeySize;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName
     *            the schemaName to set
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return the mapOfTableAndMeasureCount
     */
    public Map<String, Integer> getMapOfTableAndMeasureCount() {
        return mapOfTableAndMeasureCount;
    }

    /**
     * @param mapOfTableAndMeasureCount
     *            the mapOfTableAndMeasureCount to set
     */
    public void setMapOfTableAndMeasureCount(Map<String, Integer> mapOfTableAndMeasureCount) {
        this.mapOfTableAndMeasureCount = mapOfTableAndMeasureCount;
    }

    /**
     * @return the mapOfAggTableAndAggClass
     */
    public Map<String, String[]> getMapOfAggTableAndAggClass() {
        return mapOfAggTableAndAggClass;
    }

    /**
     * @param mapOfAggTableAndAggClass the mapOfAggTableAndAggClass to set
     */
    public void setMapOfAggTableAndAggClass(Map<String, String[]> mapOfAggTableAndAggClass) {
        this.mapOfAggTableAndAggClass = mapOfAggTableAndAggClass;
    }

    /**
     * @return the mapOfAggTableAndAgg
     */
    public Map<String, String[]> getMapOfAggTableAndAgg() {
        return mapOfAggTableAndAgg;
    }

    /**
     * @param mapOfAggTableAndAgg the mapOfAggTableAndAgg to set
     */
    public void setMapOfAggTableAndAgg(Map<String, String[]> mapOfAggTableAndAgg) {
        this.mapOfAggTableAndAgg = mapOfAggTableAndAgg;
    }

    /**
     * @return the tabelName
     */
    public String getTabelName() {
        return tabelName;
    }

    /**
     * @param tabelName the tabelName to set
     */
    public void setTabelName(String tabelName) {
        this.tabelName = tabelName;
    }

    /**
     * @return the mdkeySize
     */
    public String getMdkeySize() {
        return mdkeySize;
    }

    /**
     * @param mdkeySize the mdkeySize to set
     */
    public void setMdkeySize(String mdkeySize) {
        this.mdkeySize = mdkeySize;
    }

    /**
     * @return the measureCount
     */
    public String getMeasureCount() {
        return measureCount;
    }

    /**
     * @param measureCount the measureCount to set
     */
    public void setMeasureCount(String measureCount) {
        this.measureCount = measureCount;
    }

    /**
     * @return the aggregatorString
     */
    public String getAggregatorString() {
        return aggregatorString;
    }

    /**
     * @param aggregatorString the aggregatorString to set
     */
    public void setAggregatorString(String aggregatorString) {
        this.aggregatorString = aggregatorString;
    }

    /**
     * @return the aggregatorClassString
     */
    public String getAggregatorClassString() {
        return aggregatorClassString;
    }

    /**
     * @param aggregatorClassString the aggregatorClassString to set
     */
    public void setAggregatorClassString(String aggregatorClassString) {
        this.aggregatorClassString = aggregatorClassString;
    }

    /**
     * @return the factDimLensString
     */
    public String getFactDimLensString() {
        return factDimLensString;
    }

    /**
     * @param factDimLensString the factDimLensString to set
     */
    public void setFactDimLensString(String factDimLensString) {
        this.factDimLensString = factDimLensString;
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
