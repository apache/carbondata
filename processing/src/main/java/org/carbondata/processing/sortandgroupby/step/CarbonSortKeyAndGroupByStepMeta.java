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

package org.carbondata.processing.sortandgroupby.step;

import java.util.List;
import java.util.Map;

import org.carbondata.core.constants.CarbonCommonConstants;
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

public class CarbonSortKeyAndGroupByStepMeta extends BaseStepMeta implements StepMetaInterface {
    /**
     * PKG
     */
    private static final Class<?> PKG = CarbonSortKeyAndGroupByStepMeta.class;

    /**
     * tabelName
     */
    private String tabelName;

    /**
     * outputRowSize
     */
    private String outputRowSize;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * isAutoAggRequest
     */
    private String autoAggRequest;

    /**
     * measureCount
     */
    private String measureCount;

    /**
     * isMDkeyInInputRow
     */
    private String isFactMDkeyInInputRow;

    private String aggregatorString;

    private String aggregatorClassString;

    private String factDimLensString;

    /**
     * factTableName
     */
    private String factTableName;

    private String factStorePath;

    /**
     * aggregateMeasuresString
     */
    private String aggregateMeasuresString;

    /**
     * aggregateMeasures
     */
    private String[] aggregateMeasures;

    /**
     * factMeasureString
     */
    private String factMeasureString;

    /**
     * factMeasure
     */
    private String[] factMeasure;

    /**
     * isUpdateMemberRequest
     */
    private String updateMemberRequest;

    /**
     * aggregateLevelsString
     */
    private String aggregateLevelsString;

    /**
     * aggregateLevels
     */
    private String[] aggregateLevels;

    /**
     * factLevelsString
     */
    private String factLevelsString;

    /**
     * factLevels
     */
    private String[] factLevels;

    /**
     * aggDimeLensString
     */
    private String aggDimeLensString;

    /**
     * aggDimeLens
     */
    private int[] aggDimeLens;

    /**
     * heirAndKeySize
     */
    private String heirAndKeySize;

    /**
     * isManualAutoAggRequest
     */
    private String manualAutoAggRequest;

    /**
     * aggregateMeasuresColumnNameString
     */
    private String aggregateMeasuresColumnNameString;

    /**
     * aggregateMeasuresColumnName
     */
    private String[] aggregateMeasuresColumnName;

    /**
     * heirAndDimLens
     */
    private String heirAndDimLens;

    /**
     * factDimLens
     */
    private int[] factDimLens;

    private int currentRestructNumber;

    /**
     * mdkeyLength
     */
    private String mdkeyLength;

    /**
     *
     */
    private int noDictionaryCount;

    /**
     * set the default value for all the properties
     */
    @Override
    public void setDefault() {
        this.tabelName = "";
        aggregatorClassString = "";
        outputRowSize = "";
        autoAggRequest = "";
        measureCount = "";
        isFactMDkeyInInputRow = "";
        updateMemberRequest = "";
        factMeasureString = "";
        schemaName = "";
        cubeName = "";
        factLevelsString = "";
        aggregateLevelsString = "";
        aggregateMeasuresString = "";
        factTableName = "";
        heirAndKeySize = "";
        heirAndDimLens = "";
        aggregatorString = "";
        aggDimeLensString = "";
        aggregateMeasuresColumnNameString = "";
        manualAutoAggRequest = "";
        factDimLensString = "";
        factStorePath = "";
        currentRestructNumber = -1;
        noDictionaryCount = -1;
        mdkeyLength = "";
    }

    /**
     * below method will be used initialise the meta
     */
    public void initialize() {
        factLevels = factLevelsString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
        aggregateMeasuresColumnName =
                aggregateMeasuresColumnNameString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
        factMeasure = factMeasureString.split(CarbonCommonConstants.AMPERSAND_SPC_CHARACTER);
        aggregateLevels = aggregateLevelsString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
        aggregateMeasures = aggregateMeasuresString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);

        String[] dimLensStringArray =
                factDimLensString.split(CarbonCommonConstants.COMA_SPC_CHARACTER);
        factDimLens = new int[dimLensStringArray.length];
        for (int i = 0; i < dimLensStringArray.length; i++) {
            factDimLens[i] = Integer.parseInt(dimLensStringArray[i]);
        }
    }

    /**
     * Get the XML that represents the values in this step
     *
     * @return the XML that represents the metadata in this step
     * @throws KettleException in case there is a conversion or XML encoding error
     */
    public String getXML() {
        StringBuffer strBuffValue = new StringBuffer(150);
        strBuffValue.append("    ").append(XMLHandler.addTagValue("TableName", this.tabelName));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("aggregatorClassString", aggregatorClassString));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("outputRowSize", this.outputRowSize));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("isAutoAggRequest", this.autoAggRequest));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("measureCount", this.measureCount));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("isMDkeyInInputRow", this.isFactMDkeyInInputRow));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("isUpdateMemberRequest", this.updateMemberRequest));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("factStorePath", this.factStorePath));
        strBuffValue.append("    ").append(XMLHandler
                .addTagValue("aggregateMeasuresString", this.aggregateMeasuresString));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("factMeasureString", this.factMeasureString));
        strBuffValue.append("    ").append(XMLHandler.addTagValue("cubeName", cubeName));
        strBuffValue.append("    ").append(XMLHandler.addTagValue("schemaName", schemaName));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("factLevelsString", factLevelsString));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("aggregateLevelsString", aggregateLevelsString));
        strBuffValue.append("    ").append(XMLHandler.addTagValue("factTableName", factTableName));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("heirAndKeySize", heirAndKeySize));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("heirAndDimLens", heirAndDimLens));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("aggDimeLensString", aggDimeLensString));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("aggregatorString", aggregatorString));
        strBuffValue.append("    ").append(XMLHandler
                .addTagValue("aggregateMeasuresColumnNameString",
                        aggregateMeasuresColumnNameString));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("isManualAutoAggRequest", manualAutoAggRequest));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("factDimLensString", factDimLensString));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        strBuffValue.append("    ")
                .append(XMLHandler.addTagValue("noDictionaryCount", noDictionaryCount));
        strBuffValue.append("    ").append(XMLHandler.addTagValue("mdkeyLength", mdkeyLength));
        return strBuffValue.toString();
    }

    /**
     * Load the values for this step from an XML Node
     *
     * @param stepnode  the Node to get the info from
     * @param databases The available list of databases to reference to
     * @param counters  Counters to reference.
     * @throws KettleXMLException When an unexpected XML error occurred. (malformed etc.)
     */
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleXMLException {
        try {
            this.tabelName = XMLHandler.getTagValue(stepnode, "TableName");
            this.aggregatorClassString = XMLHandler.getTagValue(stepnode, "aggregatorClassString");
            this.aggregatorString = XMLHandler.getTagValue(stepnode, "aggregatorString");
            this.outputRowSize = XMLHandler.getTagValue(stepnode, "outputRowSize");
            this.factDimLensString = XMLHandler.getTagValue(stepnode, "factDimLensString");
            this.cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            this.schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            this.autoAggRequest = XMLHandler.getTagValue(stepnode, "isAutoAggRequest");
            this.measureCount = XMLHandler.getTagValue(stepnode, "measureCount");
            this.isFactMDkeyInInputRow = XMLHandler.getTagValue(stepnode, "isMDkeyInInputRow");
            this.updateMemberRequest = XMLHandler.getTagValue(stepnode, "isUpdateMemberRequest");
            this.factTableName = XMLHandler.getTagValue(stepnode, "factTableName");
            this.factStorePath = XMLHandler.getTagValue(stepnode, "factStorePath");
            this.aggregateMeasuresString =
                    XMLHandler.getTagValue(stepnode, "aggregateMeasuresString");
            this.factMeasureString = XMLHandler.getTagValue(stepnode, "factMeasureString");
            aggregateMeasuresColumnNameString =
                    XMLHandler.getTagValue(stepnode, "aggregateMeasuresColumnNameString");
            factLevelsString = XMLHandler.getTagValue(stepnode, "factLevelsString");
            aggregateLevelsString = XMLHandler.getTagValue(stepnode, "aggregateLevelsString");
            heirAndKeySize = XMLHandler.getTagValue(stepnode, "heirAndKeySize");
            heirAndDimLens = XMLHandler.getTagValue(stepnode, "heirAndDimLens");
            aggDimeLensString = XMLHandler.getTagValue(stepnode, "aggDimeLensString");
            manualAutoAggRequest = XMLHandler.getTagValue(stepnode, "isManualAutoAggRequest");
            currentRestructNumber =
                    Integer.parseInt(XMLHandler.getTagValue(stepnode, "currentRestructNumber"));
            noDictionaryCount =
                    Integer.parseInt(XMLHandler.getTagValue(stepnode, "noDictionaryCount"));
            mdkeyLength = XMLHandler.getTagValue(stepnode, "mdkeyLength");
        } catch (Exception e) {
            throw new KettleXMLException("Unable to read step info from XML node", e);
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
    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
            throws KettleException {
        try {
            rep.saveStepAttribute(idTransformation, idStep, "TableName", this.tabelName);
            rep.saveStepAttribute(idTransformation, idStep, "aggregatorClassString",
                    aggregatorClassString);

            rep.saveStepAttribute(idTransformation, idStep, "aggregatorString", aggregatorString);
            rep.saveStepAttribute(idTransformation, idStep, "factDimLensString", factDimLensString);
            rep.saveStepAttribute(idTransformation, idStep, "outputRowSize",
                    this.outputRowSize); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "cubeName", this.cubeName);
            rep.saveStepAttribute(idTransformation, idStep, "schemaName",
                    this.schemaName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "isAutoAggRequest",
                    this.autoAggRequest); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "measureCount",
                    this.measureCount); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "isMDkeyInInputRow",
                    this.isFactMDkeyInInputRow); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "isUpdateMemberRequest",
                    this.updateMemberRequest); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "factTableName", this.factTableName);
            rep.saveStepAttribute(idTransformation, idStep, "factStorePath", this.factStorePath);
            rep.saveStepAttribute(idTransformation, idStep, "aggregateMeasuresString",
                    this.aggregateMeasuresString);
            rep.saveStepAttribute(idTransformation, idStep, "factMeasureString",
                    this.factMeasureString);
            rep.saveStepAttribute(idTransformation, idStep, "aggregateMeasuresColumnNameString",
                    aggregateMeasuresColumnNameString);
            rep.saveStepAttribute(idTransformation, idStep, "factLevelsString",
                    factLevelsString); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "aggregateLevelsString",
                    aggregateLevelsString);
            rep.saveStepAttribute(idTransformation, idStep, "heirAndDimLens", heirAndDimLens);
            rep.saveStepAttribute(idTransformation, idStep, "heirAndKeySize", heirAndKeySize);
            rep.saveStepAttribute(idTransformation, idStep, "aggDimeLensString", aggDimeLensString);
            rep.saveStepAttribute(idTransformation, idStep, "isManualAutoAggRequest",
                    manualAutoAggRequest);
            rep.saveStepAttribute(idTransformation, idStep, "currentRestructNumber",
                    currentRestructNumber);
            rep.saveStepAttribute(idTransformation, idStep, "noDictionaryCount",
                    noDictionaryCount);
            rep.saveStepAttribute(idTransformation, idStep, "mdkeyLength", mdkeyLength);
        } catch (Exception e) {
            throw new KettleException(BaseMessages
                    .getString(PKG, "TemplateStep.Exception.UnableToSaveStepInfoToRepository",
                            new String[0]) + idStep, e);
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
    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
            Map<String, Counter> counters) throws KettleException {
        try {
            this.tabelName = rep.getStepAttributeString(idStep, "TableName");
            this.aggregatorClassString =
                    rep.getStepAttributeString(idStep, "aggregatorClassString");
            this.aggregatorString = rep.getStepAttributeString(idStep, "aggregatorString");
            this.factDimLensString = rep.getStepAttributeString(idStep, "factDimLensString");
            this.outputRowSize = rep.getStepAttributeString(idStep, "outputRowSize");
            this.schemaName = rep.getStepAttributeString(idStep, "schemaName");
            this.cubeName = rep.getStepAttributeString(idStep, "cubeName");
            this.autoAggRequest = rep.getStepAttributeString(idStep, "isAutoAggRequest");
            this.measureCount = rep.getStepAttributeString(idStep, "measureCount");
            this.isFactMDkeyInInputRow = rep.getStepAttributeString(idStep, "isMDkeyInInputRow");
            this.updateMemberRequest = rep.getStepAttributeString(idStep, "isUpdateMemberRequest");
            this.factTableName = rep.getStepAttributeString(idStep, "factTableName");
            this.factStorePath = rep.getStepAttributeString(idStep, "factStorePath");
            this.aggregateMeasuresString =
                    rep.getStepAttributeString(idStep, "aggregateMeasuresString");
            this.factMeasureString = rep.getStepAttributeString(idStep, "factMeasureString");
            aggregateMeasuresColumnNameString =
                    rep.getStepAttributeString(idStep, "aggregateMeasuresColumnNameString");
            factLevelsString = rep.getStepAttributeString(idStep, "factLevelsString");
            aggregateLevelsString = rep.getStepAttributeString(idStep, "aggregateLevelsString");
            heirAndDimLens = rep.getStepAttributeString(idStep, "heirAndDimLens");
            heirAndKeySize = rep.getStepAttributeString(idStep, "heirAndKeySize");
            aggDimeLensString = rep.getStepAttributeString(idStep, "aggDimeLensString");
            manualAutoAggRequest = rep.getStepAttributeString(idStep, "isManualAutoAggRequest");
            currentRestructNumber =
                    (int) rep.getStepAttributeInteger(idStep, "currentRestructNumber");
            noDictionaryCount =
                    (int) rep.getStepAttributeInteger(idStep, "noDictionaryCount");
            mdkeyLength = rep.getStepAttributeString(idStep, "mdkeyLength");
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG,
                    "CarbonDataWriterStepMeta.Exception.UnexpectedErrorInReadingStepInfo",
                    new String[0]), e);
        }
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
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
            RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
        if (input.length > 0) {
            CheckResult cr =
                    new CheckResult(1, "Step is receiving info from other steps.", stepMeta);
            remarks.add(cr);
        } else {
            CheckResult cr = new CheckResult(4, "No input received from other steps!", stepMeta);
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
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans) {
        return new CarbonSortKeyAndGroupByStep(stepMeta, stepDataInterface, copyNr, transMeta,
                trans);
    }

    /**
     * Get a new instance of the appropriate data class. This data class
     * implements the StepDataInterface. It basically contains the persisting
     * data that needs to live on, even if a worker thread is terminated.
     *
     * @return The appropriate StepDataInterface class.
     */
    public StepDataInterface getStepData() {
        return new CarbonSortKeyAndGroupByStepData();
    }

    /**
     * This method will return the table name
     *
     * @return tabelName
     */

    public String getTabelName() {
        return this.tabelName;
    }

    /**
     * This method will set the table name
     *
     * @param tabelName
     */
    public void setTabelName(String tabelName) {
        this.tabelName = tabelName;
    }

    /**
     * Below method will be used to get the out row size
     *
     * @return outputRowSize
     */
    public String getOutputRowSize() {
        return outputRowSize;
    }

    /**
     * below mthod will be used to set the out row size
     *
     * @param outputRowSize
     */
    public void setOutputRowSize(String outputRowSize) {
        this.outputRowSize = outputRowSize;
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
     * @return the isAutoAggRequest
     */
    public boolean isAutoAggRequest() {
        return Boolean.parseBoolean(autoAggRequest);
    }

    /**
     * @param isAutoAggRequest the isAutoAggRequest to set
     */
    public void setIsAutoAggRequest(String isAutoAggRequest) {
        this.autoAggRequest = isAutoAggRequest;
    }

    /**
     * @return the measureCount
     */
    public int getMeasureCount() {
        return Integer.parseInt(measureCount);
    }

    /**
     * @param measureCount the measureCount to set
     */
    public void setMeasureCount(String measureCount) {
        this.measureCount = measureCount;
    }

    /**
     * @return the isMDkeyInInputRow
     */
    public boolean isFactMdKeyInInputRow() {
        return Boolean.parseBoolean(isFactMDkeyInInputRow);
    }

    /**
     * @param isMDkeyInInputRow the isMDkeyInInputRow to set
     */
    public void setFactMdKeyInInputRow(String isMDkeyInInputRow) {
        this.isFactMDkeyInInputRow = isMDkeyInInputRow;
    }

    /**
     * @return the aggregators
     */
    public String[] getAggregators() {
        return aggregatorString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    /**
     * @return the aggregatorClass
     */
    public String[] getAggregatorClass() {
        return aggregatorClassString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    /**
     * @param aggregatorString the aggregatorString to set
     */
    public void setAggregatorString(String aggregatorString) {
        this.aggregatorString = aggregatorString;
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
     * @return the isUpdateMemberRequest
     */
    public boolean isUpdateMemberRequest() {
        return Boolean.parseBoolean(updateMemberRequest);
    }

    /**
     * @param isUpdateMemberRequest the isUpdateMemberRequest to set
     */
    public void setIsUpdateMemberRequest(String isUpdateMemberRequest) {
        this.updateMemberRequest = isUpdateMemberRequest;
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

    public String getFactStorePath() {
        return factStorePath;
    }

    public void setFactStorePath(String factStorePath) {
        this.factStorePath = factStorePath;
    }

    /**
     * @return the aggregateMeasuresString
     */
    public String getAggregateMeasuresString() {
        return aggregateMeasuresString;
    }

    /**
     * @param aggregateMeasuresString the aggregateMeasuresString to set
     */
    public void setAggregateMeasuresString(String aggregateMeasuresString) {
        this.aggregateMeasuresString = aggregateMeasuresString;
    }

    /**
     * @return the aggregateMeasures
     */
    public String[] getAggregateMeasures() {
        return aggregateMeasures;
    }

    /**
     * @param aggregateMeasures the aggregateMeasures to set
     */
    public void setAggregateMeasures(String[] aggregateMeasures) {
        this.aggregateMeasures = aggregateMeasures;
    }

    /**
     * @return the factMeasure
     */
    public String[] getFactMeasure() {
        return factMeasure;
    }

    /**
     * @param factMeasure the factMeasure to set
     */
    public void setFactMeasure(String[] factMeasure) {
        this.factMeasure = factMeasure;
    }

    /**
     * @return the factMeasureString
     */
    public String getFactMeasureString() {
        return factMeasureString;
    }

    /**
     * @param factMeasureString the factMeasureString to set
     */
    public void setFactMeasureString(String factMeasureString) {
        this.factMeasureString = factMeasureString;
    }

    /**
     * @return Returns the aggregateLevelsString.
     */
    public String getAggregateLevelsString() {
        return aggregateLevelsString;
    }

    /**
     * @param aggregateLevelsString The aggregateLevelsString to set.
     */
    public void setAggregateLevelsString(String aggregateLevelsString) {
        this.aggregateLevelsString = aggregateLevelsString;
    }

    /**
     * @return Returns the aggregateLevels.
     */
    public String[] getAggregateLevels() {
        return aggregateLevels;
    }

    /**
     * @param aggregateLevels The aggregateLevels to set.
     */
    public void setAggregateLevels(String[] aggregateLevels) {
        this.aggregateLevels = aggregateLevels;
    }

    /**
     * @return Returns the factLevelsString.
     */
    public String getFactLevelsString() {
        return factLevelsString;
    }

    /**
     * @param factLevelsString The factLevelsString to set.
     */
    public void setFactLevelsString(String factLevelsString) {
        this.factLevelsString = factLevelsString;
    }

    /**
     * @return Returns the factLevels.
     */
    public String[] getFactLevels() {
        return factLevels;
    }

    /**
     * @param factLevels The factLevels to set.
     */
    public void setFactLevels(String[] factLevels) {
        this.factLevels = factLevels;
    }

    /**
     * @return Returns the aggDimeLensString.
     */
    public String getAggDimeLensString() {
        return aggDimeLensString;
    }

    /**
     * @param aggDimeLensString The aggDimeLensString to set.
     */
    public void setAggDimeLensString(String aggDimeLensString) {
        this.aggDimeLensString = aggDimeLensString;
    }

    /**
     * @return Returns the aggDimeLens.
     */
    public int[] getAggDimeLens() {
        return aggDimeLens;
    }

    /**
     * @param aggDimeLens The aggDimeLens to set.
     */
    public void setAggDimeLens(int[] aggDimeLens) {
        this.aggDimeLens = aggDimeLens;
    }

    /**
     * @return Returns the heirAndKeySize.
     */
    public String getHeirAndKeySize() {
        return heirAndKeySize;
    }

    /**
     * @param heirAndKeySize The heirAndKeySize to set.
     */
    public void setHeirAndKeySize(String heirAndKeySize) {
        this.heirAndKeySize = heirAndKeySize;
    }

    /**
     * @return the isManualAutoAggRequest
     */
    public boolean isManualAutoAggRequest() {
        return Boolean.parseBoolean(manualAutoAggRequest);
    }

    /**
     * @param isManualAutoAggRequest the isManualAutoAggRequest to set
     */
    public void setIsManualAutoAggRequest(String isManualAutoAggRequest) {
        this.manualAutoAggRequest = isManualAutoAggRequest;
    }

    /**
     * @return Returns the aggregateMeasuresColumnNameString.
     */
    public String getAggregateMeasuresColumnNameString() {
        return aggregateMeasuresColumnNameString;
    }

    /**
     * @param aggregateMeasuresColumnNameString The aggregateMeasuresColumnNameString to set.
     */
    public void setAggregateMeasuresColumnNameString(String aggregateMeasuresColumnNameString) {
        this.aggregateMeasuresColumnNameString = aggregateMeasuresColumnNameString;
    }

    /**
     * @return Returns the aggregateMeasuresColumnName.
     */
    public String[] getAggregateMeasuresColumnName() {
        return aggregateMeasuresColumnName;
    }

    /**
     * @param aggregateMeasuresColumnName The aggregateMeasuresColumnName to set.
     */
    public void setAggregateMeasuresColumnName(String[] aggregateMeasuresColumnName) {
        this.aggregateMeasuresColumnName = aggregateMeasuresColumnName;
    }

    /**
     * @return Returns the heirAndDimLens.
     */
    public String getHeirAndDimLens() {
        return heirAndDimLens;
    }

    /**
     * @param heirAndDimLens The heirAndDimLens to set.
     */
    public void setHeirAndDimLens(String heirAndDimLens) {
        this.heirAndDimLens = heirAndDimLens;
    }

    /**
     * @return Returns the factDimLens.
     */
    public int[] getFactDimLens() {
        return factDimLens;
    }

    /**
     * @param factDimLens The factDimLens to set.
     */
    public void setFactDimLens(int[] factDimLens) {
        this.factDimLens = factDimLens;
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
     * @return the mdkeyLength
     */
    public int getMdkeyLength() {
        return Integer.parseInt(mdkeyLength);
    }

    /**
     * @param mdkeyLength the mdkeyLength to set
     */
    public void setMdkeyLength(String mdkeyLength) {
        this.mdkeyLength = mdkeyLength;
    }

    /**
     * @return
     */
    public int getNoDictionaryCount() {
        return noDictionaryCount;
    }

    /**
     * @param noDictionaryCount the noDictionaryCount to set
     */
    public void setNoDictionaryCount(int noDictionaryCount) {
        this.noDictionaryCount = noDictionaryCount;
    }

}