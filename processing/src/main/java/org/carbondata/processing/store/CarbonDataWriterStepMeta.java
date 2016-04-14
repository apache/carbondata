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

package org.carbondata.processing.store;

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

/**
 * This class is the entry point for CarbonDataWriterStepMeta plug-in
 */
public class CarbonDataWriterStepMeta extends BaseStepMeta implements StepMetaInterface, Cloneable {

    /**
     * for i18n purposes
     */
    private final Class<?> pkg = CarbonDataWriterStepMeta.class;

    /**
     * table name
     */
    private String tabelName;

    /**
     * leaf node size
     */
    private String leafNodeSize;

    /**
     * max leaf node size
     */
    private String maxLeafNode;

    /**
     * measureCount
     */
    private String measureCount;

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * isGroupByEnabled
     */
    private String groupByEnabled;

    /**
     * aggregatorString
     */
    private String aggregatorString;

    /**
     * aggregatorClassString
     */
    private String aggregatorClassString;

    /**
     * factDimLensString
     */
    private String factDimLensString;

    /**
     * isMDkeyInInputRow
     */
    private String isFactMDkeyInInputRow;

    /**
     * factMdkeyLength
     */
    private String factMdkeyLength;

    /**
     * isUpdateMemberRequest
     */
    private String updateMemberRequest;

    /**
     * aggregateLevelsString
     */
    private String aggregateLevelsString;

    /**
     * factLevelsString
     */
    private String factLevelsString;

    /**
     * mdkeyLength
     */
    private String mdkeyLength;

    private int currentRestructNumber;

    private int NoDictionaryCount;

    /**
     * CarbonDataWriterStepMeta constructor to initialize this class
     */
    public CarbonDataWriterStepMeta() {
        super();
    }

    /**
     * set the default value for all the properties
     */
    @Override
    public void setDefault() {
        tabelName = "";
        leafNodeSize = "";
        maxLeafNode = "";
        measureCount = "";
        schemaName = "";
        cubeName = "";
        groupByEnabled = "";
        aggregatorClassString = "";
        aggregatorString = "";
        factDimLensString = "";
        isFactMDkeyInInputRow = "";
        factMdkeyLength = "";
        updateMemberRequest = "";
        aggregateLevelsString = "";
        factLevelsString = "";
        mdkeyLength = "";
        currentRestructNumber = -1;
        NoDictionaryCount = -1;
    }

    /**
     * Get the XML that represents the values in this step
     *
     * @return the XML that represents the metadata in this step
     * @throws KettleException in case there is a conversion or XML encoding error
     */
    public String getXML() {
        StringBuffer retval = new StringBuffer(150);
        retval.append("    ").append(XMLHandler.addTagValue("TableName", tabelName));
        retval.append("    ").append(XMLHandler.addTagValue("LeafNodeSize", leafNodeSize));
        retval.append("    ").append(XMLHandler.addTagValue("MaxLeafInFile", maxLeafNode));
        retval.append("    ").append(XMLHandler.addTagValue("measureCount", measureCount));
        retval.append("    ").append(XMLHandler.addTagValue("schemaName", schemaName));
        retval.append("    ").append(XMLHandler.addTagValue("cubeName", cubeName));
        retval.append("    ").append(XMLHandler.addTagValue("isGroupByEnabled", groupByEnabled));
        retval.append("    ")
                .append(XMLHandler.addTagValue("aggregatorClassString", aggregatorClassString));
        retval.append("    ").append(XMLHandler.addTagValue("aggregatorString", aggregatorString));
        retval.append("    ")
                .append(XMLHandler.addTagValue("factDimLensString", factDimLensString));
        retval.append("    ")
                .append(XMLHandler.addTagValue("isFactMDkeyInInputRow", isFactMDkeyInInputRow));
        retval.append("    ").append(XMLHandler.addTagValue("factMdkeyLength", factMdkeyLength));
        retval.append("    ")
                .append(XMLHandler.addTagValue("isUpdateMemberRequest", updateMemberRequest));
        retval.append("    ")
                .append(XMLHandler.addTagValue("aggregateLevelsString", aggregateLevelsString));
        retval.append("    ").append(XMLHandler.addTagValue("factLevelsString", factLevelsString));
        retval.append("    ").append(XMLHandler.addTagValue("mdkeyLength", mdkeyLength));
        retval.append("    ")
                .append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        retval.append("    ").append(XMLHandler.addTagValue("NoDictionaryCount", NoDictionaryCount));
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
            tabelName = XMLHandler.getTagValue(stepnode, "TableName");
            leafNodeSize = XMLHandler.getTagValue(stepnode, "LeafNodeSize");
            maxLeafNode = XMLHandler.getTagValue(stepnode, "MaxLeafInFile");
            measureCount = XMLHandler.getTagValue(stepnode, "measureCount");
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            groupByEnabled = XMLHandler.getTagValue(stepnode, "isGroupByEnabled");
            aggregatorClassString = XMLHandler.getTagValue(stepnode, "aggregatorClassString");
            aggregatorString = XMLHandler.getTagValue(stepnode, "aggregatorString");
            factDimLensString = XMLHandler.getTagValue(stepnode, "factDimLensString");
            isFactMDkeyInInputRow = XMLHandler.getTagValue(stepnode, "isFactMDkeyInInputRow");
            factMdkeyLength = XMLHandler.getTagValue(stepnode, "factMdkeyLength");
            updateMemberRequest = XMLHandler.getTagValue(stepnode, "isUpdateMemberRequest");
            aggregateLevelsString = XMLHandler.getTagValue(stepnode, "aggregateLevelsString");
            factLevelsString = XMLHandler.getTagValue(stepnode, "factLevelsString");
            mdkeyLength = XMLHandler.getTagValue(stepnode, "mdkeyLength");
            currentRestructNumber =
                    Integer.parseInt(XMLHandler.getTagValue(stepnode, "currentRestructNumber"));
            NoDictionaryCount = Integer.parseInt(XMLHandler.getTagValue(stepnode, "NoDictionaryCount"));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

    /**
     * Make an exact copy of this step, make sure to explicitly copy Collections
     * etc.
     *
     * @return an exact copy of this step
     */
    public Object clone() {
        Object retval = super.clone();
        return retval;
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
            //
            rep.saveStepAttribute(idTransformation, idStep, "TableName", tabelName); //$NON-NLS-1$

            rep.saveStepAttribute(idTransformation, idStep, "schemaName", schemaName); //$NON-NLS-1$

            rep.saveStepAttribute(idTransformation, idStep, "cubeName", cubeName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "LeafNodeSize", leafNodeSize);
            //
            rep.saveStepAttribute(idTransformation, idStep, "MaxLeafInFile", maxLeafNode);
            rep.saveStepAttribute(idTransformation, idStep, "measureCount", measureCount);

            rep.saveStepAttribute(idTransformation, idStep, "isGroupByEnabled", groupByEnabled);

            rep.saveStepAttribute(idTransformation, idStep, "aggregatorClassString",
                    aggregatorClassString);

            rep.saveStepAttribute(idTransformation, idStep, "aggregatorString", aggregatorString);

            rep.saveStepAttribute(idTransformation, idStep, "factDimLensString", factDimLensString);

            rep.saveStepAttribute(idTransformation, idStep, "isFactMDkeyInInputRow",
                    isFactMDkeyInInputRow);

            rep.saveStepAttribute(idTransformation, idStep, "factMdkeyLength", factMdkeyLength);
            rep.saveStepAttribute(idTransformation, idStep, "isUpdateMemberRequest",
                    updateMemberRequest);

            rep.saveStepAttribute(idTransformation, idStep, "aggregateLevelsString",
                    aggregateLevelsString);
            rep.saveStepAttribute(idTransformation, idStep, "factLevelsString", factLevelsString);
            rep.saveStepAttribute(idTransformation, idStep, "mdkeyLength", mdkeyLength);
            rep.saveStepAttribute(idTransformation, idStep, "currentRestructNumber",
                    currentRestructNumber);
            rep.saveStepAttribute(idTransformation, idStep, "NoDictionaryCount", NoDictionaryCount);

        } catch (Exception e) {
            //
            throw new KettleException(BaseMessages
                    .getString(pkg, "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
                    + idStep, e);
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

            schemaName = rep.getStepAttributeString(idStep, "schemaName");
            cubeName = rep.getStepAttributeString(idStep, "cubeName");
            tabelName = rep.getStepAttributeString(idStep, "TableName");

            leafNodeSize = rep.getStepAttributeString(idStep, "leafNodeSize");
            maxLeafNode = rep.getStepAttributeString(idStep, "MaxLeafInFile");
            measureCount = rep.getStepAttributeString(idStep, "measureCount");
            groupByEnabled = rep.getStepAttributeString(idStep, "isGroupByEnabled");
            aggregatorClassString = rep.getStepAttributeString(idStep, "aggregatorClassString");
            aggregatorString = rep.getStepAttributeString(idStep, "aggregatorString");
            factDimLensString = rep.getStepAttributeString(idStep, "factDimLensString");
            isFactMDkeyInInputRow = rep.getStepAttributeString(idStep, "isFactMDkeyInInputRow");
            factMdkeyLength = rep.getStepAttributeString(idStep, "factMdkeyLength");
            updateMemberRequest = rep.getStepAttributeString(idStep, "isUpdateMemberRequest");
            aggregateLevelsString = rep.getStepAttributeString(idStep, "aggregateLevelsString");
            factLevelsString = rep.getStepAttributeString(idStep, "factLevelsString");
            mdkeyLength = rep.getStepAttributeString(idStep, "mdkeyLength");
            currentRestructNumber =
                    (int) rep.getStepAttributeInteger(idStep, "currentRestructNumber");
            NoDictionaryCount = (int) rep.getStepAttributeInteger(idStep, "NoDictionaryCount");
        } catch (Exception ex3) {
            //
            throw new KettleException(BaseMessages.getString(pkg,
                    "CarbonDataWriterStepMeta.Exception.UnexpectedErrorInReadingStepInfo"), ex3);
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
    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
            RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
        //
        CheckResult checkRes;

        // See if we have input streams leading to this step!
        if (input.length > 0) {
            //
            checkRes = new CheckResult(CheckResult.TYPE_RESULT_OK,
                    "Step is receiving info from other steps.", stepMeta);
            remarks.add(checkRes);
        } else {
            //
            checkRes = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
                    "No input received from other steps!", stepMeta);
            remarks.add(checkRes);
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
        return new CarbonDataWriterStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
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
        return new CarbonDataWriterStepData();
    }

    /**
     * This method will return the table name
     *
     * @return tabelName
     */
    public String getTabelName() {
        return tabelName;
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
     * This method will return the leaf node size
     *
     * @return leaf node size
     */
    public String getLeafNodeSize() {
        return leafNodeSize;
    }

    /**
     * This method will set the leaf nod size
     *
     * @param leafNodeSize
     */
    public void setLeafNodeSize(String leafNodeSize) {
        this.leafNodeSize = leafNodeSize;
    }

    /**
     * This method will return the max leaf node in one file
     *
     * @return maxLeafNode
     */
    public String getMaxLeafNode() {
        return maxLeafNode;
    }

    /**
     * This method will set the max number of leaf node in file
     *
     * @param maxLeafNode
     */
    public void setMaxLeafNode(String maxLeafNode) {
        this.maxLeafNode = maxLeafNode;
    }

    /**
     * Get measure count
     *
     * @return measureCount
     */
    public String getMeasureCount() {
        return measureCount;
    }

    /**
     * set measure count
     *
     * @param measureCount
     */
    public void setMeasureCount(String measureCount) {
        this.measureCount = measureCount;
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
     * @return the isGroupByEnabled
     */
    public boolean isGroupByEnabled() {
        return Boolean.parseBoolean(groupByEnabled);
    }

    /**
     * @param isGroupByEnabled the isGroupByEnabled to set
     */
    public void setGroupByEnabled(String isGroupByEnabled) {
        this.groupByEnabled = isGroupByEnabled;
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
     * @return the isFactMDkeyInInputRow
     */
    public boolean isFactMdKeyInInputRow() {
        return Boolean.parseBoolean(isFactMDkeyInInputRow);
    }

    /**
     * @param isFactMDkeyInInputRow the isFactMDkeyInInputRow to set
     */
    public void setIsFactMDkeyInInputRow(String isFactMDkeyInInputRow) {
        this.isFactMDkeyInInputRow = isFactMDkeyInInputRow;
    }

    /**
     * @return the factMdkeyLength
     */
    public int getFactMdkeyLength() {
        return Integer.parseInt(factMdkeyLength);
    }

    /**
     * @param factMdkeyLength the factMdkeyLength to set
     */
    public void setFactMdkeyLength(String factMdkeyLength) {
        this.factMdkeyLength = factMdkeyLength;
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
     * @param aggregateLevelsString the aggregateLevelsString to set
     */
    public void setAggregateLevelsString(String aggregateLevelsString) {
        this.aggregateLevelsString = aggregateLevelsString;
    }

    /**
     * @param factLevelsString the factLevelsString to set
     */
    public void setFactLevelsString(String factLevelsString) {
        this.factLevelsString = factLevelsString;
    }

    public String[] getFactLevels() {
        return this.factLevelsString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }

    public String[] getAggregateLevels() {
        return this.aggregateLevelsString.split(CarbonCommonConstants.HASH_SPC_CHARACTER);
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
     * @return the NoDictionaryCount
     */
    public int getNoDictionaryCount() {
        return NoDictionaryCount;
    }

    /**
     * @param NoDictionaryCount the NoDictionaryCount to set
     */
    public void setNoDictionaryCount(int NoDictionaryCount) {
        this.NoDictionaryCount = NoDictionaryCount;
    }
}
