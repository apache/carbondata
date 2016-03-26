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

package org.carbondata.processing.mdkeygen;

import java.util.List;
import java.util.Map;

import org.carbondata.processing.util.MolapDataProcessorUtil;
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

public class MolapMDKeyGenStepMeta extends BaseStepMeta implements StepMetaInterface, Cloneable {
    /**
     * for i18n purposes
     */
    private static Class<?> pkg = MolapMDKeyGenStepMeta.class; // for i18n
    // purposes

    /**
     * tableName
     */
    private String tableName;

    /**
     * aggregateLevels
     */
    private String aggregateLevels;

    /**
     * numberOfCores
     */
    private String numberOfCores;

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
     * isAutoAggRequest
     */
    private String factMdKeyInInputRow;

    /**
     * measureCount
     */
    private String measureCount;

    private int currentRestructNumber;

    /**
     * Constructor
     */
    public MolapMDKeyGenStepMeta() {
        super();
    }

    @Override public void setDefault() {
        tableName = "";
        aggregateLevels = "";
        numberOfCores = "";
        cubeName = "";
        schemaName = "";
        autoAggRequest = "";
        factMdKeyInInputRow = "";
        currentRestructNumber = -1;
    }

    /**
     *
     */
    public String getXML() {
        StringBuffer retval = new StringBuffer(150);

        retval.append("    ").append(XMLHandler.addTagValue("TableName", tableName));
        retval.append("    ").append(XMLHandler.addTagValue("AggregateLevels", aggregateLevels));
        retval.append("    ").append(XMLHandler.addTagValue("NumberOfCores", numberOfCores));

        retval.append("    ").append(XMLHandler.addTagValue("cubeName", cubeName));
        retval.append("    ").append(XMLHandler.addTagValue("schemaName", schemaName));
        retval.append("    ").append(XMLHandler.addTagValue("isAutoAggRequest", autoAggRequest));
        retval.append("    ")
                .append(XMLHandler.addTagValue("isFactMdKeyInInputRow", factMdKeyInInputRow));
        retval.append("    ").append(XMLHandler.addTagValue("measureCount", measureCount));
        retval.append("    ")
                .append(XMLHandler.addTagValue("currentRestructNumber", currentRestructNumber));
        return retval.toString();
    }

    @Override public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
            throws KettleException {
        try {
            rep.saveStepAttribute(idTransformation, idStep, "TableName", tableName); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "AggregateLevels",
                    aggregateLevels); //$NON-NLS-1$
            rep.saveStepAttribute(idTransformation, idStep, "NumberOfCores", numberOfCores);

            rep.saveStepAttribute(idTransformation, idStep, "schemaName", schemaName);
            rep.saveStepAttribute(idTransformation, idStep, "cubeName", cubeName);
            rep.saveStepAttribute(idTransformation, idStep, "isAutoAggRequest", autoAggRequest);
            rep.saveStepAttribute(idTransformation, idStep, "isFactMdKeyInInputRow",
                    factMdKeyInInputRow);
            rep.saveStepAttribute(idTransformation, idStep, "measureCount", measureCount);
            rep.saveStepAttribute(idTransformation, idStep, "currentRestructNumber",
                    currentRestructNumber);

        } catch (Exception e) {
            throw new KettleException(BaseMessages
                    .getString(pkg, "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
                    + idStep, e);
        }

    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
            throws KettleXMLException {
        try {
            tableName = XMLHandler.getTagValue(stepnode, "TableName");
            aggregateLevels = XMLHandler.getTagValue(stepnode, "AggregateLevels");
            numberOfCores = XMLHandler.getTagValue(stepnode, "NumberOfCores");
            schemaName = XMLHandler.getTagValue(stepnode, "schemaName");
            cubeName = XMLHandler.getTagValue(stepnode, "cubeName");
            autoAggRequest = XMLHandler.getTagValue(stepnode, "isAutoAggRequest");
            factMdKeyInInputRow = XMLHandler.getTagValue(stepnode, "isFactMdKeyInInputRow");
            measureCount = XMLHandler.getTagValue(stepnode, "measureCount");
            currentRestructNumber =
                    Integer.parseInt(XMLHandler.getTagValue(stepnode, "currentRestructNumber"));
        } catch (Exception ex) {
            throw new KettleXMLException("Unable to read step info from XML node", ex);
        }
    }

    public Object clone() {
        Object retval = super.clone();
        return retval;
    }

    @Override public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
            Map<String, Counter> counters) throws KettleException {
        try {
            tableName = rep.getStepAttributeString(idStep, "TableName");
            aggregateLevels = rep.getStepAttributeString(idStep, "AggregateLevels");
            numberOfCores = rep.getStepAttributeString(idStep, "NumberOfCores");

            schemaName = rep.getStepAttributeString(idStep, "schemaName");
            cubeName = rep.getStepAttributeString(idStep, "cubeName");
            autoAggRequest = rep.getStepAttributeString(idStep, "isAutoAggRequest");
            factMdKeyInInputRow = rep.getStepAttributeString(idStep, "isFactMdKeyInInputRow");
            measureCount = rep.getStepAttributeString(idStep, "measureCount");
            currentRestructNumber =
                    (int) rep.getStepAttributeInteger(idStep, "currentRestructNumber");
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(pkg,
                    "MolapMDKeyStepMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
        }

    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
            RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
        MolapDataProcessorUtil.checkResult(remarks, stepMeta, input);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
            TransMeta transMeta, Trans trans) {
        return new MolapMDKeyGenStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override public StepDataInterface getStepData() {
        return new MolapMDKeyGenStepData();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getNumberOfCores() {
        return numberOfCores;
    }

    public void setNumberOfCores(String numberOfCores) {
        this.numberOfCores = numberOfCores;
    }

    public String getAggregateLevels() {
        return aggregateLevels;
    }

    public void setAggregateLevels(String aggregateLevels) {
        this.aggregateLevels = aggregateLevels;
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
     * @return the isFactMdKeyInInputRow
     */
    public boolean isFactMdKeyInInputRow() {
        return Boolean.parseBoolean(factMdKeyInInputRow);
    }

    /**
     * @param isFactMdKeyInInputRow the isFactMdKeyInInputRow to set
     */
    public void setFactMdKeyInInputRow(String isFactMdKeyInInputRow) {
        this.factMdKeyInInputRow = isFactMdKeyInInputRow;
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
