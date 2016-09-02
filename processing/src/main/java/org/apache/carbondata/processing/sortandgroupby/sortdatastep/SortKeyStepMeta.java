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

package org.apache.carbondata.processing.sortandgroupby.sortdatastep;

import java.util.List;
import java.util.Map;

import org.apache.carbondata.processing.sortdatastep.SortKeyStepData;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

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
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

public class SortKeyStepMeta extends BaseStepMeta implements StepMetaInterface {
  /**
   * PKG
   */
  private static final Class<?> PKG = SortKeyStepMeta.class;

  /**
   * tabelName
   */
  private String tabelName;

  /**
   * outputRowSize
   */
  private String outputRowSize;

  /**
   * tableName
   */
  private String tableName;

  /**
   * databaseName
   */
  private String databaseName;

  /**
   * Dimension Count
   */
  private String dimensionCount;

  /**
   * ComplexTypes Count
   */
  private String complexDimensionCount;

  /**
   * Dimension Count
   */
  private int noDictionaryCount;

  /**
   * measureCount
   */
  private String measureCount;

  private String factDimLensString;

  /**
   * isUpdateMemberRequest
   */
  private String updateMemberRequest;

  private String measureDataType;

  private String noDictionaryDims;
  /**
   * partitionID
   */
  private String partitionID;
  /**
   * Id of the load folder
   */
  private String segmentId;
  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;
  /**
   * To determine the column whether is dictionary or not.
   */
  private String noDictionaryDimsMapping;

  /**
   * set the default value for all the properties
   */
  @Override public void setDefault() {
    this.tabelName = "";
    factDimLensString = "";
    outputRowSize = "";
    databaseName = "";
    noDictionaryDims = "";
    noDictionaryDimsMapping = "";
    tableName = "";
    dimensionCount = "";
    complexDimensionCount = "";
    measureCount = "";
    updateMemberRequest = "";
    measureDataType = "";
    partitionID = "";
    segmentId = "";
    taskNo = "";
  }

  /**
   * Get the XML that represents the values in this step
   *
   * @return the XML that represents the metadata in this step
   * @throws KettleException in case there is a conversion or XML encoding error
   */
  public String getXML() {
    StringBuffer retval = new StringBuffer(150);
    retval.append("    ").append(XMLHandler.addTagValue("TableName", this.tabelName));
    retval.append("    ").append(XMLHandler.addTagValue("factDimLensString", factDimLensString));
    retval.append("    ").append(XMLHandler.addTagValue("outputRowSize", this.outputRowSize));
    retval.append("    ").append(XMLHandler.addTagValue("tableName", this.tableName));
    retval.append("    ").append(XMLHandler.addTagValue("databaseName", this.databaseName));
    retval.append("    ").append(XMLHandler.addTagValue("dimensionCount", this.dimensionCount));
    retval.append("    ").append(XMLHandler.addTagValue("noDictionaryDims", this.noDictionaryDims));
    retval.append("    ")
        .append(XMLHandler.addTagValue("noDictionaryDimsMapping", this.noDictionaryDimsMapping));
    retval.append("    ")
        .append(XMLHandler.addTagValue("complexDimensionCount", this.complexDimensionCount));
    retval.append("    ").append(XMLHandler.addTagValue("measureCount", this.measureCount));
    retval.append("    ")
        .append(XMLHandler.addTagValue("isUpdateMemberRequest", this.updateMemberRequest));
    retval.append("    ").append(XMLHandler.addTagValue("measureDataType", measureDataType));
    retval.append("    ").append(XMLHandler.addTagValue("partitionID", partitionID));
    retval.append("    ").append(XMLHandler.addTagValue("segmentId", segmentId));
    retval.append("    ").append(XMLHandler.addTagValue("taskNo", taskNo));
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
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleXMLException {
    try {
      this.tabelName = XMLHandler.getTagValue(stepnode, "TableName");
      this.outputRowSize = XMLHandler.getTagValue(stepnode, "outputRowSize");
      this.factDimLensString = XMLHandler.getTagValue(stepnode, "factDimLensString");
      this.tableName = XMLHandler.getTagValue(stepnode, "tableName");
      this.databaseName = XMLHandler.getTagValue(stepnode, "databaseName");
      this.dimensionCount = XMLHandler.getTagValue(stepnode, "dimensionCount");
      this.noDictionaryDims = XMLHandler.getTagValue(stepnode, "noDictionaryDims");
      this.noDictionaryDimsMapping = XMLHandler.getTagValue(stepnode, "noDictionaryDimsMapping");
      this.complexDimensionCount = XMLHandler.getTagValue(stepnode, "complexDimensionCount");
      this.measureCount = XMLHandler.getTagValue(stepnode, "measureCount");
      this.updateMemberRequest = XMLHandler.getTagValue(stepnode, "isUpdateMemberRequest");
      this.measureDataType = XMLHandler.getTagValue(stepnode, "measureDataType");
      this.partitionID = XMLHandler.getTagValue(stepnode, "partitionID");
      this.segmentId = XMLHandler.getTagValue(stepnode, "segmentId");
      this.taskNo = XMLHandler.getTagValue(stepnode, "taskNo");
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

      rep.saveStepAttribute(idTransformation, idStep, "factDimLensString", factDimLensString);
      rep.saveStepAttribute(idTransformation, idStep, "outputRowSize", this.outputRowSize);
      rep.saveStepAttribute(idTransformation, idStep, "tableName", this.tableName);
      rep.saveStepAttribute(idTransformation, idStep, "databaseName", this.databaseName);
      rep.saveStepAttribute(idTransformation, idStep, "dimensionCount", this.dimensionCount);
      rep.saveStepAttribute(idTransformation, idStep, "noDictionaryDims", this.noDictionaryDims);
      rep.saveStepAttribute(idTransformation, idStep, "noDictionaryDimsMapping",
          this.noDictionaryDimsMapping);
      rep.saveStepAttribute(idTransformation, idStep, "complexDimensionCount",
          this.complexDimensionCount);
      rep.saveStepAttribute(idTransformation, idStep, "measureCount", this.measureCount);
      rep.saveStepAttribute(idTransformation, idStep, "isUpdateMemberRequest",
          this.updateMemberRequest);
      rep.saveStepAttribute(idTransformation, idStep, "measureDataType", measureDataType);
      rep.saveStepAttribute(idTransformation, idStep, "partitionID", partitionID);
      rep.saveStepAttribute(idTransformation, idStep, "segmentId", segmentId);
      rep.saveStepAttribute(idTransformation, idStep, "taskNo", taskNo);
    } catch (Exception e) {
      throw new KettleException(BaseMessages
          .getString(PKG, "TemplateStep.Exception.UnableToSaveStepInfoToRepository", new String[0])
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
  public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleException {
    try {
      this.tabelName = rep.getStepAttributeString(idStep, "TableName");
      this.outputRowSize = rep.getStepAttributeString(idStep, "outputRowSize");
      this.databaseName = rep.getStepAttributeString(idStep, "databaseName");
      this.tableName = rep.getStepAttributeString(idStep, "tableName");
      this.dimensionCount = rep.getStepAttributeString(idStep, "dimensionCount");
      this.noDictionaryDims = rep.getStepAttributeString(idStep, "noDictionaryDims");
      this.noDictionaryDims = rep.getStepAttributeString(idStep, "noDictionaryDimsMapping");
      this.complexDimensionCount = rep.getStepAttributeString(idStep, "complexDimensionCount");
      this.measureCount = rep.getStepAttributeString(idStep, "measureCount");
      this.updateMemberRequest = rep.getStepAttributeString(idStep, "isUpdateMemberRequest");
      this.measureDataType = rep.getStepAttributeString(idStep, "measureDataType");
      this.partitionID = rep.getStepAttributeString(idStep, "partitionID");
      this.segmentId = rep.getStepAttributeString(idStep, "segmentId");
      this.taskNo = rep.getStepAttributeString(idStep, "taskNo");
    } catch (Exception ex) {
      throw new KettleException(BaseMessages
          .getString(PKG, "CarbonDataWriterStepMeta.Exception.UnexpectedErrorInReadingStepInfo",
              new String[0]), ex);
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
    CarbonDataProcessorUtil.checkResult(remarks, stepMeta, input);
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
    return new SortKeyStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  /**
   * Get a new instance of the appropriate data class. This data class
   * implements the StepDataInterface. It basically contains the persisting
   * data that needs to live on, even if a worker thread is terminated.
   *
   * @return The appropriate StepDataInterface class.
   */
  public StepDataInterface getStepData() {
    return new SortKeyStepData();
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
   * @return the databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * @param databaseName the databaseName to set
   */
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * @return the dimensionCount
   */
  public int getDimensionCount() {
    return Integer.parseInt(dimensionCount);
  }

  public void setDimensionCount(String dimensionCount) {
    this.dimensionCount = dimensionCount;
  }

  /**
   * @return the complexDimensionCount
   */
  public int getComplexDimensionCount() {
    return Integer.parseInt(complexDimensionCount);
  }

  public void setComplexDimensionCount(String complexDimensionCount) {
    this.complexDimensionCount = complexDimensionCount;
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

  public String getMeasureDataType() {
    return measureDataType;
  }

  public void setMeasureDataType(String measureDataType) {
    this.measureDataType = measureDataType;
  }

  public String getNoDictionaryDims() {
    return noDictionaryDims;
  }

  public void setNoDictionaryDims(String noDictionaryDims) {
    this.noDictionaryDims = noDictionaryDims;
  }

  /**
   * @return the noDictionaryCount
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

  /**
   * @return partitionId
   */
  public String getPartitionID() {
    return partitionID;
  }

  /**
   * @param partitionID
   */
  public void setPartitionID(String partitionID) {
    this.partitionID = partitionID;
  }

  /**
   * return segmentId
   *
   * @return
   */
  public int getSegmentId() {
    return Integer.parseInt(segmentId);
  }

  /**
   * set segment Id
   *
   * @param segmentId
   */
  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

  /**
   * @param taskNo
   */
  public void setTaskNo(String taskNo) {
    this.taskNo = taskNo;
  }

  /**
   * @return
   */
  public String getTaskNo() {
    return taskNo;
  }

  public String getNoDictionaryDimsMapping() {
    return noDictionaryDimsMapping;
  }

  public void setNoDictionaryDimsMapping(String noDictionaryDimsMapping) {
    this.noDictionaryDimsMapping = noDictionaryDimsMapping;
  }
}