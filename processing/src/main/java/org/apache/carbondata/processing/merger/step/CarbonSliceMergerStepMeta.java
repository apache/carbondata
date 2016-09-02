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

package org.apache.carbondata.processing.merger.step;

import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

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
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

public class CarbonSliceMergerStepMeta extends BaseStepMeta
    implements StepMetaInterface, Cloneable {

  /**
   * for i18n purposes
   */
  private static final Class<?> PKG = CarbonSliceMergerStepMeta.class;

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
   * databaseName
   */
  private String databaseName;

  /**
   * tableName
   */
  private String tableName;

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

  private String levelAnddataTypeString;
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
   * CarbonDataWriterStepMeta constructor to initialize this class
   */
  public CarbonSliceMergerStepMeta() {
    super();
  }

  /**
   * set the default value for all the properties
   */
  @Override public void setDefault() {
    tabelName = "";
    mdkeySize = "";
    measureCount = "";
    heirAndKeySize = "";
    tableName = "";
    databaseName = "";
    groupByEnabled = "";
    aggregatorClassString = "";
    aggregatorString = "";
    factDimLensString = "";
    levelAnddataTypeString = "";
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
    retval.append("    ").append(XMLHandler.addTagValue("TableName", tabelName));
    retval.append("    ").append(XMLHandler.addTagValue("MDKeySize", mdkeySize));
    retval.append("    ").append(XMLHandler.addTagValue("Measurecount", measureCount));
    retval.append("    ").append(XMLHandler.addTagValue("HeirAndKeySize", heirAndKeySize));
    retval.append("    ").append(XMLHandler.addTagValue("tableName", tableName));
    retval.append("    ").append(XMLHandler.addTagValue("databaseName", databaseName));
    retval.append("    ").append(XMLHandler.addTagValue("isGroupByEnabled", groupByEnabled));
    retval.append("    ")
        .append(XMLHandler.addTagValue("aggregatorClassString", aggregatorClassString));
    retval.append("    ").append(XMLHandler.addTagValue("aggregatorString", aggregatorString));
    retval.append("    ").append(XMLHandler.addTagValue("factDimLensString", factDimLensString));
    retval.append("    ")
        .append(XMLHandler.addTagValue("levelAnddataTypeString", levelAnddataTypeString));
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
  @Override public void loadXML(Node stepnode, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleXMLException {
    try {
      databaseName = XMLHandler.getTagValue(stepnode, "databaseName");
      tabelName = XMLHandler.getTagValue(stepnode, "TableName");
      mdkeySize = XMLHandler.getTagValue(stepnode, "MDKeySize");
      measureCount = XMLHandler.getTagValue(stepnode, "Measurecount");
      heirAndKeySize = XMLHandler.getTagValue(stepnode, "HeirAndKeySize");
      tableName = XMLHandler.getTagValue(stepnode, "tableName");
      groupByEnabled = XMLHandler.getTagValue(stepnode, "isGroupByEnabled");
      aggregatorClassString = XMLHandler.getTagValue(stepnode, "aggregatorClassString");
      aggregatorString = XMLHandler.getTagValue(stepnode, "aggregatorString");
      factDimLensString = XMLHandler.getTagValue(stepnode, "factDimLensString");
      levelAnddataTypeString = XMLHandler.getTagValue(stepnode, "levelAnddataTypeString");
      partitionID = XMLHandler.getTagValue(stepnode, "partitionID");
      segmentId = XMLHandler.getTagValue(stepnode, "segmentId");
      taskNo = XMLHandler.getTagValue(stepnode, "taskNo");
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
  @Override public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
      throws KettleException {
    try {
      rep.saveStepAttribute(idTransformation, idStep, "TableName", tabelName); //$NON-NLS-1$
      rep.saveStepAttribute(idTransformation, idStep, "MDKeySize", mdkeySize); //$NON-NLS-1$
      rep.saveStepAttribute(idTransformation, idStep, "Measurecount", measureCount);
      rep.saveStepAttribute(idTransformation, idStep, "HeirAndKeySize",
          heirAndKeySize); //$NON-NLS-1$
      rep.saveStepAttribute(idTransformation, idStep, "tableName", tableName); //$NON-NLS-1$
      rep.saveStepAttribute(idTransformation, idStep, "databaseName", databaseName); //$NON-NLS-1$
      rep.saveStepAttribute(idTransformation, idStep, "isGroupByEnabled", groupByEnabled);
      rep.saveStepAttribute(idTransformation, idStep, "aggregatorClassString",
          aggregatorClassString);
      rep.saveStepAttribute(idTransformation, idStep, "aggregatorString", aggregatorString);
      rep.saveStepAttribute(idTransformation, idStep, "factDimLensString", factDimLensString);
      rep.saveStepAttribute(idTransformation, idStep, "levelAnddataTypeString",
          levelAnddataTypeString);
      rep.saveStepAttribute(idTransformation, idStep, "partitionID", partitionID);
      rep.saveStepAttribute(idTransformation, idStep, "segmentId", segmentId);
      rep.saveStepAttribute(idTransformation, idStep, "taskNo", taskNo);
    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(PKG, "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
              + idStep, e);
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
   * Read the steps information from a Kettle repository
   *
   * @param rep       The repository to read from
   * @param idStep    The step ID
   * @param databases The databases to reference
   * @param counters  The counters to reference
   * @throws KettleException When an unexpected error occurred (database, network, etc)
   */
  @Override public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleException {
    try {
      tabelName = rep.getStepAttributeString(idStep, "TableName");
      mdkeySize = rep.getStepAttributeString(idStep, "MDKeySize");
      measureCount = rep.getStepAttributeString(idStep, "Measurecount");
      heirAndKeySize = rep.getStepAttributeString(idStep, "HeirAndKeySize");
      databaseName = rep.getStepAttributeString(idStep, "databaseName");
      tableName = rep.getStepAttributeString(idStep, "tableName");
      groupByEnabled = rep.getStepAttributeString(idStep, "isGroupByEnabled");
      aggregatorClassString = rep.getStepAttributeString(idStep, "aggregatorClassString");
      aggregatorString = rep.getStepAttributeString(idStep, "aggregatorString");
      factDimLensString = rep.getStepAttributeString(idStep, "factDimLensString");
      levelAnddataTypeString = rep.getStepAttributeString(idStep, "levelAnddataTypeString");
      partitionID = rep.getStepAttributeString(idStep, "partitionID");
      segmentId = rep.getStepAttributeString(idStep, "segmentId");
      taskNo = rep.getStepAttributeString(idStep, "taskNo");
    } catch (Exception exception) {
      throw new KettleException(BaseMessages
          .getString(PKG, "CarbonDataWriterStepMeta.Exception.UnexpectedErrorInReadingStepInfo"),
          exception);
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
  @Override public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface,
      int copyNr, TransMeta transMeta, Trans trans) {
    return new CarbonSliceMergerStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
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
  @Override public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
      StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
      RowMetaInterface info) {

    CheckResult checkResVal;

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      checkResVal =
          new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.",
              stepMeta);
      remarks.add(checkResVal);
    } else {
      checkResVal =
          new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!",
              stepMeta);
      remarks.add(checkResVal);
    }

  }

  /**
   * Get a new instance of the appropriate data class. This data class
   * implements the StepDataInterface. It basically contains the persisting
   * data that needs to live on, even if a worker thread is terminated.
   *
   * @return The appropriate StepDataInterface class.
   */
  @Override public StepDataInterface getStepData() {
    return new CarbonSliceMergerStepData();
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
   * This method will return mdkey size
   *
   * @return mdkey size
   */
  public String getMdkeySize() {
    return mdkeySize;
  }

  /**
   * This method will be used to set the mdkey
   *
   * @param mdkeySize
   */
  public void setMdkeySize(String mdkeySize) {
    this.mdkeySize = mdkeySize;
  }

  /**
   * This method will be used to get the measure count
   *
   * @return measure count
   */
  public String getMeasureCount() {
    return measureCount;
  }

  /**
   * This method will be used to set the measure count
   *
   * @param measureCount
   */
  public void setMeasureCount(String measureCount) {
    this.measureCount = measureCount;
  }

  /**
   * This method will be used to get the heir and its key suze string
   *
   * @return heirAndKeySize
   */
  public String getHeirAndKeySize() {
    return heirAndKeySize;
  }

  /**
   * This method will be used to set the heir and key size string
   *
   * @param heirAndKeySize
   */
  public void setHeirAndKeySize(String heirAndKeySize) {
    this.heirAndKeySize = heirAndKeySize;
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
   * @param factDimLensString1 the factDimLensString to set
   */
  public void setFactDimLensString(String factDimLensString1) {
    this.factDimLensString = factDimLensString1;
  }

  public String getLevelAnddataTypeString() {
    return levelAnddataTypeString;
  }

  public void setLevelAnddataTypeString(String levelAnddataTypeString) {
    this.levelAnddataTypeString = levelAnddataTypeString;
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
   * @return
   */
  public int getSegmentId() {
    return Integer.parseInt(segmentId);
  }

  /**
   * set segment Id
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
}
