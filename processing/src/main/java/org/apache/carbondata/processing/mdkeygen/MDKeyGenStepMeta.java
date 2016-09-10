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

package org.apache.carbondata.processing.mdkeygen;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.processing.datatypes.ArrayDataType;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.datatypes.PrimitiveDataType;
import org.apache.carbondata.processing.datatypes.StructDataType;
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

public class MDKeyGenStepMeta extends BaseStepMeta implements StepMetaInterface {
  /**
   * for i18n purposes
   */
  private static Class<?> pkg = MDKeyGenStepMeta.class;

  /**
   * tableName
   */
  private String tableName;

  /**
   * numberOfCores
   */
  private String numberOfCores;

  /**
   * databaseName
   */
  private String databaseName;

  /**
   * aggregateLevels
   */
  private String aggregateLevels;

  /**
   * measureCount
   */
  private String measureCount;

  /**
   * dimensionCount
   */
  private String dimensionCount;

  /**
   * complexDimsCount
   */
  private String complexDimsCount;

  /**
   * ComplexTypeString
   */
  private String complexTypeString;

  private Map<String, GenericDataType> complexTypes;

  /**
   * It is column groups in below format
   * 0,1~2~3,4,5,6~7~8,9
   * groups are
   * ,-> all ordinal with different group id
   * ~-> all ordinal with same group id
   */
  private String columnGroupsString;
  private String noDictionaryDims;

  /**
   * noDictionaryCount
   */
  private int noDictionaryCount;

  private String measureDataType;
  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;
  /**
   * new load start time
   */
  private String factTimeStamp;
  /**
   * partitionID
   */
  private String partitionID;
  /**
   * Id of the load folder
   */
  private String segmentId;
  /**
   * To determine the column whether is dictionary or not.
   */
  private String noDictionaryDimsMapping;
  /**
   * To determine the column whether use inverted index or not.
   */
  private String isUseInvertedIndex;

  /**
   * Constructor
   */
  public MDKeyGenStepMeta() {
    super();
  }

  @Override public void setDefault() {
    tableName = "";
    numberOfCores = "";
    aggregateLevels = "";
    tableName = "";
    databaseName = "";
    columnGroupsString = "";
    noDictionaryDims = "";
    measureDataType = "";
    taskNo = "";
    factTimeStamp = "";
    partitionID = "";
    segmentId = "";
    noDictionaryDimsMapping = "";
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer(150);

    retval.append("    ").append(XMLHandler.addTagValue("TableName", tableName));
    retval.append("    ").append(XMLHandler.addTagValue("AggregateLevels", aggregateLevels));
    retval.append("    ").append(XMLHandler.addTagValue("NumberOfCores", numberOfCores));
    retval.append("    ").append(XMLHandler.addTagValue("tableName", tableName));
    retval.append("    ").append(XMLHandler.addTagValue("databaseName", databaseName));
    retval.append("    ").append(XMLHandler.addTagValue("noDictionaryDims", noDictionaryDims));
    retval.append("    ").append(XMLHandler.addTagValue("measureCount", measureCount));
    retval.append("    ").append(XMLHandler.addTagValue("dimensionsStoreType", columnGroupsString));
    retval.append("    ").append(XMLHandler.addTagValue("dimensionCount", dimensionCount));
    retval.append("    ").append(XMLHandler.addTagValue("complexDimsCount", complexDimsCount));
    retval.append("    ").append(XMLHandler.addTagValue("complexTypeString", complexTypeString));
    retval.append("    ").append(XMLHandler.addTagValue("measureDataType", measureDataType));
    retval.append("    ").append(XMLHandler.addTagValue("taskNo", taskNo));
    retval.append("    ").append(XMLHandler.addTagValue("factTimeStamp", factTimeStamp));
    retval.append("    ").append(XMLHandler.addTagValue("factTimeStamp", factTimeStamp));
    retval.append("    ").append(XMLHandler.addTagValue("partitionID", partitionID));
    retval.append("    ").append(XMLHandler.addTagValue("isUseInvertedIndex", isUseInvertedIndex));
    retval.append("    ").append(XMLHandler.addTagValue("segmentId", segmentId));
    retval.append("    ")
        .append(XMLHandler.addTagValue("noDictionaryDimsMapping", noDictionaryDimsMapping));
    return retval.toString();
  }

  @Override
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleXMLException {
    try {
      tableName = XMLHandler.getTagValue(stepnode, "TableName");
      aggregateLevels = XMLHandler.getTagValue(stepnode, "AggregateLevels");
      numberOfCores = XMLHandler.getTagValue(stepnode, "NumberOfCores");
      databaseName = XMLHandler.getTagValue(stepnode, "databaseName");
      tableName = XMLHandler.getTagValue(stepnode, "tableName");
      noDictionaryDims = XMLHandler.getTagValue(stepnode, "noDictionaryDims");
      measureCount = XMLHandler.getTagValue(stepnode, "measureCount");
      columnGroupsString = XMLHandler.getTagValue(stepnode, "dimensionsStoreType");
      dimensionCount = XMLHandler.getTagValue(stepnode, "dimensionCount");
      complexDimsCount = XMLHandler.getTagValue(stepnode, "complexDimsCount");
      complexTypeString = XMLHandler.getTagValue(stepnode, "complexTypeString");
      measureDataType = XMLHandler.getTagValue(stepnode, "measureDataType");
      taskNo = XMLHandler.getTagValue(stepnode, "taskNo");
      factTimeStamp = XMLHandler.getTagValue(stepnode, "factTimeStamp");
      partitionID = XMLHandler.getTagValue(stepnode, "partitionID");
      isUseInvertedIndex = XMLHandler.getTagValue(stepnode, "isUseInvertedIndex");
      segmentId = XMLHandler.getTagValue(stepnode, "segmentId");
      noDictionaryDimsMapping = XMLHandler.getTagValue(stepnode, "noDictionaryDimsMapping");
    } catch (Exception e) {
      throw new KettleXMLException("Unable to read step info from XML node", e);
    }
  }

  @Override public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep)
      throws KettleException {
    try {
      rep.saveStepAttribute(idTransformation, idStep, "TableName", tableName);
      rep.saveStepAttribute(idTransformation, idStep, "AggregateLevels", aggregateLevels);
      rep.saveStepAttribute(idTransformation, idStep, "NumberOfCores", numberOfCores);
      rep.saveStepAttribute(idTransformation, idStep, "databaseName", databaseName);
      rep.saveStepAttribute(idTransformation, idStep, "tableName", tableName);
      rep.saveStepAttribute(idTransformation, idStep, "noDictionaryDims", noDictionaryDims);
      rep.saveStepAttribute(idTransformation, idStep, "measureCount", measureCount);
      rep.saveStepAttribute(idTransformation, idStep, "dimensionsStoreType", columnGroupsString);
      rep.saveStepAttribute(idTransformation, idStep, "dimensionCount", dimensionCount);
      rep.saveStepAttribute(idTransformation, idStep, "complexDimsCount", complexDimsCount);
      rep.saveStepAttribute(idTransformation, idStep, "complexTypeString", complexTypeString);
      rep.saveStepAttribute(idTransformation, idStep, "measureDataType", measureDataType);
      rep.saveStepAttribute(idTransformation, idStep, "taskNo", taskNo);
      rep.saveStepAttribute(idTransformation, idStep, "factTimeStamp", factTimeStamp);
      rep.saveStepAttribute(idTransformation, idStep, "partitionID", partitionID);
      rep.saveStepAttribute(idTransformation, idStep, "isUseInvertedIndex", isUseInvertedIndex);
      rep.saveStepAttribute(idTransformation, idStep, "segmentId", segmentId);
      rep.saveStepAttribute(idTransformation, idStep, "noDictionaryDimsMapping",
          noDictionaryDimsMapping);
    } catch (Exception e) {
      throw new KettleException(
          BaseMessages.getString(pkg, "TemplateStep.Exception.UnableToSaveStepInfoToRepository")
              + idStep, e);
    }

  }

  @Override public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleException {
    try {
      tableName = rep.getStepAttributeString(idStep, "TableName");
      aggregateLevels = rep.getStepAttributeString(idStep, "AggregateLevels");
      numberOfCores = rep.getStepAttributeString(idStep, "NumberOfCores");
      databaseName = rep.getStepAttributeString(idStep, "databaseName");
      tableName = rep.getStepAttributeString(idStep, "tableName");
      noDictionaryDims = rep.getStepAttributeString(idStep, "noDictionaryDims");
      measureCount = rep.getStepAttributeString(idStep, "measureCount");
      columnGroupsString = rep.getStepAttributeString(idStep, "dimensionsStoreType");
      dimensionCount = rep.getStepAttributeString(idStep, "dimensionCount");
      complexDimsCount = rep.getStepAttributeString(idStep, "complexDimsCount");
      complexTypeString = rep.getStepAttributeString(idStep, "complexTypeString");
      measureDataType = rep.getStepAttributeString(idStep, "measureDataType");
      taskNo = rep.getStepAttributeString(idStep, "taskNo");
      factTimeStamp = rep.getStepAttributeString(idStep, "factTimeStamp");
      partitionID = rep.getStepAttributeString(idStep, "partitionID");
      isUseInvertedIndex = rep.getStepAttributeString(idStep, "isUseInvertedIndex");
      segmentId = rep.getStepAttributeString(idStep, "segmentId");
      noDictionaryDimsMapping = rep.getStepAttributeString(idStep, "noDictionaryDimsMapping");
    } catch (Exception e) {
      throw new KettleException(BaseMessages
          .getString(pkg, "CarbonMDKeyStepMeta.Exception.UnexpectedErrorInReadingStepInfo"), e);
    }
  }

  @Override
  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans) {
    return new MDKeyGenStep(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  @Override
  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info) {
    CarbonDataProcessorUtil.checkResult(remarks, stepMeta, input);
  }

  @Override public StepDataInterface getStepData() {
    return new MDKeyGenStepData();
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getAggregateLevels() {
    return aggregateLevels;
  }

  public void setAggregateLevels(String aggregateLevels) {
    this.aggregateLevels = aggregateLevels;
  }

  public Map<String, GenericDataType> getComplexTypes() {
    return complexTypes;
  }

  public void setComplexTypes(Map<String, GenericDataType> complexTypes) {
    this.complexTypes = complexTypes;
  }

  public String getNumberOfCores() {
    return numberOfCores;
  }

  public void setNumberOfCores(String numberOfCores) {
    this.numberOfCores = numberOfCores;
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
   * @return the dimensionCount
   */
  public int getDimensionCount() {
    return Integer.parseInt(dimensionCount);
  }

  /**
   * @param dimensionCount the dimensionCount to set
   */
  public void setDimensionCount(String dimensionCount) {
    this.dimensionCount = dimensionCount;
  }

  /**
   * @return the complexDimsCount
   */
  public int getComplexDimsCount() {
    return Integer.parseInt(complexDimsCount);
  }

  /**
   * @param complexDimsCount the complexDimsCount to set
   */
  public void setComplexDimsCount(String complexDimsCount) {
    this.complexDimsCount = complexDimsCount;
  }

  /**
   * @return the complexTypeString
   */
  public int getComplexTypeString() {
    return Integer.parseInt(complexTypeString);
  }

  /**
   * @param complexTypeString the complexTypeString to set
   */
  public void setComplexTypeString(String complexTypeString) {
    this.complexTypeString = complexTypeString;
  }

  /**
   * @return
   */
  public String getNoDictionaryDims() {
    return noDictionaryDims;
  }

  /**
   * @param noDictionaryDims
   */
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

  public String getColumnGroupsString() {
    return this.columnGroupsString;
  }

  public void setColumnGroupsString(String columnGroups) {
    this.columnGroupsString = columnGroups;

  }

  public void initialize() {
    complexTypes = getComplexTypesMap(complexTypeString);
  }

  private Map<String, GenericDataType> getComplexTypesMap(String complexTypeString) {
    if (null == complexTypeString) {
      return new LinkedHashMap<>();
    }
    Map<String, GenericDataType> complexTypesMap = new LinkedHashMap<String, GenericDataType>();
    String[] hierarchies = complexTypeString.split(CarbonCommonConstants.SEMICOLON_SPC_CHARACTER);
    for (int i = 0; i < hierarchies.length; i++) {
      String[] levels = hierarchies[i].split(CarbonCommonConstants.HASH_SPC_CHARACTER);
      String[] levelInfo = levels[0].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
      GenericDataType g = levelInfo[1].equals(CarbonCommonConstants.ARRAY) ?
          new ArrayDataType(levelInfo[0], "", levelInfo[3]) :
          new StructDataType(levelInfo[0], "", levelInfo[3]);
      complexTypesMap.put(levelInfo[0], g);
      for (int j = 1; j < levels.length; j++) {
        levelInfo = levels[j].split(CarbonCommonConstants.COLON_SPC_CHARACTER);
        switch (levelInfo[1]) {
          case CarbonCommonConstants.ARRAY:
            g.addChildren(new ArrayDataType(levelInfo[0], levelInfo[2], levelInfo[3]));
            break;
          case CarbonCommonConstants.STRUCT:
            g.addChildren(new StructDataType(levelInfo[0], levelInfo[2], levelInfo[3]));
            break;
          default:
            g.addChildren(new PrimitiveDataType(levelInfo[0], levelInfo[2], levelInfo[3],
                Integer.parseInt(levelInfo[4])));
        }
      }
    }
    return complexTypesMap;
  }

  public String getMeasureDataType() {
    return measureDataType;
  }

  public void setMeasureDataType(String measureDataType) {
    this.measureDataType = measureDataType;
  }

  /**
   * @return
   */
  public int getTaskNo() {
    return Integer.parseInt(taskNo);
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
  public String getFactTimeStamp() {
    return factTimeStamp;
  }

  /**
   * @param factTimeStamp
   */
  public void setFactTimeStamp(String factTimeStamp) {
    this.factTimeStamp = factTimeStamp;
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
   * @return the noDictionaryDimsMapping
   */
  public String getNoDictionaryDimsMapping() {
    return noDictionaryDimsMapping;
  }

  /**
   * @param noDictionaryDimsMapping the noDictionaryDimsMapping to set
   */
  public void setNoDictionaryDimsMapping(String noDictionaryDimsMapping) {
    this.noDictionaryDimsMapping = noDictionaryDimsMapping;
  }
  /**
   * @return isUseInvertedIndex
   */
  public String getIsUseInvertedIndex() {
    return isUseInvertedIndex;
  }

  /**
   * @param isUseInvertedIndex the bool array whether use inverted index to set
   */
  public void setIsUseInvertedIndex(String isUseInvertedIndex) {
    this.isUseInvertedIndex = isUseInvertedIndex;
  }
}
