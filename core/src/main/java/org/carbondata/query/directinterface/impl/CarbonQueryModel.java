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

package org.carbondata.query.directinterface.impl;

import java.util.List;
import java.util.Map;

import org.carbondata.core.metadata.CalculatedMeasure;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.query.executer.impl.topn.TopNModel;
import org.carbondata.query.filters.measurefilter.MeasureFilterModel;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;
import org.carbondata.query.queryinterface.query.metadata.DSLTransformation;

/**
 * It is model object for carbon query
 */
public class CarbonQueryModel {
  /**
   * cube
   */
  private Cube cube;

  /**
   * factTableName
   */
  private String factTableName;
  /**
   * queryDims
   */
  private List<CarbonMetadata.Dimension> queryDims;
  /**
   * cube
   */
  private List<Measure> msrs;
  /**
   * cube
   */
  private Map<Measure, MeasureFilterModel[]> msrFilter;
  /**
   * constraints
   */
  private Map<Dimension, CarbonFilterInfo> constraints;

  /**
   * msrFilterAfterTopN
   */
  private Map<Measure, MeasureFilterModel[]> msrFilterAfterTopN;
  /**
   * constraintsAfterTopN
   */
  private Map<Dimension, CarbonFilterInfo> constraintsAfterTopN;

  /**
   * dimSortTypes
   */
  private byte[] dimSortTypes;
  /**
   * topNModel
   */
  private TopNModel topNModel;
  /**
   * queryDimsRows
   */
  private List<CarbonMetadata.Dimension> queryDimsRows;
  /**
   * queryDimsCols
   */
  private List<CarbonMetadata.Dimension> queryDimsCols;

  /**
   * dimSortTypes
   */
  private byte[] globalDimSortTypes;

  /**
   * queryDimsRows
   */
  private List<CarbonMetadata.Dimension> globalQueryDimsRows;
  /**
   * queryDimsCols
   */
  private List<CarbonMetadata.Dimension> globalQueryDimsCols;

  /**
   * queryDims including the dynamic columns
   */
  private List<CarbonMetadata.Dimension> globalQueryDims;
  /**
   * sortModel
   */
  private MeasureSortModel sortModel;

  /**
   * When it set as true then user needs to provide the filters exactly with there parent members.
   */
  private boolean exactLevelsMatch;

  /**
   * Whether pagination required or not
   */
  private boolean paginationRequired;

  /**
   * Row range for pagination.
   */
  private int[] rowRange;

  /**
   * Unique query ID
   */
  private String queryId;

  /**
   * Calculated measures
   */
  private List<CalculatedMeasure> calcMsrs;

  /**
   * Grand total enabled
   */
  private boolean grandTotalForAllRows;

  /**
   * relativefilter
   */
  private boolean relativefilter;

  /**
   * pushTopNInCarbon
   */
  private boolean pushTopNInCarbon;

  /**
   * isAnalyzer
   */
  private boolean isAnalyzer;

  /**
   * isSubTotal
   */
  private boolean isSubTotal;

  /**
   * isPresent.
   */
  private boolean isPresent;
  private List<DSLTransformation> carbonTransformations;

  /**
   * @return the cube
   */
  public Cube getCube() {
    return cube;
  }

  /**
   * @param cube the cube to set
   */
  public void setCube(Cube cube) {
    this.cube = cube;
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
   * @return the queryDims
   */
  public List<CarbonMetadata.Dimension> getQueryDims() {
    return queryDims;
  }

  /**
   * @param queryDims the queryDims to set
   */
  public void setQueryDims(List<CarbonMetadata.Dimension> queryDims) {
    this.queryDims = queryDims;
  }

  /**
   * @return the msrs
   */
  public List<Measure> getMsrs() {
    return msrs;
  }

  /**
   * @param msrs the msrs to set
   */
  public void setMsrs(List<Measure> msrs) {
    this.msrs = msrs;
  }

  /**
   * @return the msrFilter
   */
  public Map<Measure, MeasureFilterModel[]> getMsrFilter() {
    return msrFilter;
  }

  /**
   * @param msrFilter the msrFilter to set
   */
  public void setMsrFilter(Map<Measure, MeasureFilterModel[]> msrFilter) {
    this.msrFilter = msrFilter;
  }

  /**
   * @return the constraints
   */
  public Map<Dimension, CarbonFilterInfo> getConstraints() {
    return constraints;
  }

  /**
   * @param constraints the constraints to set
   */
  public void setConstraints(Map<Dimension, CarbonFilterInfo> constraints) {
    this.constraints = constraints;
  }

  /**
   * @return the dimSortTypes
   */
  public byte[] getDimSortTypes() {
    return dimSortTypes;
  }

  /**
   * @param dimSortTypes the dimSortTypes to set
   */
  public void setDimSortTypes(byte[] dimSortTypes) {
    this.dimSortTypes = dimSortTypes;
  }

  /**
   * @return the topNModel
   */
  public TopNModel getTopNModel() {
    return topNModel;
  }

  /**
   * @param topNModel the topNModel to set
   */
  public void setTopNModel(TopNModel topNModel) {
    this.topNModel = topNModel;
  }

  /**
   * @return the queryDimsRows
   */
  public List<CarbonMetadata.Dimension> getQueryDimsRows() {
    return queryDimsRows;
  }

  /**
   * @param queryDimsRows the queryDimsRows to set
   */
  public void setQueryDimsRows(List<CarbonMetadata.Dimension> queryDimsRows) {
    this.queryDimsRows = queryDimsRows;
  }

  /**
   * @return the queryDimsCols
   */
  public List<CarbonMetadata.Dimension> getQueryDimsCols() {
    return queryDimsCols;
  }

  /**
   * @param queryDimsCols the queryDimsCols to set
   */
  public void setQueryDimsCols(List<CarbonMetadata.Dimension> queryDimsCols) {
    this.queryDimsCols = queryDimsCols;
  }

  /**
   * @return the sortModel
   */
  public MeasureSortModel getSortModel() {
    return sortModel;
  }

  /**
   * @param sortModel the sortModel to set
   */
  public void setSortModel(MeasureSortModel sortModel) {
    this.sortModel = sortModel;
  }

  /**
   * @return the exactLevelsMatch
   */
  public boolean isExactLevelsMatch() {
    return exactLevelsMatch;
  }

  /**
   * @param exactLevelsMatch the exactLevelsMatch to set
   */
  public void setExactLevelsMatch(boolean exactLevelsMatch) {
    this.exactLevelsMatch = exactLevelsMatch;
  }

  /**
   * @return the paginationRequired
   */
  public boolean isPaginationRequired() {
    return paginationRequired;
  }

  /**
   * @param paginationRequired the paginationRequired to set
   */
  public void setPaginationRequired(boolean paginationRequired) {
    this.paginationRequired = paginationRequired;
  }

  /**
   * @return the rowRange
   */
  public int[] getRowRange() {
    return rowRange;
  }

  /**
   * @param rowRange the rowRange to set
   */
  public void setRowRange(int[] rowRange) {
    this.rowRange = rowRange;
  }

  /**
   * @return the queryId
   */
  public String getQueryId() {
    return queryId;
  }

  /**
   * @param queryId the queryId to set
   */
  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  /**
   * @return the calcMsrs
   */
  public List<CalculatedMeasure> getCalcMsrs() {
    return calcMsrs;
  }

  /**
   * @param calcMsrs the calcMsrs to set
   */
  public void setCalcMsrs(List<CalculatedMeasure> calcMsrs) {
    this.calcMsrs = calcMsrs;
  }

  /**
   * @return the msrFilterAfterTopN
   */
  public Map<Measure, MeasureFilterModel[]> getMsrFilterAfterTopN() {
    return msrFilterAfterTopN;
  }

  /**
   * @param msrFilterAfterTopN the msrFilterAfterTopN to set
   */
  public void setMsrFilterAfterTopN(Map<Measure, MeasureFilterModel[]> msrFilterAfterTopN) {
    this.msrFilterAfterTopN = msrFilterAfterTopN;
  }

  /**
   * @return the constraintsAfterTopN
   */
  public Map<Dimension, CarbonFilterInfo> getConstraintsAfterTopN() {
    return constraintsAfterTopN;
  }

  /**
   * @param constraintsAfterTopN the constraintsAfterTopN to set
   */
  public void setConstraintsAfterTopN(Map<Dimension, CarbonFilterInfo> constraintsAfterTopN) {
    this.constraintsAfterTopN = constraintsAfterTopN;
  }

  /**
   * @return the grandTotalForAllRows
   */
  public boolean isGrandTotalForAllRows() {
    return grandTotalForAllRows;
  }

  /**
   * @param grandTotalForAllRows the grandTotalForAllRows to set
   */
  public void setGrandTotalForAllRows(boolean grandTotalForAllRows) {
    this.grandTotalForAllRows = grandTotalForAllRows;
  }

  /**
   * @return the relativefilter
   */
  public boolean isRelativefilter() {
    return relativefilter;
  }

  /**
   * @param relativefilter the relativefilter to set
   */
  public void setRelativefilter(boolean relativefilter) {
    this.relativefilter = relativefilter;
  }

  public void pushTopNToCarbonEngine(boolean pushTopNInCarbon) {
    this.pushTopNInCarbon = pushTopNInCarbon;

  }

  public boolean pushTopN() {
    return pushTopNInCarbon;

  }

  /**
   * @return the isAnalyzer
   */
  public boolean isAnalyzer() {
    return isAnalyzer;
  }

  /**
   * @param isAnalyzer the isAnalyzer to set
   */
  public void setAnalyzer(boolean isAnalyzer) {
    this.isAnalyzer = isAnalyzer;
  }

  public boolean isSubTotal() {
    return isSubTotal;
  }

  public void setSubTotal(boolean isSubTotal) {
    this.isSubTotal = isSubTotal;
  }

  /**
   * @param isPresent
   * @Author s71955
   * @Description : setIsSliceFilterPresent
   */
  public void setIsSliceFilterPresent(boolean isPresent) {
    this.isPresent = isPresent;

  }

  /**
   * @return
   * @Author s71955
   * @Description : isSliceFilterPresent
   */
  public boolean isSliceFilterPresent() {
    return isPresent;

  }

  /**
   * @return the carbonTransformations
   */
  public List<DSLTransformation> getCarbonTransformations() {
    return carbonTransformations;
  }

  /**
   * @param carbonTransformations the carbonTransformations to set
   */
  public void setCarbonTransformations(List<DSLTransformation> carbonTransformations) {
    this.carbonTransformations = carbonTransformations;
  }

  /**
   * @return Returns the globalDimSortTypes.
   */
  public byte[] getGlobalDimSortTypes() {
    return globalDimSortTypes;
  }

  /**
   * @param globalDimSortTypes The globalDimSortTypes to set.
   */
  public void setGlobalDimSortTypes(byte[] globalDimSortTypes) {
    this.globalDimSortTypes = globalDimSortTypes;
  }

  /**
   * @return Returns the globalQueryDimsRows.
   */
  public List<CarbonMetadata.Dimension> getGlobalQueryDimsRows() {
    return globalQueryDimsRows;
  }

  /**
   * @param globalQueryDimsRows The globalQueryDimsRows to set.
   */
  public void setGlobalQueryDimsRows(List<CarbonMetadata.Dimension> globalQueryDimsRows) {
    this.globalQueryDimsRows = globalQueryDimsRows;
  }

  /**
   * @return Returns the globalQueryDimsCols.
   */
  public List<CarbonMetadata.Dimension> getGlobalQueryDimsCols() {
    return globalQueryDimsCols;
  }

  /**
   * @param globalQueryDimsCols The globalQueryDimsCols to set.
   */
  public void setGlobalQueryDimsCols(List<CarbonMetadata.Dimension> globalQueryDimsCols) {
    this.globalQueryDimsCols = globalQueryDimsCols;
  }

  /**
   * @return Returns the globalQueryDims.
   */
  public List<CarbonMetadata.Dimension> getGlobalQueryDims() {
    return globalQueryDims;
  }

  /**
   * @param globalQueryDims The globalQueryDims to set.
   */
  public void setGlobalQueryDims(List<CarbonMetadata.Dimension> globalQueryDims) {
    this.globalQueryDims = globalQueryDims;
  }

}
