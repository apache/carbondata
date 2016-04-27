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

package org.carbondata.query.executer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.query.aggregator.CustomCarbonAggregateExpression;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.directinterface.impl.MeasureSortModel;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.filters.measurefilter.GroupMeasureFilterModel;
import org.carbondata.query.holders.CarbonResultHolder;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;
import org.carbondata.query.scope.QueryScopeObject;

/**
 * Its a model object for CarbonExecutor interface
 */
public class CarbonQueryExecutorModel implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 2237659331132556737L;
  /**
   * Cube
   */
  private Cube cube;
  /**
   * hIterator
   */
  private CarbonResultHolder hIterator;
  /**
   * factTable
   */
  private String factTable;
  /**
   * dims
   */
  private Dimension[] dims;

  private List<String> listValidSliceNumbers;
  /**
   * msrs
   */
  private List<Measure> msrs;
  /**
   * constraints
   */
  private Map<Dimension, CarbonFilterInfo> constraints;

  /**
   * msrFilterModels
   */
  private List<GroupMeasureFilterModel> msrFilterModels;

  /**
   * constraints
   */
  private Map<Dimension, CarbonFilterInfo> constraintsAfterTopN;

  /**
   * constraints
   */
  private Map<String, GenericQueryType> complexDimensionsMap;

  /**
   * msrFilterModels
   */
  private List<GroupMeasureFilterModel> msrFilterModelsTopN;
  /**
   * sortOrder
   */
  private byte[] sortOrder;
  /**
   * properties
   */
  private boolean properties;

  /**
   * sortModel
   */
  private MeasureSortModel sortModel;

  /**
   * filterInHierGroups
   */
  private boolean filterInHierGroups;

  /**
   * Row limit
   */
  private int rowLimit;

  /**
   * Whether requires only name column
   */
  private boolean onlyNameColumnReq;

  /**
   * Is Agg table
   */
  private boolean isAggTable;

  /**
   * In case if filter type is Range and function is "IN", will be set
   * isRangeFilter
   */
  private boolean isRangeFilter;

  /**
   * actualQueryMsrs
   */
  private Dimension[] actualQueryDims;

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
   * calcMeasures
   */
  private List<Measure> calcMeasures;

  /**
   * actual msrs
   */
  private List<Measure> actualMsrs;

  /**
   * actualCalMeasure
   */
  private List<Measure> actualCalMeasure;

  /**
   * analyzerDims
   */
  private Dimension[] analyzerDims;

  /**
   * countFunction
   */
  private boolean countFunction;

  /**
   * actualDimsRows
   */
  private Dimension[] actualDimsRows;

  /**
   * actualDimsCols
   */
  private Dimension[] actualDimsCols;

  /**
   * Use non visual total true
   */
  private boolean isUseNonVisualTotal;

  /**
   * isComputePredicatePresent
   */
  private boolean isComputePredicatePresent;
  /**
   * sliceFilterPresent.
   */
  private boolean sliceFilterPresent;

  /**
   * isSegmentCallWilFilterPresent
   */
  private boolean isSegmentCallWithFilterPresent;
  private boolean detailQuery;

  private Dimension msrFilterDimension;

  private boolean sparkExecution;

  private int limit;

  private Expression filterExpression;

  private String outLocation;

  private List<DimensionAggregatorInfo> dimensionAggInfo;

  private LoadMetadataDetails[] loadMetadataDetails;

  private List<CustomCarbonAggregateExpression> expressions =
      new ArrayList<CustomCarbonAggregateExpression>();

  /**
   * isCountStarQuery
   */
  private boolean isCountStarQuery;
  private List<String> listOfValidUpdatedSlices;

  /**
   * List of partition columns
   */
  private List<String> partitionColumns;

  /**
   * Partition id
   *
   * @return
   */
  private String partitionId;

  private Dimension[] sortedDimensions;
  /**
   * List of all the segments
   */
  private List<String> listOfAllLoadFolder;
  /**
   * Instance of query scope object holding the segment cache and map of segment name as key and
   * modification time as value.
   */
  private QueryScopeObject queryScopeObject;

  public String getPartitionId() {
    return this.partitionId;
  }

  public void setPartitionId(String partitionId) {
    this.partitionId = partitionId;
  }

  public List<String> getListValidSliceNumbers() {
    return listValidSliceNumbers;
  }

  public void setListValidSliceNumbers(List<String> listValidSliceNumbers) {
    this.listValidSliceNumbers = listValidSliceNumbers;
  }

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
   * @return the hIterator
   */
  public CarbonResultHolder gethIterator() {
    return hIterator;
  }

  /**
   * @param hIterator the hIterator to set
   */
  public void sethIterator(final CarbonResultHolder hIterator) {
    this.hIterator = hIterator;
  }

  /**
   * @return the factTable
   */
  public String getFactTable() {
    return factTable;
  }

  /**
   * @param factTable the factTable to set
   */
  public void setFactTable(final String factTable) {
    this.factTable = factTable;
  }

  /**
   * @return the dims
   */
  public Dimension[] getDims() {
    return dims;
  }

  /**
   * @param dims the dims to set
   */
  public void setDims(final Dimension[] dims) {
    this.dims = dims;
  }

  public Map<String, GenericQueryType> getComplexDimensionsMap() {
    return complexDimensionsMap;
  }

  public void setComplexDimensionsMap(Map<String, GenericQueryType> complexDimensionsMap) {
    this.complexDimensionsMap = complexDimensionsMap;
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
  public void setMsrs(final List<Measure> msrs) {
    this.msrs = msrs;
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
  public void setConstraints(final Map<Dimension, CarbonFilterInfo> constraints) {
    this.constraints = constraints;
  }

  /**
   * @return the sortOrder
   */
  public byte[] getSortOrder() {
    return sortOrder;
  }

  /**
   * @param sortOrder the sortOrder to set
   */
  public void setSortOrder(final byte[] sortOrder) {
    this.sortOrder = sortOrder;
  }

  public Dimension[] getSortedDimensions() {
    return sortedDimensions;
  }

  /**
   * @param sortedDimns
   */
  public void setSortedDimensions(final Dimension[] sortedDimns) {
    this.sortedDimensions = sortedDimns;
  }

  /**
   * @return the properties
   */
  public boolean isProperties() {
    return properties;
  }

  /**
   * @param properties the properties to set
   */
  public void setProperties(final boolean properties) {
    this.properties = properties;
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
  public void setSortModel(final MeasureSortModel sortModel) {
    this.sortModel = sortModel;
  }

  /**
   * @return the filterInHierGroups
   */
  public boolean isFilterInHierGroups() {
    return filterInHierGroups;
  }

  /**
   * @param filterInHierGroups the filterInHierGroups to set
   */
  public void setFilterInHierGroups(final boolean filterInHierGroups) {
    this.filterInHierGroups = filterInHierGroups;
  }

  /**
   * @return the rowLimit
   */
  public int getRowLimit() {
    return rowLimit;
  }

  /**
   * @param rowLimit the rowLimit to set
   */
  public void setRowLimit(final int rowLimit) {
    this.rowLimit = rowLimit;
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
  public void setQueryId(final String queryId) {
    this.queryId = queryId;
  }

  /**
   * @return the paginationRequired
   */
  public boolean isPaginationRequired() {
    return paginationRequired;
  }

  //TODO SIMIAN

  /**
   * @param paginationRequired the paginationRequired to set
   */
  public void setPaginationRequired(final boolean paginationRequired) {
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
  public void setRowRange(final int[] rowRange) {
    this.rowRange = rowRange;
  }

  /**
   * @return the onlyNameColumnReq
   */
  public boolean isOnlyNameColumnReq() {
    return onlyNameColumnReq;
  }

  /**
   * @param onlyNameColumnReq the onlyNameColumnReq to set
   */
  public void setOnlyNameColumnReq(final boolean onlyNameColumnReq) {
    this.onlyNameColumnReq = onlyNameColumnReq;
  }

  /**
   * @return the isAggTable
   */
  public boolean isAggTable() {
    return isAggTable;
  }

  /**
   * @param isAggTable the isAggTable to set
   */
  public void setAggTable(final boolean isAggTable) {
    this.isAggTable = isAggTable;
  }

  /**
   * @return the actualQueryDims
   */
  public Dimension[] getActualQueryDims() {
    return actualQueryDims;
  }

  /**
   * @param actualQueryDims the actualQueryDims to set
   */
  public void setActualQueryDims(final Dimension[] actualQueryDims) {
    this.actualQueryDims = actualQueryDims;
  }

  /**
   * boolean
   *
   * @return
   */
  public boolean isRangeFilter() {
    return isRangeFilter;
  }

  /**
   * void
   *
   * @param isRangeFilter
   */
  public void setRangeFilter(final boolean isRangeFilter) {
    this.isRangeFilter = isRangeFilter;
  }

  /**
   * @return the calcMeasures
   */
  public List<Measure> getCalcMeasures() {
    return calcMeasures;
  }

  /**
   * @param calcMeasures the calcMeasures to set
   */
  public void setCalcMeasures(final List<Measure> calcMeasures) {
    this.calcMeasures = calcMeasures;
  }

  /**
   * @return the msrFilterModels
   */
  public List<GroupMeasureFilterModel> getMsrFilterModels() {
    return msrFilterModels;
  }

  /**
   * @param msrFilterModels the msrFilterModels to set
   */
  public void setMsrFilterModels(final List<GroupMeasureFilterModel> msrFilterModels) {
    this.msrFilterModels = msrFilterModels;
  }

  /**
   * @return the actualMsrs
   */
  public List<Measure> getActualMsrs() {
    return actualMsrs;
  }

  /**
   * @param actualMsrs the actualMsrs to set
   */
  public void setActualMsrs(final List<Measure> actualMsrs) {
    this.actualMsrs = actualMsrs;
  }

  /**
   * @return the actualCalMeasure
   */
  public List<Measure> getActualCalMeasure() {
    return actualCalMeasure;
  }

  /**
   * @param actualCalMeasure the actualCalMeasure to set
   */
  public void setActualCalMeasure(final List<Measure> actualCalMeasure) {
    this.actualCalMeasure = actualCalMeasure;
  }

  /**
   * @return the countFunction
   */
  public boolean isCountFunction() {
    return countFunction;
  }

  /**
   * @param countFunction the countFunction to set
   */
  public void setCountFunction(final boolean countFunction) {
    this.countFunction = countFunction;
  }

  /**
   * @return the analyzerDims
   */
  public Dimension[] getAnalyzerDims() {
    return analyzerDims;
  }

  /**
   * @param analyzerDims the analyzerDims to set
   */
  public void setAnalyzerDims(final Dimension[] analyzerDims) {
    this.analyzerDims = analyzerDims;
  }

  /**
   * @return the actualDimsRows
   */
  public Dimension[] getActualDimsRows() {
    return actualDimsRows;
  }

  /**
   * @param actualDimsRows the actualDimsRows to set
   */
  public void setActualDimsRows(final Dimension[] actualDimsRows) {
    this.actualDimsRows = actualDimsRows;
  }

  /**
   * @return the actualDimsCols
   */
  public Dimension[] getActualDimsCols() {
    return actualDimsCols;
  }

  /**
   * @param actualDimsCols the actualDimsCols to set
   */
  public void setActualDimsCols(final Dimension[] actualDimsCols) {
    this.actualDimsCols = actualDimsCols;
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
  public void setConstraintsAfterTopN(final Map<Dimension, CarbonFilterInfo> constraintsAfterTopN) {
    this.constraintsAfterTopN = constraintsAfterTopN;
  }

  /**
   * @return the msrFilterModelsTopN
   */
  public List<GroupMeasureFilterModel> getMsrFilterModelsTopN() {
    return msrFilterModelsTopN;
  }

  /**
   * @param msrFilterModelsTopN the msrFilterModelsTopN to set
   */
  public void setMsrFilterModelsTopN(final List<GroupMeasureFilterModel> msrFilterModelsTopN) {
    this.msrFilterModelsTopN = msrFilterModelsTopN;
  }

  public boolean isUseNonVisualTotal() {
    return isUseNonVisualTotal;
  }

  public void setUseNonVisualTotal(final boolean isUseNonVisualTotal) {
    this.isUseNonVisualTotal = isUseNonVisualTotal;
  }

  public boolean isComputePredicatePresent() {
    return isComputePredicatePresent;
  }

  public void setComputePredicatePresent(final boolean isComputePredicatePresent) {
    this.isComputePredicatePresent = isComputePredicatePresent;
  }

  /**
   * @param sliceFilterPresent
   * @Author s71955
   * @Description : setIsSliceFilterPresent
   */
  public void setIsSliceFilterPresent(final boolean sliceFilterPresent) {
    this.sliceFilterPresent = sliceFilterPresent;

  }

  /**
   * @return
   * @Author s71955
   * @Description : isSliceFilterPresent
   */
  public boolean isSliceFilterPresent() {
    return sliceFilterPresent;
  }

  /**
   * @return the isSegmentCallWilFilterPresent
   */
  public boolean isSegmentCallWithFilterPresent() {
    return isSegmentCallWithFilterPresent;
  }

  /**
   * @param isSegmentCallWilFilterPresent the isSegmentCallWilFilterPresent to set
   */
  public void setSegmentCallWithFilterPresent(final boolean isSegmentCallWilFilterPresent) {
    this.isSegmentCallWithFilterPresent = isSegmentCallWilFilterPresent;
  }

  /**
   * @return the detailQuery
   */
  public boolean isDetailQuery() {
    return detailQuery;
  }

  /**
   * @param detailQuery the detailQuery to set
   */
  public void setDetailQuery(final boolean detailQuery) {
    this.detailQuery = detailQuery;
  }

  /**
   * @return the msrFilterDimension
   */
  public Dimension getMsrFilterDimension() {
    return msrFilterDimension;
  }

  /**
   * @param msrFilterDimension the msrFilterDimension to set
   */
  public void setMsrFilterDimension(final Dimension msrFilterDimension) {
    this.msrFilterDimension = msrFilterDimension;
  }

  /**
   * @return the sparkExecution
   */
  public boolean isSparkExecution() {
    return sparkExecution;
  }

  /**
   * @param sparkExecution the sparkExecution to set
   */
  public void setSparkExecution(final boolean sparkExecution) {
    this.sparkExecution = sparkExecution;
  }

  /**
   * @return the limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @param limit the limit to set
   */
  public void setLimit(final int limit) {
    this.limit = limit;
  }

  public Expression getFilterExpression() {
    return filterExpression;
  }

  public void setFilterExpression(final Expression filterExpression) {
    this.filterExpression = filterExpression;
  }

  public String getOutLocation() {
    return outLocation;
  }

  public void setOutLocation(final String outLocation) {
    this.outLocation = outLocation;
  }

  public List<DimensionAggregatorInfo> getDimensionAggInfo() {
    return dimensionAggInfo;
  }

  public void setDimensionAggInfo(List<DimensionAggregatorInfo> dimensionAggInfo) {
    this.dimensionAggInfo = dimensionAggInfo;
  }

  public boolean isCountStarQuery() {
    return isCountStarQuery;
  }

  public void setCountStarQuery(boolean isCountStarQuery) {
    this.isCountStarQuery = isCountStarQuery;
  }

  /**
   * setListValidUpdatedSlice.
   *
   * @param listOfValidUpdatedSlices
   */
  public void setListValidUpdatedSlice(List<String> listOfValidUpdatedSlices) {
    this.listOfValidUpdatedSlices = listOfValidUpdatedSlices;

  }

  /**
   * getListOfValidUpdatedSlices.
   *
   * @return
   */
  public List<String> getListOfValidUpdatedSlices() {
    return listOfValidUpdatedSlices;
  }

  public List<CustomCarbonAggregateExpression> getExpressions() {
    return expressions;
  }

  public void addExpression(CustomCarbonAggregateExpression expression) {
    this.expressions.add(expression);
  }

  /**
   * @return Returns the partitionColumns.
   */
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  /**
   * @param partitionColumns The partitionColumns to set.
   */
  public void setPartitionColumns(List<String> partitionColumns) {
    this.partitionColumns = partitionColumns;
  }

  /**
   * @return Returns the loadMetadataDetails.
   */
  public LoadMetadataDetails[] getLoadMetadataDetails() {
    return loadMetadataDetails;
  }

  /**
   * @param loadMetadataDetails The loadMetadataDetails to set.
   */
  public void setLoadMetadataDetails(LoadMetadataDetails[] loadMetadataDetails) {
    this.loadMetadataDetails = loadMetadataDetails;
  }

  /**
   * return's the queryScopeObject
   */
  public QueryScopeObject getQueryScopeObject() {
    return queryScopeObject;
  }

  /**
   * Set the queryScopeObject
   */
  public void setQueryScopeObject(QueryScopeObject queryScopeObject) {
    this.queryScopeObject = queryScopeObject;
  }

  /**
   * Returns list of all load folders
   */
  public List<String> getListOfAllLoadFolder() {
    return listOfAllLoadFolder;
  }

  /**
   * Setter to set the List Of All load Folders
   */
  public void setListOfAllLoadFolder(List<String> listOfAllLoadFolder) {
    this.listOfAllLoadFolder = listOfAllLoadFolder;
  }
}
