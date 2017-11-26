/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.scan.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.UnknownExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverter;

/**
 * Query model which will have all the detail
 * about the query, This will be sent from driver to executor '
 * This will be refereed to executing the query.
 */
public class QueryModel implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = -4674677234007089052L;
  /**
   * this will hold the information about the dictionary dimension
   * which to
   */
  public transient Map<String, Dictionary> columnToDictionaryMapping;
  /**
   * list of dimension selected for in query
   */
  private List<QueryDimension> queryDimension;
  /**
   * list of measure selected in query
   */
  private List<QueryMeasure> queryMeasures;
  /**
   * query id
   */
  private String queryId;
  /**
   * filter tree
   */
  private FilterResolverIntf filterExpressionResolverTree;

  /**
   * table block information in which query will be executed
   */
  private List<TableBlockInfo> tableBlockInfos;
  /**
   * absolute table identifier
   */
  private AbsoluteTableIdentifier absoluteTableIdentifier;
  /**
   * To handle most of the computation in query engines like spark and hive, carbon should give
   * raw detailed records to it.
   */
  private boolean forcedDetailRawQuery;
  /**
   * table on which query will be executed
   * TODO need to remove this ad pass only the path
   * and carbon metadata will load the table from metadata file
   */
  private CarbonTable table;

  private QueryStatisticsRecorder statisticsRecorder;

  private boolean vectorReader;

  private DataTypeConverter converter;

  /**
   * Invalid table blocks, which need to be removed from
   * memory, invalid blocks can be segment which are deleted
   * or compacted
   */
  private List<String> invalidSegmentIds;
  private Map<String, UpdateVO> invalidSegmentBlockIdMap =
      new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  private boolean[] isFilterDimensions;
  private boolean[] isFilterMeasures;

  public QueryModel() {
    tableBlockInfos = new ArrayList<TableBlockInfo>();
    queryDimension = new ArrayList<QueryDimension>();
    queryMeasures = new ArrayList<QueryMeasure>();
    invalidSegmentIds = new ArrayList<>();
  }

  public static QueryModel createModel(AbsoluteTableIdentifier absoluteTableIdentifier,
      CarbonQueryPlan queryPlan, CarbonTable carbonTable, DataTypeConverter converter) {
    QueryModel queryModel = new QueryModel();
    String factTableName = carbonTable.getTableName();
    queryModel.setAbsoluteTableIdentifier(absoluteTableIdentifier);

    fillQueryModel(queryPlan, carbonTable, queryModel, factTableName);

    queryModel.setForcedDetailRawQuery(queryPlan.isRawDetailQuery());
    queryModel.setQueryId(queryPlan.getQueryId());
    queryModel.setConverter(converter);
    return queryModel;
  }

  private static void fillQueryModel(CarbonQueryPlan queryPlan, CarbonTable carbonTable,
      QueryModel queryModel, String factTableName) {
    queryModel.setAbsoluteTableIdentifier(carbonTable.getAbsoluteTableIdentifier());
    queryModel.setQueryDimension(queryPlan.getDimensions());
    queryModel.setQueryMeasures(queryPlan.getMeasures());
    if (null != queryPlan.getFilterExpression()) {
      boolean[] isFilterDimensions = new boolean[carbonTable.getDimensionOrdinalMax()];
      boolean[] isFilterMeasures =
          new boolean[carbonTable.getNumberOfMeasures(carbonTable.getTableName())];
      processFilterExpression(queryPlan.getFilterExpression(),
          carbonTable.getDimensionByTableName(factTableName),
          carbonTable.getMeasureByTableName(factTableName), isFilterDimensions, isFilterMeasures);
      queryModel.setIsFilterDimensions(isFilterDimensions);
      queryModel.setIsFilterMeasures(isFilterMeasures);
    }
    //TODO need to remove this code, and executor will load the table
    // from file metadata
    queryModel.setTable(carbonTable);
  }

  public static void processFilterExpression(Expression filterExpression,
      List<CarbonDimension> dimensions, List<CarbonMeasure> measures,
      final boolean[] isFilterDimensions, final boolean[] isFilterMeasures) {
    if (null != filterExpression) {
      if (null != filterExpression.getChildren() && filterExpression.getChildren().size() == 0) {
        if (filterExpression instanceof ConditionalExpression) {
          List<ColumnExpression> listOfCol =
              ((ConditionalExpression) filterExpression).getColumnList();
          for (ColumnExpression expression : listOfCol) {
            setDimAndMsrColumnNode(dimensions, measures, expression, isFilterDimensions,
                isFilterMeasures);
          }
        }
      }
      for (Expression expression : filterExpression.getChildren()) {
        if (expression instanceof ColumnExpression) {
          setDimAndMsrColumnNode(dimensions, measures, (ColumnExpression) expression,
              isFilterDimensions, isFilterMeasures);
        } else if (expression instanceof UnknownExpression) {
          UnknownExpression exp = ((UnknownExpression) expression);
          List<ColumnExpression> listOfColExpression = exp.getAllColumnList();
          for (ColumnExpression col : listOfColExpression) {
            setDimAndMsrColumnNode(dimensions, measures, col, isFilterDimensions, isFilterMeasures);
          }
        } else {
          processFilterExpression(expression, dimensions, measures, isFilterDimensions,
              isFilterMeasures);
        }
      }
    }

  }

  private static CarbonMeasure getCarbonMetadataMeasure(String name, List<CarbonMeasure> measures) {
    for (CarbonMeasure measure : measures) {
      if (measure.getColName().equalsIgnoreCase(name)) {
        return measure;
      }
    }
    return null;
  }

  private static void setDimAndMsrColumnNode(List<CarbonDimension> dimensions,
      List<CarbonMeasure> measures, ColumnExpression col, boolean[] isFilterDimensions,
      boolean[] isFilterMeasures) {
    CarbonDimension dim;
    CarbonMeasure msr;
    String columnName;
    columnName = col.getColumnName();
    dim = CarbonUtil.findDimension(dimensions, columnName);
    msr = getCarbonMetadataMeasure(columnName, measures);
    col.setDimension(false);
    col.setMeasure(false);

    if (null != dim) {
      // Dimension Column
      col.setCarbonColumn(dim);
      col.setDimension(dim);
      col.setDimension(true);
      if (null != isFilterDimensions) {
        isFilterDimensions[dim.getOrdinal()] = true;
      }
    } else {
      col.setCarbonColumn(msr);
      col.setMeasure(msr);
      col.setMeasure(true);
      if (null != isFilterMeasures) {
        isFilterMeasures[msr.getOrdinal()] = true;
      }
    }
  }

  /**
   * It gets the projection columns
   */
  public CarbonColumn[] getProjectionColumns() {
    CarbonColumn[] carbonColumns =
        new CarbonColumn[getQueryDimension().size() + getQueryMeasures().size()];
    for (QueryDimension dimension : getQueryDimension()) {
      carbonColumns[dimension.getQueryOrder()] = dimension.getDimension();
    }
    for (QueryMeasure msr : getQueryMeasures()) {
      carbonColumns[msr.getQueryOrder()] = msr.getMeasure();
    }
    return carbonColumns;
  }

  /**
   * @return the queryDimension
   */
  public List<QueryDimension> getQueryDimension() {
    return queryDimension;
  }

  /**
   * @param queryDimension the queryDimension to set
   */
  public void setQueryDimension(List<QueryDimension> queryDimension) {
    this.queryDimension = queryDimension;
  }

  /**
   * @return the queryMeasures
   */
  public List<QueryMeasure> getQueryMeasures() {
    return queryMeasures;
  }

  /**
   * @param queryMeasures the queryMeasures to set
   */
  public void setQueryMeasures(List<QueryMeasure> queryMeasures) {
    this.queryMeasures = queryMeasures;
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
   * @return the tableBlockInfos
   */
  public List<TableBlockInfo> getTableBlockInfos() {
    return tableBlockInfos;
  }

  /**
   * @param tableBlockInfos the tableBlockInfos to set
   */
  public void setTableBlockInfos(List<TableBlockInfo> tableBlockInfos) {
    this.tableBlockInfos = tableBlockInfos;
  }

  /**
   * @return the filterEvaluatorTree
   */
  public FilterResolverIntf getFilterExpressionResolverTree() {
    return filterExpressionResolverTree;
  }

  public void setFilterExpressionResolverTree(FilterResolverIntf filterExpressionResolverTree) {
    this.filterExpressionResolverTree = filterExpressionResolverTree;
  }

  /**
   * @return the absoluteTableIdentifier
   */
  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return absoluteTableIdentifier;
  }

  /**
   * @param absoluteTableIdentifier the absoluteTableIdentifier to set
   */
  public void setAbsoluteTableIdentifier(AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
  }

  /**
   * @return the table
   */
  public CarbonTable getTable() {
    return table;
  }

  /**
   * @param table the table to set
   */
  public void setTable(CarbonTable table) {
    this.table = table;
  }

  public boolean isForcedDetailRawQuery() {
    return forcedDetailRawQuery;
  }

  public void setForcedDetailRawQuery(boolean forcedDetailRawQuery) {
    this.forcedDetailRawQuery = forcedDetailRawQuery;
  }

  /**
   * @return
   */
  public Map<String, Dictionary> getColumnToDictionaryMapping() {
    return columnToDictionaryMapping;
  }

  /**
   * @param columnToDictionaryMapping
   */
  public void setColumnToDictionaryMapping(Map<String, Dictionary> columnToDictionaryMapping) {
    this.columnToDictionaryMapping = columnToDictionaryMapping;
  }

  public QueryStatisticsRecorder getStatisticsRecorder() {
    return statisticsRecorder;
  }

  public void setStatisticsRecorder(QueryStatisticsRecorder statisticsRecorder) {
    this.statisticsRecorder = statisticsRecorder;
  }

  public List<String> getInvalidSegmentIds() {
    return invalidSegmentIds;
  }

  public void setInvalidSegmentIds(List<String> invalidSegmentIds) {
    this.invalidSegmentIds = invalidSegmentIds;
  }

  public boolean isVectorReader() {
    return vectorReader;
  }

  public void setVectorReader(boolean vectorReader) {
    this.vectorReader = vectorReader;
  }
  public void setInvalidBlockForSegmentId(List<UpdateVO> invalidSegmentTimestampList) {
    for (UpdateVO anUpdateVO : invalidSegmentTimestampList) {
      this.invalidSegmentBlockIdMap.put(anUpdateVO.getSegmentId(), anUpdateVO);
    }
  }

  public Map<String,UpdateVO>  getInvalidBlockVOForSegmentId() {
    return  invalidSegmentBlockIdMap;
  }

  public DataTypeConverter getConverter() {
    return converter;
  }

  public void setConverter(DataTypeConverter converter) {
    this.converter = converter;
  }

  public boolean[] getIsFilterDimensions() {
    return isFilterDimensions;
  }

  public void setIsFilterDimensions(boolean[] isFilterDimensions) {
    this.isFilterDimensions = isFilterDimensions;
  }

  public boolean[] getIsFilterMeasures() {
    return isFilterMeasures;
  }

  public void setIsFilterMeasures(boolean[] isFilterMeasures) {
    this.isFilterMeasures = isFilterMeasures;
  }
}
