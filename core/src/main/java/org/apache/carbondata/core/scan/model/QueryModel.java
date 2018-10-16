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
public class QueryModel {

  /**
   * this will hold the information about the dictionary dimension
   * which to
   */
  private transient Map<String, Dictionary> columnToDictionaryMapping;
  /**
   * list of projection columns in query
   */
  private QueryProjection projection;
  /**
   * query id
   */
  private String queryId;
  /**
   * filter tree
   */
  private FilterResolverIntf filterExpressionResolverTree;

  /**
   * filter expression tree
   */
  private Expression filterExpression;

  /**
   * table block information in which query will be executed
   */
  private List<TableBlockInfo> tableBlockInfos;
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

  /**
   * Read the data from carbondata file page by page instead of whole blocklet.
   */
  private boolean readPageByPage;

  /**
   * whether it require to output the row id
   */
  private boolean requiredRowId;

  /**
   * whether it is FG with search mode
   */
  private boolean isFG;

  // whether to clear/free unsafe memory or not
  private boolean freeUnsafeMemory = true;

  private boolean preFetchData = true;

  /**
   * It fills the vector directly from decoded column page with out any staging and conversions
   */
  private boolean isDirectVectorFill;

  private QueryModel(CarbonTable carbonTable) {
    tableBlockInfos = new ArrayList<TableBlockInfo>();
    invalidSegmentIds = new ArrayList<>();
    this.table = carbonTable;
    this.queryId = String.valueOf(System.nanoTime());
  }

  public static QueryModel newInstance(CarbonTable carbonTable) {
    return new QueryModel(carbonTable);
  }

  public static void processFilterExpression(FilterProcessVO processVO, Expression filterExpression,
      final boolean[] isFilterDimensions, final boolean[] isFilterMeasures) {
    if (null != filterExpression) {
      if (null != filterExpression.getChildren() && filterExpression.getChildren().size() == 0) {
        if (filterExpression instanceof ConditionalExpression) {
          List<ColumnExpression> listOfCol =
              ((ConditionalExpression) filterExpression).getColumnList();
          for (ColumnExpression expression : listOfCol) {
            setDimAndMsrColumnNode(processVO, expression, isFilterDimensions, isFilterMeasures);
          }
        }
      }
      for (Expression expression : filterExpression.getChildren()) {
        if (expression instanceof ColumnExpression) {
          setDimAndMsrColumnNode(processVO, (ColumnExpression) expression, isFilterDimensions,
              isFilterMeasures);
        } else if (expression instanceof UnknownExpression) {
          UnknownExpression exp = ((UnknownExpression) expression);
          List<ColumnExpression> listOfColExpression = exp.getAllColumnList();
          for (ColumnExpression col : listOfColExpression) {
            setDimAndMsrColumnNode(processVO, col, isFilterDimensions, isFilterMeasures);
          }
        } else {
          processFilterExpression(processVO, expression, isFilterDimensions, isFilterMeasures);
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

  private static void setDimAndMsrColumnNode(FilterProcessVO processVO, ColumnExpression col,
      boolean[] isFilterDimensions, boolean[] isFilterMeasures) {
    CarbonDimension dim;
    CarbonMeasure msr;
    String columnName;
    columnName = col.getColumnName();
    col.reset();
    dim = CarbonUtil
        .findDimension(processVO.getCarbonDimensions(), columnName);
    msr = getCarbonMetadataMeasure(columnName, processVO.getCarbonMeasures());

    if (null != dim) {
      // Dimension Column
      col.setCarbonColumn(dim);
      col.setDimension(dim);
      col.setDimension(true);
      if (null != isFilterDimensions) {
        isFilterDimensions[dim.getOrdinal()] = true;
      }
    } else if (msr != null) {
      col.setCarbonColumn(msr);
      col.setMeasure(msr);
      col.setMeasure(true);
      if (null != isFilterMeasures) {
        isFilterMeasures[msr.getOrdinal()] = true;
      }
    } else {
      // check if this is an implicit dimension
      dim = CarbonUtil
          .findDimension(processVO.getImplicitDimensions(), columnName);
      col.setCarbonColumn(dim);
      col.setDimension(dim);
      col.setDimension(true);
    }
  }

  /**
   * It gets the projection columns
   */
  public CarbonColumn[] getProjectionColumns() {
    CarbonColumn[] carbonColumns =
        new CarbonColumn[getProjectionDimensions().size() + getProjectionMeasures().size()];
    for (ProjectionDimension dimension : getProjectionDimensions()) {
      carbonColumns[dimension.getOrdinal()] = dimension.getDimension();
    }
    for (ProjectionMeasure msr : getProjectionMeasures()) {
      carbonColumns[msr.getOrdinal()] = msr.getMeasure();
    }
    return carbonColumns;
  }

  public void setProjection(QueryProjection projection) {
    this.projection = projection;
  }

  public List<ProjectionDimension> getProjectionDimensions() {
    return projection.getDimensions();
  }

  public List<ProjectionMeasure> getProjectionMeasures() {
    return projection.getMeasures();
  }

  /**
   * @return the queryId
   */
  public String getQueryId() {
    return queryId;
  }

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

  public Expression getFilterExpression() {
    return filterExpression;
  }

  public void setFilterExpression(Expression filterExpression) {
    this.filterExpression = filterExpression;
  }

  /**
   * @return the absoluteTableIdentifier
   */
  public AbsoluteTableIdentifier getAbsoluteTableIdentifier() {
    return table.getAbsoluteTableIdentifier();
  }

  /**
   * @return the table
   */
  public CarbonTable getTable() {
    return table;
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

  public boolean isReadPageByPage() {
    return readPageByPage;
  }

  public void setReadPageByPage(boolean readPageByPage) {
    this.readPageByPage = readPageByPage;
  }

  public boolean isRequiredRowId() {
    return requiredRowId;
  }

  public void setRequiredRowId(boolean requiredRowId) {
    this.requiredRowId = requiredRowId;
  }

  public boolean isFG() {
    return isFG;
  }

  public void setFG(boolean FG) {
    isFG = FG;
  }

  public boolean isPreFetchData() {
    return preFetchData;
  }

  public void setPreFetchData(boolean preFetchData) {
    this.preFetchData = preFetchData;
  }

  public boolean isDirectVectorFill() {
    return isDirectVectorFill;
  }

  public void setDirectVectorFill(boolean directVectorFill) {
    isDirectVectorFill = directVectorFill;
  }

  @Override
  public String toString() {
    return String.format("scan on table %s.%s, %d projection columns with filter (%s)",
        table.getDatabaseName(), table.getTableName(),
        projection.getDimensions().size() + projection.getMeasures().size(),
        filterExpressionResolverTree.getFilterExpression().toString());
  }

  public boolean isFreeUnsafeMemory() {
    return freeUnsafeMemory;
  }

  public void setFreeUnsafeMemory(boolean freeUnsafeMemory) {
    this.freeUnsafeMemory = freeUnsafeMemory;
  }

  public static class FilterProcessVO {

    private List<CarbonDimension> carbonDimensions;

    private List<CarbonMeasure> carbonMeasures;

    private List<CarbonDimension> implicitDimensions;

    public FilterProcessVO(List<CarbonDimension> carbonDimensions,
        List<CarbonMeasure> carbonMeasures, List<CarbonDimension> implicitDimensions) {
      this.carbonDimensions = carbonDimensions;
      this.carbonMeasures = carbonMeasures;
      this.implicitDimensions = implicitDimensions;
    }

    public List<CarbonDimension> getCarbonDimensions() {
      return carbonDimensions;
    }

    public List<CarbonMeasure> getCarbonMeasures() {
      return carbonMeasures;
    }

    public List<CarbonDimension> getImplicitDimensions() {
      return implicitDimensions;
    }
  }
}
