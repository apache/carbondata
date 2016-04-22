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

package org.carbondata.query.executer.impl;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.CarbonMetadata.Measure;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.aggregator.impl.CountAggregator;
import org.carbondata.query.aggregator.impl.DistinctCountAggregator;
import org.carbondata.query.aggregator.impl.DistinctStringCountAggregator;
import org.carbondata.query.complex.querytypes.GenericQueryType;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.result.ChunkResult;
import org.carbondata.query.scanner.impl.CarbonKey;
import org.carbondata.query.scanner.impl.CarbonValue;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.util.DataTypeConverter;
import org.carbondata.query.util.QueryExecutorUtility;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class QueryResultPreparator {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(QueryResultPreparator.class.getName());

  private QueryExecuterProperties executerProperties;

  private CarbonQueryExecutorModel queryModel;

  private int currentSliceIndex;

  public QueryResultPreparator(QueryExecuterProperties executerProperties,
      CarbonQueryExecutorModel queryModel) {
    this.executerProperties = executerProperties;
    this.queryModel = queryModel;
  }

  private void updatedWithCurrentUnique(QueryResult result, List<Measure> msrList) {
    int size = msrList.size();
    QueryResult.QueryResultIterator iterator = result.iterator();
    while (iterator.hasNext()) {
      MeasureAggregator[] value = iterator.getValue();
      for (int i = 0; i < size; i++) {
        Measure m = msrList.get(i);

        if (value[executerProperties.measureStartIndex + i].isFirstTime()) {
          value[executerProperties.measureStartIndex + i]
              .setNewValue(executerProperties.uniqueValue[m.getOrdinal()]);
        }
      }
    }
  }

  public ChunkResult prepareQueryOutputResult(QueryResult result) {
    LOGGER.debug(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
        "###########################################------ Started preparing the result");
    if ((null == result || result.size() < 1)) {
      return new ChunkResult();
    }
    Dimension[] dims = queryModel.getDims();
    int dimensionCount = dims.length;
    int size = dimensionCount + executerProperties.aggTypes.length;
    updatedWithCurrentUnique(result, queryModel.getMsrs());
    Object[][] resultData = new Object[result.size()][size];
    QueryResult.QueryResultIterator iterator = result.iterator();
    long[] keyArray = null;
    int currentRow = 0;
    Map<Integer, Integer> ordinalAndResultIndexMap = getNoDictionaryIndexInResult(dims);

    if (!executerProperties.isFunctionQuery && dimensionCount == 0 && size == 0
        && result.size() > 0) {
      return getEmptyChunkResult(result.size());
    }
    queryModel.setComplexDimensionsMap(
        QueryExecutorUtility.getComplexDimensionsMap(executerProperties.dimTables));
    QueryExecutorUtility.getComplexDimensionsKeySize(queryModel.getComplexDimensionsMap(),
        executerProperties.slices.get(currentSliceIndex).getDimensionCardinality());
    Map<String, Integer> complexQueryIndexes = QueryExecutorUtility
        .getComplexQueryIndexes(queryModel.getDims(), executerProperties.dimTables);
    while (iterator.hasNext()) {
      ByteArrayWrapper keyWrapper = iterator.getKey();
      keyArray = executerProperties.globalKeyGenerator
          .getKeyArray(keyWrapper.getMaskedKey(), executerProperties.maskedBytes);

      //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_006
      int index = 0;
      for (int i = 0; i < dimensionCount; i++) {
        if (dims[i].isNoDictionaryDim() && null != keyWrapper.getNoDictionaryValKeyList()) {
          resultData[currentRow][i] = keyWrapper.getNoDictionaryValKeyList()
              .get(ordinalAndResultIndexMap.get(dims[i].getOrdinal()));
        } else {
          if (dims[i].isNoDictionaryDim()) {
            continue;
          }
          GenericQueryType complexType =
              queryModel.getComplexDimensionsMap().get(queryModel.getDims()[i].getColName());
          if (complexType == null) {
            resultData[currentRow][i] = (int) keyArray[executerProperties.hybridStoreModel
                .getMdKeyOrdinal(queryModel.getDims()[i].getOrdinal())];
          } else {
            resultData[currentRow][i] = keyWrapper
                .getComplexTypeData(complexQueryIndexes.get(queryModel.getDims()[i].getColName()));
          }
        }
      }
      //CHECKSTYLE:ON

      MeasureAggregator[] d = iterator.getValue();

      if (executerProperties.isFunctionQuery) {
        if (d[0].toString().contains("Long")) {
          Long sizeOfListL = d[0].getLongValue();
          return getEmptyChunkResult(sizeOfListL.intValue());
        } else if (d[0].toString().contains("Decimal")) {
          BigDecimal sizeOfListD = d[0].getBigDecimalValue();
          return getEmptyChunkResult(sizeOfListD.intValue());
        } else {
          Double sizeOfList = d[0].getDoubleValue();
          return getEmptyChunkResult(sizeOfList.intValue());
        }
      }

      //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_001
      for (int i = 0; i < executerProperties.aggTypes.length; i++) {
        resultData[currentRow][dimensionCount + i] = d[i];
      }
      //CHECKSTYLE:ON

      currentRow++;
    }
    if (resultData.length > 0) {
      resultData = encodeToRows(resultData);
    }
    return getResult(queryModel, resultData);
  }

  private Map<Integer, Integer> getNoDictionaryIndexInResult(Dimension[] dims) {
    Map<Integer, Integer> ordinalAndResultIndexMap = new HashMap<>();
    Dimension[] dimTables = executerProperties.dimTables;
    int index = 0;
    for (int i = 0; i < dimTables.length; i++) {
      if (dimTables[i].isNoDictionaryDim()) {
        for (int j = 0; j < dims.length; j++) {
          if (dims[j].equals(dimTables[i])) {
            ordinalAndResultIndexMap.put(dims[j].getOrdinal(), index++);
          }
        }
      }
    }
    return ordinalAndResultIndexMap;
  }

  private ChunkResult getEmptyChunkResult(int size) {
    List<CarbonKey> keys = new ArrayList<CarbonKey>(size);
    List<CarbonValue> values = new ArrayList<CarbonValue>(size);
    Object[] row = new Object[1];
    for (int i = 0; i < size; i++)

    {
      values.add(new CarbonValue(new MeasureAggregator[0]));
      keys.add(new CarbonKey(row));
    }
    ChunkResult chunkResult = new ChunkResult();
    chunkResult.setKeys(keys);
    chunkResult.setValues(values);
    return chunkResult;
  }

  private ChunkResult getResult(CarbonQueryExecutorModel queryModel, Object[][] surrogateResult) {
    Member member = null;
    int dimensionCount = queryModel.getDims().length;
    int msrCount = executerProperties.aggTypes.length;
    List<CarbonKey> keys = new ArrayList<CarbonKey>(20);
    List<CarbonValue> values = new ArrayList<CarbonValue>(20);
    if (!executerProperties.isCountMsrExistInCurrTable && executerProperties.countMsrIndex > -1) {
      msrCount--;
    }

    Object[][] resultDataA = null;
    if (executerProperties.isFunctionQuery) {
      msrCount = 1;
      resultDataA = new Object[dimensionCount + msrCount][msrCount];
    } else {
      resultDataA = new Object[dimensionCount + msrCount][surrogateResult[0].length];
    }

    String memString = null;
    Object[] row = null;
    int recordSize = dimensionCount;
    //        if(queryModel.isDetailQuery())
    //        {
    recordSize += msrCount;
    //        }

    for (int columnIndex = 0; columnIndex < resultDataA[0].length; columnIndex++) {
      row = new Object[recordSize];
      for (int i = 0; i < dimensionCount; i++) {
        boolean isComplexType = false;
        Object complexData = null;
        GenericQueryType complexType =
            queryModel.getComplexDimensionsMap().get(queryModel.getDims()[i].getColName());
        if (surrogateResult[i][columnIndex] instanceof byte[] && complexType == null) {
          member = new Member((byte[]) surrogateResult[i][columnIndex]);
        } else if (executerProperties.sortDimIndexes[i] == 1) {
          member = QueryExecutorUtility.getActualMemberBySortedKey(queryModel.getDims()[i],
              (Integer) surrogateResult[i][columnIndex], executerProperties.slices);
        } else {
          if (complexType == null) {
            member = QueryExecutorUtility.getMemberBySurrogateKey(queryModel.getDims()[i],
                (Integer) surrogateResult[i][columnIndex], executerProperties.slices);
          } else {

            isComplexType = true;
            complexData = complexType
                .getDataBasedOnDataTypeFromSurrogates(executerProperties.slices,
                    ByteBuffer.wrap((byte[]) surrogateResult[i][columnIndex]),
                    executerProperties.dimTables);
          }
        }

        if (!isComplexType) {
          if (queryModel.getDims()[i].isDirectDictionary()) {
            DirectDictionaryGenerator directDictionaryGenerator =
                DirectDictionaryKeyGeneratorFactory
                    .getDirectDictionaryGenerator(queryModel.getDims()[i].getDataType());
            row[queryModel.getDims()[i].getQueryOrder()] = directDictionaryGenerator
                .getValueFromSurrogate((int) surrogateResult[i][columnIndex]);
          } else {
            memString = member.toString();
            row[queryModel.getDims()[i].getQueryOrder()] = DataTypeConverter.getDataBasedOnDataType(
                memString.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL) ? null : memString,
                queryModel.getDims()[i].getDataType());
          }
        } else {
          row[queryModel.getDims()[i].getQueryOrder()] = complexData;
        }
      }
      MeasureAggregator[] msrAgg = new MeasureAggregator[executerProperties.aggTypes.length];
      fillMeasureValueForAggGroupByQuery(queryModel, surrogateResult, dimensionCount, columnIndex,
          msrAgg);
      fillDimensionAggValue(queryModel, surrogateResult, dimensionCount, columnIndex, msrAgg);
      if (!queryModel.isDetailQuery()) {
        for (int i = 0; i < queryModel.getMsrs().size(); i++) {
          row[queryModel.getMsrs().get(i).getQueryOrder()] =
              msrAgg[executerProperties.measureStartIndex + i].get();
        }
        int index = 0;
        for (int i = 0; i < queryModel.getDimensionAggInfo().size(); i++) {
          DimensionAggregatorInfo dimensionAggregatorInfo = queryModel.getDimensionAggInfo().get(i);
          for (int j = 0; j < dimensionAggregatorInfo.getOrderList().size(); j++) {
            row[dimensionAggregatorInfo.getOrderList().get(j)] = msrAgg[index++].get();
          }
        }
        //                values.add(new CarbonValue(msrAgg));
        for (int i = 0; i < queryModel.getExpressions().size(); i++) {
          row[queryModel.getExpressions().get(i).getQueryOrder()] =
              ((MeasureAggregator) surrogateResult[dimensionCount
                  + executerProperties.aggExpressionStartIndex + i][columnIndex]).get();
        }
      } else {
        //                for(int i = 0;i < msrCount;i++)
        //                {
        //                    row[dimensionCount + i] = msrAgg[i].getValue();
        //                }

        for (int i = 0; i < queryModel.getMsrs().size(); i++) {
          if (msrAgg[executerProperties.measureStartIndex + i].isFirstTime() && (
              executerProperties.aggTypes[executerProperties.measureStartIndex + i]
                  .equals(CarbonCommonConstants.DISTINCT_COUNT) || executerProperties.aggTypes[
                  executerProperties.measureStartIndex + i].equals(CarbonCommonConstants.COUNT))) {
            row[queryModel.getMsrs().get(i).getQueryOrder()] = 0.0;
          } else if (msrAgg[executerProperties.measureStartIndex + i].isFirstTime()) {
            row[queryModel.getMsrs().get(i).getQueryOrder()] = null;
          } else {
            Object msrVal;
            switch (executerProperties.dataTypes[executerProperties.measureStartIndex + i]) {
              case LONG:
                msrVal = msrAgg[executerProperties.measureStartIndex + i].getLongValue();
                break;
              case DECIMAL:
                msrVal = msrAgg[executerProperties.measureStartIndex + i].getBigDecimalValue();
                break;
              default:
                msrVal = msrAgg[executerProperties.measureStartIndex + i].getDoubleValue();
            }
            row[queryModel.getMsrs().get(i).getQueryOrder()] = DataTypeConverter
                .getMeasureDataBasedOnDataType(msrVal == null ? null : msrVal,
                    queryModel.getMsrs().get(i).getDataType());
          }
        }
        int index = 0;
        for (int i = 0; i < queryModel.getDimensionAggInfo().size(); i++) {
          DimensionAggregatorInfo dimensionAggregatorInfo = queryModel.getDimensionAggInfo().get(i);
          for (int j = 0; j < dimensionAggregatorInfo.getOrderList().size(); j++) {
            switch (queryModel.getDims()[i].getDataType()) {
              case LONG:
                row[dimensionAggregatorInfo.getOrderList().get(j)] = msrAgg[index++].getLongValue();
                break;
              case DECIMAL:
                row[dimensionAggregatorInfo.getOrderList().get(j)] =
                    msrAgg[index++].getBigDecimalValue();
                break;
              default:
                row[dimensionAggregatorInfo.getOrderList().get(j)] =
                    msrAgg[index++].getDoubleValue();
            }
          }
        }
      }
      values.add(new CarbonValue(new MeasureAggregator[0]));
      keys.add(new CarbonKey(row));
    }
    LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
        "###########################################------ Total Number of records"
            + resultDataA[0].length);
    ChunkResult chunkResult = new ChunkResult();
    chunkResult.setKeys(keys);
    chunkResult.setValues(values);
    return chunkResult;
  }

  private Object[][] encodeToRows(Object[][] data) {
    if (data.length == 0) {
      return data;
    }
    Object[][] rData = new Object[data[0].length][data.length];
    int len = data.length;
    for (int i = 0; i < rData.length; i++) {
      for (int j = 0; j < len; j++) {//CHECKSTYLE:OFF    Approval No:Approval-297
        rData[i][j] = data[j][i];
      }//CHECKSTYLE:ON
    }
    return rData;
  }

  private void fillDimensionAggValue(CarbonQueryExecutorModel queryModel,
      Object[][] surrogateResult, int dimensionCount, int columnIndex, MeasureAggregator[] v) {
    Iterator<DimensionAggregatorInfo> dimAggInfoIterator =
        queryModel.getDimensionAggInfo().iterator();
    DimensionAggregatorInfo dimensionAggregatorInfo = null;
    List<Measure> measures = queryModel.getCube().getMeasures(queryModel.getFactTable());
    List<String> partitionColumns = queryModel.getPartitionColumns();
    Dimension mappedDim = null;
    int rowIndex = -1;
    int index = 0;
    while (dimAggInfoIterator.hasNext()) {
      dimensionAggregatorInfo = dimAggInfoIterator.next();
      for (Measure msr : measures) {
        if (msr.getColName().equals(dimensionAggregatorInfo.getDim().getColName())) {
          mappedDim = dimensionAggregatorInfo.getDim();
          break;
        }
      }
      for (int j = 0; j < dimensionAggregatorInfo.getAggList().size(); j++) {
        ++rowIndex;
        if (!dimensionAggregatorInfo.getAggList().get(j)
            .equals(CarbonCommonConstants.DISTINCT_COUNT)) {
          v[index++] =
              ((MeasureAggregator) surrogateResult[dimensionCount + rowIndex][columnIndex]);
        } else if (partitionColumns.size() == 1 && partitionColumns
            .contains(dimensionAggregatorInfo.getColumnName()) && dimensionAggregatorInfo
            .getAggList().get(j).equals(CarbonCommonConstants.DISTINCT_COUNT)) {
          double value =
              ((MeasureAggregator) surrogateResult[dimensionCount + rowIndex][columnIndex])
                  .getDoubleValue();

          MeasureAggregator countAggregator = new CountAggregator();
          countAggregator.setNewValue(value);
          v[index++] = countAggregator;
        } else {
          if (surrogateResult[dimensionCount
              + rowIndex][columnIndex] instanceof DistinctCountAggregator) {

            Iterator<Integer> iterator =
                ((DistinctCountAggregator) surrogateResult[dimensionCount + rowIndex][columnIndex])
                    .getBitMap().iterator();
            if (null != mappedDim) {
              MeasureAggregator distinctCountAggregator = new DistinctCountAggregator(0);
              while (iterator.hasNext()) {
                distinctCountAggregator.agg(getGlobalSurrogates(mappedDim, iterator.next()));
              }
              v[index++] = distinctCountAggregator;
              currentSliceIndex = 0;
            } else {
              MeasureAggregator distinctCountAggregatorObjct = new DistinctStringCountAggregator();
              while (iterator.hasNext()) {
                String member = QueryExecutorUtility
                    .getMemberBySurrogateKey(dimensionAggregatorInfo.getDim(),
                        (Integer) iterator.next(), executerProperties.slices).toString();
                if (!member.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                  distinctCountAggregatorObjct.agg(member);
                }
              }
              v[index++] = distinctCountAggregatorObjct;
            }
          } else {
            v[index++] =
                ((MeasureAggregator) surrogateResult[dimensionCount + rowIndex][columnIndex]);
          }
        }
      }
    }
  }

  private void fillMeasureValueForAggGroupByQuery(CarbonQueryExecutorModel queryModel,
      Object[][] surrogateResult, int dimensionCount, int columnIndex, MeasureAggregator[] v) {
    int msrCount = queryModel.getMsrs().size();
    for (int i = 0; i < msrCount; i++) {
      v[executerProperties.measureStartIndex + i] =
          ((MeasureAggregator) surrogateResult[dimensionCount + executerProperties.measureStartIndex
              + i][columnIndex]);
    }
    int rowIndex = -1;
    for (int i = 0; i < msrCount; i++) {
      rowIndex++;
      if (queryModel.getMsrs().get(i).isDistinctQuery()) {
        Dimension mappedDim = null;
        List<Dimension> dimensions =
            queryModel.getCube().getDimensions(queryModel.getCube().getFactTableName());
        for (Dimension dim : dimensions) {
          if (dim.getColName().equals(queryModel.getMsrs().get(i).getColName())) {
            mappedDim = dim;
            break;
          }
        }

        boolean isPartitionColumn = false;
        if (queryModel.getPartitionColumns().size() == 1) {
          if (null != mappedDim) {
            if (queryModel.getPartitionColumns().get(0).equals(mappedDim.getColName())) {
              isPartitionColumn = true;
            }
          }
        }
        if (null != mappedDim) {
          Iterator<Integer> iterator = ((DistinctCountAggregator) v[i]).getBitMap().iterator();
          if (queryModel.isAggTable()) {
            if (!isPartitionColumn) {
              MeasureAggregator distinctCountAggregatorObjct = new DistinctStringCountAggregator();
              while (iterator.hasNext()) {
                String member = QueryExecutorUtility
                    .getMemberBySurrogateKey(mappedDim, (Integer) iterator.next(),
                        executerProperties.slices).toString();
                if (!member.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
                  distinctCountAggregatorObjct.agg(member);
                }
              }
              v[executerProperties.measureStartIndex + i] = distinctCountAggregatorObjct;
            } else {
              double value = ((DistinctCountAggregator) surrogateResult[dimensionCount
                  + rowIndex][columnIndex]).getDoubleValue();
              MeasureAggregator countAggregator = new CountAggregator();
              countAggregator.setNewValue(value);
              v[executerProperties.measureStartIndex + i] = countAggregator;
            }
          } else {
            MeasureAggregator distinctCountAggregator = new DistinctCountAggregator(0);

            // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_006
            int minValue =
                (int) executerProperties.msrMinValue[executerProperties.measureStartIndex + i];
            // CHECKSTYLE:ON

            while (iterator.hasNext()) {
              distinctCountAggregator
                  .agg(getGlobalSurrogates(mappedDim, iterator.next() - minValue));
            }
            v[executerProperties.measureStartIndex + i] = distinctCountAggregator;
            currentSliceIndex = 0;
          }
        } else {
          v[executerProperties.measureStartIndex + i] = v[executerProperties.measureStartIndex + i];
        }
      }
    }
  }

  private int getGlobalSurrogates(Dimension columnName, int surrogate) {
    int globalSurrogates = -1;
    for (int i = currentSliceIndex; i < executerProperties.slices.size(); i++) {
      globalSurrogates = executerProperties.slices.get(i).getMemberCache(
          columnName.getTableName() + '_' + columnName.getColName() + '_' + columnName.getDimName()
              + '_' + columnName.getHierName()).getGlobalSurrogateKey(surrogate);
      if (-1 != globalSurrogates) {
        currentSliceIndex = i;
        return globalSurrogates;
      }
    }
    return -1;
  }
}