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

package org.apache.carbondata.core.scan.collector.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.complextypes.StructQueryType;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.commons.lang3.ArrayUtils;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class DictionaryBasedResultCollector extends AbstractScannedResultCollector {

  protected ProjectionDimension[] queryDimensions;

  protected ProjectionMeasure[] queryMeasures;

  private DirectDictionaryGenerator[] directDictionaryGenerators;

  /**
   * query order
   */
  protected int[] order;

  private int[] actualIndexInSurrogateKey;

  private boolean[] implicitColumnArray;

  private boolean[] complexDataTypeArray;

  int dictionaryColumnIndex;
  int noDictionaryColumnIndex;
  int complexTypeColumnIndex;


  boolean isDimensionExists;

  private int[] surrogateResult;
  private byte[][] noDictionaryKeys;
  private byte[][] complexTypeKeyArray;

  protected Map<Integer, GenericQueryType> complexDimensionInfoMap;

  /**
   * Field of this Map is the parent Column and associated child columns.
   * Final Projection should be a merged list consist of only parents.
   */
  private Map<Integer, List<Integer>> parentToChildColumnsMap = new HashMap<>();

  /**
   * Map to hold the complex parent ordinal of each query dimension
   */
  private List<Integer> queryDimensionToComplexParentOrdinal = new ArrayList<>();

  /**
   * Fields of this Map of Parent Ordinal with the List is the Child Column Dimension and
   * the corresponding data buffer of that column.
   */
  private Map<Integer, Map<CarbonDimension, ByteBuffer>> mergedComplexDimensionIndex =
      new HashMap<>();

  private boolean readOnlyDelta;

  public DictionaryBasedResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    queryDimensions = executionInfo.getProjectionDimensions();
    queryMeasures = executionInfo.getProjectionMeasures();
    initDimensionAndMeasureIndexesForFillingData();
    isDimensionExists = queryDimensions.length > 0;
    this.complexDimensionInfoMap = executionInfo.getComplexDimensionInfoMap();
    this.readOnlyDelta = executionInfo.isReadOnlyDelta();
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   */
  @Override
  public List<Object[]> collectResultInRow(BlockletScannedResult scannedResult, int batchSize) {
    // scan the record and add to list
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    int rowCounter = 0;
    boolean isStructQueryType = false;
    for (Object obj : scannedResult.complexParentIndexToQueryMap.values()) {
      if (obj instanceof StructQueryType) {
        //if any one of the map elements contains struct,need to shift rows if contains null.
        isStructQueryType = true;
        break;
      }
    }
    boolean[] isComplexChildColumn = null;
    if (isStructQueryType) {
      // need to identify complex child columns for shifting rows if contains null
      isComplexChildColumn = new boolean[queryDimensions.length + queryMeasures.length];
      for (ProjectionDimension dimension : queryDimensions) {
        if (null != dimension.getDimension().getComplexParentDimension()) {
          isComplexChildColumn[dimension.getOrdinal()] = true;
        }
      }
    }
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      scannedResult.incrementCounter();
      if (readOnlyDelta) {
        if (!scannedResult.containsDeletedRow(scannedResult.getCurrentRowId()) &&
                scannedResult.getCurrentDeleteDeltaVo() != null) {
          continue;
        }
      } else {
        if (scannedResult.containsDeletedRow(scannedResult.getCurrentRowId())) {
          continue;
        }
      }
      Object[] row = new Object[queryDimensions.length + queryMeasures.length];
      if (isDimensionExists) {
        surrogateResult = scannedResult.getDictionaryKeyIntegerArray();
        noDictionaryKeys = scannedResult.getNoDictionaryKeyArray();
        complexTypeKeyArray = scannedResult.getComplexTypeKeyArray();
        dictionaryColumnIndex = 0;
        noDictionaryColumnIndex = 0;
        complexTypeColumnIndex = 0;

        // get the complex columns data of this row
        fillComplexColumnDataBufferForThisRow();
        for (int i = 0; i < queryDimensions.length; i++) {
          fillDimensionData(scannedResult, surrogateResult, noDictionaryKeys, complexTypeKeyArray,
              complexDimensionInfoMap, row, i, queryDimensions[i].getDimension().getOrdinal());
        }
      }
      fillMeasureData(scannedResult, row);
      if (isStructQueryType) {
        shiftNullForStruct(row, isComplexChildColumn);
      }
      listBasedResult.add(row);
      rowCounter++;
    }
    return listBasedResult;
  }

  /**
   * shift the complex column null to the end
   *
   * @param row
   * @param isComplexChildColumn
   */
  private void shiftNullForStruct(Object[] row, boolean[] isComplexChildColumn) {
    int count = 0;
    // If a : <b,c> and d : <e,f> are two struct and if a.b,a.c,d.e is given in the
    // projection list,then object array will contain a,null,d as result, because for a.b,
    // a will be filled and for a.c null will be placed.
    // Instead place null in the end of object array and send a,d,null as result.
    for (int j = 0; j < row.length; j++) {
      if (null == row[j] && !isComplexChildColumn[j]) {
        // if it is a primitive column, don't shift the null to the end.
        row[count++] = null;
      } else if (null != row[j]) {
        row[count++] = row[j];
      }
    }
    // fill the skipped content
    while (count < row.length) row[count++] = null;
  }

  private void fillComplexColumnDataBufferForThisRow() {
    mergedComplexDimensionIndex.clear();
    int noDictionaryComplexColumnIndex = 0;
    int complexTypeComplexColumnIndex = 0;
    for (int i = 0; i < queryDimensions.length; i++) {
      int complexParentOrdinal = queryDimensionToComplexParentOrdinal.get(i);
      if (complexParentOrdinal != -1) {
        Map<CarbonDimension, ByteBuffer> childColumnByteBuffer;
        // Add the parent and the child ordinal to the parentToChildColumnsMap
        if (mergedComplexDimensionIndex.get(complexParentOrdinal) == null) {
          childColumnByteBuffer = new HashMap<>();
        } else {
          childColumnByteBuffer = mergedComplexDimensionIndex.get(complexParentOrdinal);
        }

        // send the byte buffer for the complex columns. Currently expected columns for
        // complex types are
        // a) Complex Columns
        // b) No Dictionary columns.
        // TODO have to fill out for dictionary columns. Once the support for push down in
        // complex dictionary columns comes.
        ByteBuffer buffer;
        if (queryDimensions[i].getDimension().getDataType() != DataTypes.DATE) {
          if (implicitColumnArray[i]) {
            throw new RuntimeException("Not Supported Column Type");
          } else if (complexDataTypeArray[i]) {
            buffer = ByteBuffer.wrap(complexTypeKeyArray[complexTypeComplexColumnIndex++]);
          } else {
            buffer = ByteBuffer.wrap(noDictionaryKeys[noDictionaryComplexColumnIndex++]);
          }
        } else if (queryDimensions[i].getDimension().getDataType() == DataTypes.DATE) {
          throw new RuntimeException("Direct Dictionary Column Type Not Supported Yet.");
        } else if (complexDataTypeArray[i]) {
          buffer = ByteBuffer.wrap(complexTypeKeyArray[complexTypeComplexColumnIndex++]);
        } else {
          throw new RuntimeException("Not Supported Column Type");
        }

        childColumnByteBuffer
            .put(queryDimensions[i].getDimension(), buffer);
        mergedComplexDimensionIndex.put(complexParentOrdinal, childColumnByteBuffer);
      } else if (!queryDimensions[i].getDimension().isComplex()) {
        // If Dimension is not a Complex Column, then increment index for noDictionaryComplexColumn
        noDictionaryComplexColumnIndex++;
      }
    }
  }

  /**
   * fill the data of dimension columns into row
   *
   * @param scannedResult
   * @param surrogateResult
   * @param noDictionaryKeys
   * @param complexTypeKeyArray
   * @param complexDimensionInfoMap
   * @param row: row data
   * @param i: dimension columns index
   * @param actualOrdinal: the actual ordinal of dimension columns in segment
   *
   */
  void fillDimensionData(BlockletScannedResult scannedResult, int[] surrogateResult,
      byte[][] noDictionaryKeys, byte[][] complexTypeKeyArray,
      Map<Integer, GenericQueryType> complexDimensionInfoMap, Object[] row, int i,
      int actualOrdinal) {
    if (queryDimensions[i].getDimension().getDataType() != DataTypes.DATE) {
      if (implicitColumnArray[i]) {
        if (CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID
            .equals(queryDimensions[i].getColumnName())) {
          row[order[i]] = DataTypeUtil.getDataBasedOnDataType(
              scannedResult.getBlockletId() + CarbonCommonConstants.FILE_SEPARATOR + scannedResult
                  .getCurrentPageCounter() + CarbonCommonConstants.FILE_SEPARATOR + scannedResult
                  .getCurrentRowId(), DataTypes.STRING);
        } else {
          row[order[i]] =
              DataTypeUtil.getDataBasedOnDataType(scannedResult.getBlockletId(), DataTypes.STRING);
        }
      } else if (complexDataTypeArray[i]) {
        // Complex Type With No Dictionary Encoding.
        if (queryDimensionToComplexParentOrdinal.get(i) != -1) {
          fillRow(complexDimensionInfoMap, row, i,
              ByteBuffer.wrap(complexTypeKeyArray[complexTypeColumnIndex++]));
        } else {
          row[order[i]] =
              complexDimensionInfoMap.get(actualOrdinal).getDataBasedOnDataType(
                  ByteBuffer.wrap(complexTypeKeyArray[complexTypeColumnIndex++]));
        }
      } else {
        if (queryDimensionToComplexParentOrdinal.get(i) != -1) {
          // When the parent Ordinal is not -1 then this is a predicate is being pushed down
          // for complex column.
          fillRow(complexDimensionInfoMap, row, i,
              ByteBuffer.wrap(noDictionaryKeys[noDictionaryColumnIndex++]));
        } else {
          row[order[i]] = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(
              noDictionaryKeys[noDictionaryColumnIndex++],
              queryDimensions[i].getDimension().getDataType());
        }
      }
    } else if (queryDimensions[i].getDimension().getDataType() == DataTypes.DATE) {
      if (directDictionaryGenerators[i] != null) {
        row[order[i]] = directDictionaryGenerators[i].getValueFromSurrogate(
            surrogateResult[actualIndexInSurrogateKey[dictionaryColumnIndex++]]);
      }
    } else if (complexDataTypeArray[i]) {
      row[order[i]] = complexDimensionInfoMap.get(actualOrdinal)
          .getDataBasedOnDataType(ByteBuffer.wrap(complexTypeKeyArray[complexTypeColumnIndex++]));
      dictionaryColumnIndex++;
    } else {
      row[order[i]] = surrogateResult[actualIndexInSurrogateKey[dictionaryColumnIndex++]];
    }
  }

  private void fillRow(Map<Integer, GenericQueryType> complexDimensionInfoMap, Object[] row, int i,
      ByteBuffer wrap) {
    if (parentToChildColumnsMap.get(queryDimensionToComplexParentOrdinal.get(i)).size() > 1) {
      fillRowForComplexColumn(complexDimensionInfoMap, row, i);
    } else {
      row[order[i]] = complexDimensionInfoMap.get(queryDimensionToComplexParentOrdinal.get(i))
          .getDataBasedOnColumn(wrap, queryDimensions[i].getDimension().getComplexParentDimension(),
              queryDimensions[i].getDimension());
    }
  }

  private void fillRowForComplexColumn(Map<Integer, GenericQueryType> complexDimensionInfoMap,
      Object[] row, int i) {
    // When multiple columns are then the first child elements is only going to make
    // parent Object Array. For all other cases it should be null.
    // For e.g. a : <b,c,d>. here as a is the parent column and b, c, d are child columns
    // during traversal when we encounter the first element in list i.e. column 'b'
    // a will be completely filled. In case when column 'c' and 'd' encountered then
    // only place null in the output.
    int complexParentOrdinal = queryDimensionToComplexParentOrdinal.get(i);
    List<Integer> childColumns = parentToChildColumnsMap.get(complexParentOrdinal);
    if (childColumns.get(0).equals(queryDimensions[i].getDimension().getOrdinal())) {
      // Fill out Parent Column.
      row[order[i]] = complexDimensionInfoMap.get(complexParentOrdinal).getDataBasedOnColumnList(
          mergedComplexDimensionIndex.get(queryDimensions[i].getParentDimension().getOrdinal()),
          queryDimensions[i].getParentDimension());
    } else {
      row[order[i]] = null;
    }
  }

  void fillMeasureData(BlockletScannedResult scannedResult, Object[] row) {
    if (measureInfo.getMeasureDataTypes().length > 0) {
      Object[] msrValues = new Object[measureInfo.getMeasureDataTypes().length];
      fillMeasureData(msrValues, 0, scannedResult);
      for (int i = 0; i < msrValues.length; i++) {
        row[order[i + queryDimensions.length]] = msrValues[i];
      }
    }
  }

  void initDimensionAndMeasureIndexesForFillingData() {
    List<Integer> dictionaryIndexes = new ArrayList<Integer>();
    for (ProjectionDimension queryDimension : queryDimensions) {
      if (queryDimension.getDimension().getDataType() == DataTypes.DATE) {
        dictionaryIndexes.add(queryDimension.getDimension().getOrdinal());
      }
    }
    int[] primitive =
        ArrayUtils.toPrimitive(dictionaryIndexes.toArray(new Integer[dictionaryIndexes.size()]));
    Arrays.sort(primitive);
    actualIndexInSurrogateKey = new int[dictionaryIndexes.size()];
    int index = 0;

    implicitColumnArray = CarbonUtil.getImplicitColumnArray(queryDimensions);
    complexDataTypeArray = CarbonUtil.getComplexDataTypeArray(queryDimensions);

    parentToChildColumnsMap.clear();
    queryDimensionToComplexParentOrdinal.clear();
    for (ProjectionDimension queryDimension : queryDimensions) {
      if (queryDimension.getDimension().getDataType() == DataTypes.DATE) {
        actualIndexInSurrogateKey[index++] =
            Arrays.binarySearch(primitive, queryDimension.getDimension().getOrdinal());
      }
      if (null != queryDimension.getDimension().getComplexParentDimension()) {
        // Add the parent and the child ordinal to the parentToChildColumnsMap
        int complexParentOrdinal =
            queryDimension.getDimension().getComplexParentDimension().getOrdinal();
        queryDimensionToComplexParentOrdinal.add(complexParentOrdinal);
        if (parentToChildColumnsMap.get(complexParentOrdinal) == null) {
          // Add the parent and child ordinal in the map
          List<Integer> childOrdinals = new ArrayList<>();
          childOrdinals.add(queryDimension.getDimension().getOrdinal());
          parentToChildColumnsMap.put(complexParentOrdinal, childOrdinals);

        } else {
          List<Integer> childOrdinals = parentToChildColumnsMap.get(complexParentOrdinal);
          childOrdinals.add(queryDimension.getDimension().getOrdinal());
          parentToChildColumnsMap.put(complexParentOrdinal, childOrdinals);
        }
      } else {
        queryDimensionToComplexParentOrdinal.add(-1);
      }
    }

    order = new int[queryDimensions.length + queryMeasures.length];
    for (int i = 0; i < queryDimensions.length; i++) {
      order[i] = queryDimensions[i].getOrdinal();
    }
    for (int i = 0; i < queryMeasures.length; i++) {
      order[i + queryDimensions.length] = queryMeasures[i].getOrdinal();
    }
    directDictionaryGenerators = new DirectDictionaryGenerator[queryDimensions.length];
    for (int i = 0; i < queryDimensions.length; i++) {
      directDictionaryGenerators[i] = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(queryDimensions[i].getDimension().getDataType());
    }
  }
}
