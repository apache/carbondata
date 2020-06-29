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

package org.apache.carbondata.core.scan.executor.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.complextypes.ArrayQueryType;
import org.apache.carbondata.core.scan.complextypes.MapQueryType;
import org.apache.carbondata.core.scan.complextypes.PrimitiveQueryType;
import org.apache.carbondata.core.scan.complextypes.StructQueryType;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Utility class for query execution
 */
public class QueryUtil {

  /**
   * Below method will be used to get the dimension block index in file based
   * on query dimension
   *
   * @param queryDimensions                query dimension
   * @param dimensionOrdinalToChunkMapping mapping of dimension block in file to query dimension
   * @return block index of file
   */
  public static int[] getDimensionChunkIndexes(List<ProjectionDimension> queryDimensions,
      Map<Integer, Integer> dimensionOrdinalToChunkMapping,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2720
      Set<CarbonDimension> filterDimensions,
      Set<Integer> allProjectionListDimensionIndexes) {
    // using set as in row group columns will point to same block
    Set<Integer> dimensionChunkIndex = new HashSet<Integer>();
    Set<Integer> filterDimensionOrdinal = getFilterDimensionOrdinal(filterDimensions);
    int chunkIndex = 0;
    for (int i = 0; i < queryDimensions.size(); i++) {
      if (queryDimensions.get(i).getDimension().hasEncoding(Encoding.IMPLICIT)) {
        continue;
      }

      Integer dimensionOrdinal = queryDimensions.get(i).getDimension().getOrdinal();
      allProjectionListDimensionIndexes.add(dimensionOrdinalToChunkMapping.get(dimensionOrdinal));
      if (queryDimensions.get(i).getDimension().getNumberOfChild() > 0) {
        addChildrenBlockIndex(allProjectionListDimensionIndexes,
            queryDimensions.get(i).getDimension());
      }

      if (!filterDimensionOrdinal.contains(dimensionOrdinal)) {
        chunkIndex = dimensionOrdinalToChunkMapping.get(dimensionOrdinal);
        dimensionChunkIndex.add(chunkIndex);
        if (queryDimensions.get(i).getDimension().getNumberOfChild() > 0) {
          addChildrenBlockIndex(dimensionChunkIndex, queryDimensions.get(i).getDimension());
        }
      }
    }
    int[] dimensionIndex = ArrayUtils
        .toPrimitive(dimensionChunkIndex.toArray(new Integer[dimensionChunkIndex.size()]));
    Arrays.sort(dimensionIndex);
    return dimensionIndex;
  }

  /**
   * Below method will be used to add the children block index
   * this will be basically for complex dimension which will have children
   *
   * @param blockIndexes block indexes
   * @param dimension    parent dimension
   */
  private static void addChildrenBlockIndex(Set<Integer> blockIndexes, CarbonDimension dimension) {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      addChildrenBlockIndex(blockIndexes, dimension.getListOfChildDimensions().get(i));
      blockIndexes.add(dimension.getListOfChildDimensions().get(i).getOrdinal());
    }
  }

  /**
   * Below method will used to get the method will be used to get the measure
   * block indexes to be read from the file
   *
   * @param queryMeasures              query measure
   * @param expressionMeasure          measure present in the expression
   * @param ordinalToBlockIndexMapping measure ordinal to block mapping
   * @return block indexes
   */
  public static int[] getMeasureChunkIndexes(List<ProjectionMeasure> queryMeasures,
      List<CarbonMeasure> expressionMeasure, Map<Integer, Integer> ordinalToBlockIndexMapping,
      Set<CarbonMeasure> filterMeasures, List<Integer> allProjectionListMeasureIdexes) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
    Set<Integer> measureChunkIndex = new HashSet<Integer>();
    Set<Integer> filterMeasureOrdinal = getFilterMeasureOrdinal(filterMeasures);
    for (int i = 0; i < queryMeasures.size(); i++) {
      Integer measureOrdinal = queryMeasures.get(i).getMeasure().getOrdinal();
      allProjectionListMeasureIdexes.add(measureOrdinal);
      if (!filterMeasureOrdinal.contains(measureOrdinal)) {
        measureChunkIndex.add(ordinalToBlockIndexMapping.get(measureOrdinal));
      }
    }
    for (int i = 0; i < expressionMeasure.size(); i++) {
      measureChunkIndex.add(ordinalToBlockIndexMapping.get(expressionMeasure.get(i).getOrdinal()));
    }
    int[] measureIndexes =
        ArrayUtils.toPrimitive(measureChunkIndex.toArray(new Integer[measureChunkIndex.size()]));
    Arrays.sort(measureIndexes);
    return measureIndexes;
  }

  /**
   * Below method will be used to get mapping whether dimension is present in
   * order by or not
   *
   * @param sortedDimensions sort dimension present in order by query
   * @param queryDimensions  query dimension
   * @return sort dimension indexes
   */
  public static byte[] getSortDimensionIndexes(List<ProjectionDimension> sortedDimensions,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
      List<ProjectionDimension> queryDimensions) {
    byte[] sortedDims = new byte[queryDimensions.size()];
    int indexOf = 0;
    for (int i = 0; i < sortedDims.length; i++) {
      indexOf = sortedDimensions.indexOf(queryDimensions.get(i));
      if (indexOf > -1) {
        sortedDims[i] = 1;
      }
    }
    return sortedDims;
  }

  /**
   * return true if given key is found in array
   *
   * @param data
   * @param key
   * @return
   */
  public static boolean searchInArray(int[] data, int key) {
    for (int i = 0; i < data.length; i++) {
      if (key == data[i]) {
        return true;
      }
    }
    return false;
  }

  /**
   * Below method will be used to get masked byte
   *
   * @param data           actual data
   * @param maxKey         max key
   * @param maskByteRanges mask byte range
   * @param byteCount
   * @return masked byte
   */
  public static byte[] getMaskedKey(byte[] data, byte[] maxKey, int[] maskByteRanges,
      int byteCount) {
    byte[] maskedKey = new byte[byteCount];
    int counter = 0;
    int byteRange = 0;
    for (int i = 0; i < byteCount; i++) {
      byteRange = maskByteRanges[i];
      if (byteRange != -1) {
        maskedKey[counter++] = (byte) (data[byteRange] & maxKey[byteRange]);
      }
    }
    return maskedKey;
  }

  /**
   * Below method will be used to fill block indexes of the query dimension
   * which will be used in creating a output row Here is method we are passing
   * two list which store the indexes one for dictionary column other for not
   * dictionary column. This is done for specific purpose so that in one
   * iteration we will be able to fill both type dimension block indexes
   *
   * @param projectDimensions                  dimension present in the query
   * @param columnOrdinalToChunkIndexMapping column ordinal to block index mapping
   * @param dictionaryDimensionChunkIndex    list to store dictionary column block indexes
   * @param noDictionaryDimensionChunkIndex  list to store no dictionary block indexes
   */
  public static void fillQueryDimensionChunkIndexes(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
      List<ProjectionDimension> projectDimensions,
      Map<Integer, Integer> columnOrdinalToChunkIndexMapping,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3371
      List<Integer> dictionaryDimensionChunkIndex,
      List<Integer> noDictionaryDimensionChunkIndex) {
    for (ProjectionDimension queryDimension : projectDimensions) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
      if (queryDimension.getDimension().getDataType() == DataTypes.DATE) {
        dictionaryDimensionChunkIndex
            .add(columnOrdinalToChunkIndexMapping.get(queryDimension.getDimension().getOrdinal()));
      } else if (
          !CarbonUtil.hasEncoding(queryDimension.getDimension().getEncoder(), Encoding.IMPLICIT)
              && queryDimension.getDimension().getNumberOfChild() == 0) {
        noDictionaryDimensionChunkIndex
            .add(columnOrdinalToChunkIndexMapping.get(queryDimension.getDimension().getOrdinal()));
      }
    }
  }

  /**
   * Below method will be used to get the map of for complex dimension and its type
   * which will be used to during query execution to
   *
   * @param queryDimensions          complex dimension in query
   * @param dimensionToBlockIndexMap dimension to block index in file map
   * @return complex dimension and query type
   */
  public static Map<Integer, GenericQueryType> getComplexDimensionsMap(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
      List<ProjectionDimension> queryDimensions, Map<Integer, Integer> dimensionToBlockIndexMap,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
      Set<CarbonDimension> filterDimensions) {
    Map<Integer, GenericQueryType> complexTypeMap = new HashMap<Integer, GenericQueryType>();
    for (ProjectionDimension dimension : queryDimensions) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2606
      CarbonDimension actualDimension;
      CarbonDimension complexDimension = null;
      if (null != dimension.getDimension().getComplexParentDimension()) {
        // get the parent dimension column.
        actualDimension = dimension.getParentDimension();
        if (dimension.getDimension().isComplex()) {
          complexDimension = dimension.getDimension();
        }
      } else {
        actualDimension = dimension.getDimension();
      }
      if (actualDimension.getNumberOfChild() == 0) {
        continue;
      }
      if (complexDimension != null) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
        fillParentDetails(dimensionToBlockIndexMap, complexDimension, complexTypeMap);
      }
      fillParentDetails(dimensionToBlockIndexMap, actualDimension, complexTypeMap);
    }
    if (null != filterDimensions) {
      for (CarbonDimension filterDimension : filterDimensions) {
        // do not fill nay details for implicit dimension type
        if (filterDimension.hasEncoding(Encoding.IMPLICIT)
            || filterDimension.getNumberOfChild() == 0) {
          continue;
        }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
        fillParentDetails(dimensionToBlockIndexMap, filterDimension, complexTypeMap);
      }
    }
    return complexTypeMap;
  }

  private static void fillParentDetails(Map<Integer, Integer> dimensionToBlockIndexMap,
      CarbonDimension dimension, Map<Integer, GenericQueryType> complexTypeMap) {
    int parentBlockIndex = dimensionToBlockIndexMap.get(dimension.getOrdinal());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
    GenericQueryType parentQueryType;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1662
    if (DataTypes.isArrayType(dimension.getDataType())) {
      parentQueryType =
          new ArrayQueryType(dimension.getColName(), dimension.getColName(), parentBlockIndex);
    } else if (DataTypes.isStructType(dimension.getDataType())) {
      parentQueryType =
          new StructQueryType(dimension.getColName(), dimension.getColName(),
              dimensionToBlockIndexMap.get(dimension.getOrdinal()));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2869
    } else if (DataTypes.isMapType(dimension.getDataType())) {
      parentQueryType =
          new MapQueryType(dimension.getColName(), dimension.getColName(), parentBlockIndex);
    } else {
      throw new UnsupportedOperationException(dimension.getDataType().getName() +
          " is not supported");
    }
    complexTypeMap.put(dimension.getOrdinal(), parentQueryType);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
    fillChildrenDetails(parentBlockIndex, dimension, parentQueryType);
  }

  private static int fillChildrenDetails(int parentColumnIndex, CarbonDimension dimension,
      GenericQueryType parentQueryType) {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      DataType dataType = dimension.getListOfChildDimensions().get(i).getDataType();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1662
      if (DataTypes.isArrayType(dataType)) {
        parentQueryType.addChildren(
            new ArrayQueryType(dimension.getListOfChildDimensions().get(i).getColName(),
                dimension.getColName(), ++parentColumnIndex));
      } else if (DataTypes.isStructType(dataType)) {
        parentQueryType.addChildren(
            new StructQueryType(dimension.getListOfChildDimensions().get(i).getColName(),
                dimension.getColName(), ++parentColumnIndex));
      } else if (DataTypes.isMapType(dataType)) {
        parentQueryType.addChildren(
            new MapQueryType(dimension.getListOfChildDimensions().get(i).getColName(),
                dimension.getColName(), ++parentColumnIndex));
      } else {
        boolean isDirectDictionary = CarbonUtil
            .hasEncoding(dimension.getListOfChildDimensions().get(i).getEncoder(),
                Encoding.DIRECT_DICTIONARY);

        parentQueryType.addChildren(
            new PrimitiveQueryType(dimension.getListOfChildDimensions().get(i).getColName(),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3684
                dimension.getColName(), ++parentColumnIndex,
                dimension.getListOfChildDimensions().get(i).getDataType(), isDirectDictionary));
      }
      if (dimension.getListOfChildDimensions().get(i).getNumberOfChild() > 0) {
        parentColumnIndex = fillChildrenDetails(parentColumnIndex,
            dimension.getListOfChildDimensions().get(i), parentQueryType);
      }
    }
    return parentColumnIndex;
  }

  public static void getAllFilterDimensionsAndMeasures(FilterResolverIntf filterResolverTree,
      Set<CarbonDimension> filterDimensions, Set<CarbonMeasure> filterMeasure) {
    if (null == filterResolverTree) {
      return;
    }
    List<ColumnExpression> dimensionResolvedInfos = new ArrayList<ColumnExpression>();
    Expression filterExpression = filterResolverTree.getFilterExpression();
    addColumnDimensions(filterExpression, filterDimensions, filterMeasure);
    for (ColumnExpression info : dimensionResolvedInfos) {
      if (info.isDimension() && info.getDimension().getNumberOfChild() > 0) {
        filterDimensions.add(info.getDimension());
      }
    }
  }

  /**
   * This method will check if a given expression contains a column expression
   * recursively and add the dimension instance to the set which holds the dimension
   * instances of the complex filter expressions.
   */
  private static void addColumnDimensions(Expression expression,
      Set<CarbonDimension> filterDimensions, Set<CarbonMeasure> filterMeasure) {
    if (null != expression && expression instanceof ColumnExpression) {
      if (((ColumnExpression) expression).isDimension()) {
        filterDimensions.add(((ColumnExpression) expression).getDimension());
      } else {
        filterMeasure.add((CarbonMeasure) ((ColumnExpression) expression).getCarbonColumn());
      }
      return;
    } else if (null != expression) {
      for (Expression child : expression.getChildren()) {
        addColumnDimensions(child, filterDimensions, filterMeasure);
      }
    }
  }

  private static Set<Integer> getFilterMeasureOrdinal(Set<CarbonMeasure> filterMeasures) {
    Set<Integer> filterMeasuresOrdinal = new HashSet<>();
    for (CarbonMeasure filterMeasure : filterMeasures) {
      filterMeasuresOrdinal.add(filterMeasure.getOrdinal());
    }
    return filterMeasuresOrdinal;
  }

  private static Set<Integer> getFilterDimensionOrdinal(Set<CarbonDimension> filterDimensions) {
    Set<Integer> filterDimensionsOrdinal = new HashSet<>();
    for (CarbonDimension filterDimension : filterDimensions) {
      filterDimensionsOrdinal.add(filterDimension.getOrdinal());
      getChildDimensionOrdinal(filterDimension, filterDimensionsOrdinal);
    }
    return filterDimensionsOrdinal;
  }

  /**
   * Below method will be used to fill the children dimension column id
   */
  private static void getChildDimensionOrdinal(CarbonDimension queryDimensions,
      Set<Integer> filterDimensionsOrdinal) {
    for (int j = 0; j < queryDimensions.getNumberOfChild(); j++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
      CarbonDimension dimension = queryDimensions.getListOfChildDimensions().get(j);
      if (dimension.getNumberOfChild() > 0) {
        getChildDimensionOrdinal(queryDimensions.getListOfChildDimensions().get(j),
            filterDimensionsOrdinal);
      } else if (dimension.getDataType() != DataTypes.DATE) {
        filterDimensionsOrdinal.add(queryDimensions.getListOfChildDimensions().get(j).getOrdinal());
      }
    }
  }

  /**
   * Below method will be used to convert the thrift presence meta to wrapper
   * presence meta
   *
   * @return wrapper presence meta
   */
  public static BitSet getNullBitSet(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
      org.apache.carbondata.format.PresenceMeta presentMetadataThrift, Compressor compressor) {
    final byte[] present_bit_stream = presentMetadataThrift.getPresent_bit_stream();
    if (null != present_bit_stream) {
      return BitSet.valueOf(compressor.unCompressByte(present_bit_stream));
    } else {
      return new BitSet(1);
    }
  }

  /**
   * In case of non transactional table just set columnuniqueid as columnName to support
   * backward compatabiity. non transactional tables column uniqueid is always equal to
   * columnname
   */
  public static void updateColumnUniqueIdForNonTransactionTable(List<ColumnSchema> columnSchemas) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2910
    for (ColumnSchema columnSchema : columnSchemas) {
      // In case of complex types only add the name after removing parent names.
      int index = columnSchema.getColumnName().lastIndexOf(".");
      if (index >= 0) {
        columnSchema.setColumnUniqueId(columnSchema.getColumnName()
            .substring(index + 1, columnSchema.getColumnName().length()));
      } else {
        columnSchema.setColumnUniqueId(columnSchema.getColumnName());
      }
    }
  }

  /**
   * Put the data to vector
   *
   * @param vector
   * @param value
   * @param vectorRow
   * @param length
   */
  public static void putDataToVector(CarbonColumnVector vector, byte[] value, int vectorRow,
      int length) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
    DataType dt = vector.getType();
    if ((!(dt == DataTypes.STRING) && length == 0) || ByteUtil.UnsafeComparer.INSTANCE
        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
            CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, value, 0, length)) {
      vector.putNull(vectorRow);
    } else {
      if (dt == DataTypes.STRING) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3012
        vector.putByteArray(vectorRow, 0, length, value);
      } else if (dt == DataTypes.BOOLEAN) {
        vector.putBoolean(vectorRow, ByteUtil.toBoolean(value[0]));
      } else if (dt == DataTypes.BYTE) {
        vector.putByte(vectorRow, value[0]);
      } else if (dt == DataTypes.SHORT) {
        vector.putShort(vectorRow, ByteUtil.toXorShort(value, 0, length));
      } else if (dt == DataTypes.INT) {
        vector.putInt(vectorRow, ByteUtil.toXorInt(value, 0, length));
      } else if (dt == DataTypes.LONG) {
        vector.putLong(vectorRow, DataTypeUtil
            .getDataBasedOnRestructuredDataType(value, vector.getBlockDataType(), 0, length));
      } else if (dt == DataTypes.TIMESTAMP) {
        vector.putLong(vectorRow, ByteUtil.toXorLong(value, 0, length) * 1000L);
      }
    }
  }

  /**
   * In case of index server there will not be any details info serialize from driver.
   * Below method will use to create blocklet detail info object from footer
   * @param fileFooter
   * @param blockInfo
   * @return
   */
  public static BlockletDetailInfo getBlockletDetailInfo(DataFileFooter fileFooter,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3447
      TableBlockInfo blockInfo) {
    BlockletDetailInfo detailInfo = new BlockletDetailInfo();
    detailInfo.setBlockletInfoBinary(new byte[0]);
    detailInfo.setColumnSchemas(fileFooter.getColumnInTable());
    detailInfo.setBlockletId((short) -1);
    detailInfo.setRowCount((int) fileFooter.getNumberOfRows());
    detailInfo.setSchemaUpdatedTimeStamp(fileFooter.getSchemaUpdatedTimeStamp());
    detailInfo.setBlockFooterOffset(blockInfo.getBlockOffset());
    detailInfo.setBlockSize(blockInfo.getBlockLength());
    detailInfo.setUseMinMaxForPruning(true);
    detailInfo.setVersionNumber(blockInfo.getVersion().number());
    return detailInfo;
  }
}
