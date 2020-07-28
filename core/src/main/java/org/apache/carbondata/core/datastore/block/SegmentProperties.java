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

package org.apache.carbondata.core.datastore.block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.log4j.Logger;

/**
 * This class contains all the details about the restructuring information of
 * the block. This will be used during query execution to handle restructure
 * information
 */
public class SegmentProperties {

  private static final Logger LOG =
        LogServiceFactory.getLogService(SegmentProperties.class.getName());

  // When calculating the finger printer of all columns. In order to
  // identify dimension columns with other column. The finger printer
  // of dimensions will left shift 1 bit
  private static final int DIMENSIONS_FINGER_PRINTER_SHIFT = 1;

  // When calculating the finger pinter of all columns. In order to
  // identify measure columns with other column. The finger printer
  // of measures will left shift 2 bit
  private static final int MEASURES_FINGER_PRINTER_SHIFT = 2;

  // When calculating the finger pinter of all columns. In order to
  // identify complex columns with other column. The finger printer
  // of complex columns will left shift 3 bit
  private static final int COMPLEX_FINGER_PRINTER_SHIFT = 3;

  /**
   * list of dimension present in the block
   */
  private List<CarbonDimension> dimensions;

  /**
   * list of dimension present in the block
   */
  private List<CarbonDimension> complexDimensions;

  /**
   * list of measure present in the block
   */
  private List<CarbonMeasure> measures;

  /**
   * mapping of dimension ordinal in schema to column chunk index in the data file
   */
  private Map<Integer, Integer> dimensionOrdinalToChunkMapping;

  /**
   * a block can have multiple columns. This will have block index as key
   * and all dimension participated in that block as values
   */
  private Map<Integer, Set<Integer>> blockToDimensionOrdinalMapping;

  /**
   * mapping of measure ordinal in schema to column chunk index in the data file
   */
  private Map<Integer, Integer> measuresOrdinalToChunkMapping;

  /**
   * to store the number of no dictionary dimension
   * this will be used during query execution for creating
   * start and end key. Purpose of storing this value here is
   * so during query execution no need to calculate every time
   */
  private int numberOfNoDictionaryDimension;

  private int numberOfSortColumns;

  private int numberOfDictDimensions;

  private int numberOfColumnsAfterFlatten;

  private int lastDimensionColOrdinal;

  /**
   * The finger printer is the xor result of all the columns in table.
   * Besides, in the case of two segment properties have same columns
   * but different sort column, n like there is a column exists in both
   * segment properties, but is dimension in one segment properties,
   * but is a measure in the other. In order to identify the difference
   * of these two segment properties. The xor result of all dimension
   * will left shift 1 bit, the xor results of all measures will left shift
   * 2bit, and the xor results of all complex columns will left shift 3 bits
   * Sum up, the Formula of generate finger printer is
   *
   * fingerPrinter = (dimensionFingerPrinter >> 1)
   * ^ (measureFingerPrinter >> 1) ^ (complexFingerPrinter >> 1)
   * dimensionsFingerPrinter = dimension1 ^ dimension2 ^ ...
   * measuresFingerPrinter = measure1 ^ measure2 ^ measure3 ...
   * complexFingerPrinter = complex1 ^ complex2 ^ complex3 ...
   */
  private long fingerPrinter = Long.MAX_VALUE;

  public SegmentProperties(List<ColumnSchema> columnsInTable) {
    dimensions = new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    complexDimensions =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    measures = new ArrayList<CarbonMeasure>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    fillDimensionAndMeasureDetails(columnsInTable);
    dimensionOrdinalToChunkMapping =
        new HashMap<Integer, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    blockToDimensionOrdinalMapping =
        new HashMap<Integer, Set<Integer>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    measuresOrdinalToChunkMapping =
        new HashMap<Integer, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    fillOrdinalToBlockMappingForDimension();
    fillOrdinalToChunkIndexMappingForMeasureColumns();
  }

  /**
   * below method is to fill the dimension and its mapping to file blocks all
   * the column will point to same column group
   */
  private void fillOrdinalToBlockMappingForDimension() {
    int blockOrdinal = -1;
    CarbonDimension dimension = null;
    int index = 0;
    while (index < dimensions.size()) {
      dimension = dimensions.get(index);
      blockOrdinal++;
      dimensionOrdinalToChunkMapping.put(dimension.getOrdinal(), blockOrdinal);
      index++;
    }
    index = 0;
    // complex dimension will be stored at last
    while (index < complexDimensions.size()) {
      dimension = complexDimensions.get(index);
      dimensionOrdinalToChunkMapping.put(dimension.getOrdinal(), ++blockOrdinal);
      blockOrdinal = fillComplexDimensionChildBlockIndex(blockOrdinal, dimension);
      index++;
    }
    fillBlockToDimensionOrdinalMapping();
  }

  private void fillBlockToDimensionOrdinalMapping() {
    Set<Entry<Integer, Integer>> blocks = dimensionOrdinalToChunkMapping.entrySet();
    for (Entry<Integer, Integer> block : blocks) {
      Set<Integer> dimensionOrdinals = blockToDimensionOrdinalMapping.get(block.getValue());
      if (dimensionOrdinals == null) {
        dimensionOrdinals = new HashSet<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        blockToDimensionOrdinalMapping.put(block.getValue(), dimensionOrdinals);
      }
      dimensionOrdinals.add(block.getKey());
    }
  }

  /**
   * compare the segment properties based on finger printer
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SegmentProperties)) {
      return false;
    }
    // If these two segment properties have different number of columns
    // Return false directly
    SegmentProperties segmentProperties = (SegmentProperties) obj;
    if (this.getNumberOfColumns() != segmentProperties.getNumberOfColumns()) {
      return false;
    }
    // Compare the finger printer
    return getFingerPrinter() != Long.MIN_VALUE &&
            segmentProperties.getFingerPrinter() != Long.MIN_VALUE &&
            (getFingerPrinter() == segmentProperties.getFingerPrinter());
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Below method will be used to add the complex dimension child
   * block index.It is a recursive method which will be get the children
   * add the block index
   *
   * @param blockOrdinal start block ordinal
   * @param dimension    parent dimension
   * @return last block index
   */
  private int fillComplexDimensionChildBlockIndex(int blockOrdinal, CarbonDimension dimension) {
    for (int i = 0; i < dimension.getNumberOfChild(); i++) {
      dimensionOrdinalToChunkMapping
          .put(dimension.getListOfChildDimensions().get(i).getOrdinal(), ++blockOrdinal);
      if (dimension.getListOfChildDimensions().get(i).getNumberOfChild() > 0) {
        blockOrdinal = fillComplexDimensionChildBlockIndex(blockOrdinal,
            dimension.getListOfChildDimensions().get(i));
      }
    }
    return blockOrdinal;
  }

  /**
   * Below method will be used to fill the mapping
   * of measure ordinal to its block index mapping in
   * file
   */
  private void fillOrdinalToChunkIndexMappingForMeasureColumns() {
    int blockOrdinal = 0;
    int index = 0;
    while (index < measures.size()) {
      measuresOrdinalToChunkMapping.put(measures.get(index).getOrdinal(), blockOrdinal);
      blockOrdinal++;
      index++;
    }
  }

  /**
   * fingerPrinter = (dimensionFingerPrinter >> 1)
   *   ^ (measureFingerPrinter >> 1) ^ (complexFingerPrinter >> 1)
   * dimensionsFingerPrinter = dimension1 ^ dimension2 ^ ...
   * measuresFingerPrinter = measure1 ^ measure2 ^ measure3 ...
   * complexFingerPrinter = complex1 ^ complex2 ^ complex3 ...
   */
  protected long getFingerPrinter() {
    if (this.fingerPrinter == Long.MAX_VALUE) {
      long dimensionsFingerPrinter = getFingerPrinter(this.dimensions.stream()
              .map(CarbonColumn::getColumnSchema).collect(Collectors.toList()));
      long measuresFingerPrinter = getFingerPrinter(this.measures.stream()
              .map(CarbonColumn::getColumnSchema).collect(Collectors.toList()));
      long complexFingerPrinter = getFingerPrinter(this.complexDimensions.stream()
              .map(CarbonColumn::getColumnSchema).collect(Collectors.toList()));
      this.fingerPrinter = (dimensionsFingerPrinter >> DIMENSIONS_FINGER_PRINTER_SHIFT)
              ^ (measuresFingerPrinter >> MEASURES_FINGER_PRINTER_SHIFT)
              ^ (complexFingerPrinter >> COMPLEX_FINGER_PRINTER_SHIFT);
    }
    return this.fingerPrinter;
  }

  private long getFingerPrinter(List<ColumnSchema> columns) {
    int counter = 0;
    ColumnSchema columnSchema = null;
    long fingerprint = Long.MAX_VALUE;
    while (counter < columns.size()) {
      columnSchema = columns.get(counter);
      UUID columnUUID = null;
      try {
        columnUUID = UUID.fromString(columnSchema.getColumnUniqueId());
      } catch (Exception e) {
        LOG.error("Invalid UUID string: " + columnSchema.getColumnUniqueId());
        return Long.MIN_VALUE;
      }
      long columnUUIDToBits = columnUUID.getMostSignificantBits();
      fingerprint = fingerprint ^ columnUUIDToBits;
      counter++;
    }
    return fingerprint;
  }

  /**
   * below method will fill dimension and measure detail of the block.
   *
   * @param columnsInTable
   */
  private void fillDimensionAndMeasureDetails(List<ColumnSchema> columnsInTable) {
    ColumnSchema columnSchema = null;
    // ordinal will be required to read the data from file block
    int dimensionOrdinal = 0;
    int measureOrdinal = -1;
    // table ordinal is actually a schema ordinal this is required as
    // cardinality array
    // which is stored in segment info contains -1 if that particular column
    // is n
    boolean isComplexDimensionStarted = false;
    CarbonDimension carbonDimension = null;
    int keyOrdinal = 0;
    int counter = 0;
    int complexTypeOrdinal = -1;
    while (counter < columnsInTable.size()) {
      columnSchema = columnsInTable.get(counter);
      if (columnSchema.isDimensionColumn()) {
        // not adding the cardinality of the non dictionary
        // column as it was not the part of MDKey
        if (CarbonUtil.hasEncoding(columnSchema.getEncodingList(), Encoding.DICTIONARY)
            && !isComplexDimensionStarted && columnSchema.getNumberOfChild() == 0) {
          this.numberOfDictDimensions++;
          this.numberOfColumnsAfterFlatten++;
          if (columnSchema.isSortColumn()) {
            this.numberOfSortColumns++;
          }
          // if it is a columnar dimension participated in MDKey then added
          // key ordinal and dimension ordinal
          carbonDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, keyOrdinal++, -1);
        }
        // as complex type will be stored at last so once complex type started all the dimension
        // will be added to complex type
        else if (isComplexDimensionStarted || columnSchema.getDataType().isComplexType()) {
          carbonDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, -1, ++complexTypeOrdinal);
          carbonDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
          complexDimensions.add(carbonDimension);
          isComplexDimensionStarted = true;
          dimensionOrdinal =
              readAllComplexTypeChildren(dimensionOrdinal, columnSchema.getNumberOfChild(),
                  columnsInTable, carbonDimension, complexTypeOrdinal);
          counter = dimensionOrdinal;
          complexTypeOrdinal = assignComplexOrdinal(carbonDimension, complexTypeOrdinal);
          this.numberOfColumnsAfterFlatten += getNumColumnsAfterFlatten(carbonDimension);
          continue;
        } else {
          // for no dictionary dimension
          carbonDimension = new CarbonDimension(columnSchema, dimensionOrdinal++, -1, -1);
          numberOfColumnsAfterFlatten++;
          numberOfNoDictionaryDimension++;
          if (columnSchema.isSortColumn()) {
            this.numberOfSortColumns++;
          }
        }
        dimensions.add(carbonDimension);
      } else {
        numberOfColumnsAfterFlatten++;
        measures.add(new CarbonMeasure(columnSchema, ++measureOrdinal));
      }
      counter++;
    }
    lastDimensionColOrdinal = dimensionOrdinal;
  }

  /**
   * Read all primitive/complex children and set it as list of child carbon dimension to parent
   * dimension
   *
   * @param dimensionOrdinal
   * @param childCount
   * @param listOfColumns
   * @param parentDimension
   * @return
   */
  private int readAllComplexTypeChildren(int dimensionOrdinal, int childCount,
      List<ColumnSchema> listOfColumns, CarbonDimension parentDimension,
      int complexDimensionOrdinal) {
    for (int i = 0; i < childCount; i++) {
      ColumnSchema columnSchema = listOfColumns.get(dimensionOrdinal);
      if (columnSchema.isDimensionColumn()) {
        if (columnSchema.getNumberOfChild() > 0) {
          CarbonDimension complexDimension =
              new CarbonDimension(columnSchema, dimensionOrdinal++, -1, complexDimensionOrdinal++);
          complexDimension.initializeChildDimensionsList(columnSchema.getNumberOfChild());
          parentDimension.getListOfChildDimensions().add(complexDimension);
          dimensionOrdinal =
              readAllComplexTypeChildren(dimensionOrdinal, columnSchema.getNumberOfChild(),
                  listOfColumns, complexDimension, complexDimensionOrdinal);
        } else {
          parentDimension.getListOfChildDimensions().add(
              new CarbonDimension(columnSchema, dimensionOrdinal++, -1, complexDimensionOrdinal++));
        }
      }
    }
    return dimensionOrdinal;
  }

  /**
   * Read all primitive/complex children and set it as list of child carbon dimension to parent
   * dimension
   */
  private int assignComplexOrdinal(CarbonDimension parentDimension, int complexDimensionOrdinal) {
    for (int i = 0; i < parentDimension.getNumberOfChild(); i++) {
      CarbonDimension dimension = parentDimension.getListOfChildDimensions().get(i);
      if (dimension.getNumberOfChild() > 0) {
        dimension.setComplexTypeOrdinal(++complexDimensionOrdinal);
        complexDimensionOrdinal = assignComplexOrdinal(dimension, complexDimensionOrdinal);
      } else {
        parentDimension.getListOfChildDimensions().get(i)
            .setComplexTypeOrdinal(++complexDimensionOrdinal);
      }
    }
    return complexDimensionOrdinal;
  }

  /**
   * @return the dimensions
   */
  public List<CarbonDimension> getDimensions() {
    return dimensions;
  }

  /**
   * @return the complexDimensions
   */
  public List<CarbonDimension> getComplexDimensions() {
    return complexDimensions;
  }

  /**
   * @return the measures
   */
  public List<CarbonMeasure> getMeasures() {
    return measures;
  }

  /**
   * @return the dimensionOrdinalToChunkMapping
   */
  public Map<Integer, Integer> getDimensionOrdinalToChunkMapping() {
    return dimensionOrdinalToChunkMapping;
  }

  /**
   * @return the measuresOrdinalToChunkMapping
   */
  public Map<Integer, Integer> getMeasuresOrdinalToChunkMapping() {
    return measuresOrdinalToChunkMapping;
  }

  /**
   * @return the numberOfNoDictionaryDimension
   */
  public int getNumberOfNoDictionaryDimension() {
    return numberOfNoDictionaryDimension;
  }

  /**
   * @return It returns block index to dimension ordinal mapping
   */
  public Map<Integer, Set<Integer>> getBlockToDimensionOrdinalMapping() {
    return blockToDimensionOrdinalMapping;
  }

  /**
   * This method will search a given dimension and return the dimension from current block
   *
   * @param queryDimension
   * @return
   */
  public CarbonDimension getDimensionFromCurrentBlock(CarbonDimension queryDimension) {
    if (queryDimension.isComplex()) {
      return CarbonUtil.getDimensionFromCurrentBlock(this.complexDimensions, queryDimension);
    }
    return CarbonUtil.getDimensionFromCurrentBlock(this.dimensions, queryDimension);
  }

  /**
   * This method will search for a given measure in the current block measures list
   *
   * @param measureToBeSearched
   * @return
   */
  public CarbonMeasure getMeasureFromCurrentBlock(CarbonMeasure measureToBeSearched) {
    return CarbonUtil.getMeasureFromCurrentBlock(this.measures, measureToBeSearched);
  }

  public int getNumberOfSortColumns() {
    return numberOfSortColumns;
  }

  public int getLastDimensionColOrdinal() {
    return lastDimensionColOrdinal;
  }

  public int getNumberOfColumns() {
    return numberOfColumnsAfterFlatten;
  }

  public int getNumberOfDictDimensions() {
    return numberOfDictDimensions;
  }

  public int getNumberOfPrimitiveDimensions() {
    return numberOfDictDimensions + numberOfNoDictionaryDimension;
  }

  public int getNumberOfComplexDimensions() {
    return complexDimensions.size();
  }

  public int getNumberOfMeasures() {
    return measures.size();
  }

  /**
   * Return column value length in byte for all dimension columns in the table
   * for dimension it is -1 (for DATE it is 4),
   */
  public int[] createDimColumnValueLength() {
    int[] length = new int[dimensions.size()];
    int index = 0;
    for (CarbonDimension dimension : dimensions) {
      DataType dataType = dimension.getDataType();
      if (dataType == DataTypes.DATE) {
        length[index] = 4;
      } else {
        length[index] = -1;
      }
      index++;
    }
    return length;
  }

  /**
   * Return column value length in byte for all columns in the table
   * for dimension and complex column it is -1 (for DATE it is 4),
   * for measure is 8 (for decimal is -1)
   */
  public int[] createColumnValueLength() {
    int[] length = new int[numberOfColumnsAfterFlatten];
    int index = 0;
    for (CarbonDimension dimension : dimensions) {
      DataType dataType = dimension.getDataType();
      if (dataType == DataTypes.DATE) {
        length[index] = 4;
      } else {
        length[index] = -1;
      }
      index++;
    }
    for (CarbonDimension complexDimension : complexDimensions) {
      int depth = getNumColumnsAfterFlatten(complexDimension);
      for (int i = 0; i < depth; i++) {
        length[index++] = -1;
      }
    }
    for (CarbonMeasure measure : measures) {
      DataType dataType = measure.getDataType();
      if (DataTypes.isDecimal(dataType)) {
        length[index++] = -1;
      } else {
        length[index++] = 8;
      }
    }
    return length;
  }

  private int getNumColumnsAfterFlatten(CarbonDimension dimension) {
    int count = 1;
    if (dimension.isComplex()) {
      List<CarbonDimension> children = dimension.getListOfChildDimensions();
      for (CarbonDimension child : children) {
        count += getNumColumnsAfterFlatten(child);
      }
    }
    return count;
  }
}
