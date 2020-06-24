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

package org.apache.carbondata.core.datastore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

public class TableSpec {

  // column spec for each dimension and measure
  private DimensionSpec[] dimensionSpec;
  private MeasureSpec[] measureSpec;

  // Many places we might have to access no-dictionary column spec.
  // but no-dictionary column spec are not always in below order like,
  // dictionary + no dictionary + complex + measure
  // when sort_columns are empty, no columns are selected for sorting.
  // so, spec will not be in above order.
  // Hence noDictionaryDimensionSpec will be useful and it will be subset of dimensionSpec.
  private List<DimensionSpec> noDictionaryDimensionSpec;

  // number of simple dimensions
  private int numSimpleDimensions;

  private CarbonTable carbonTable;

  private boolean isUpdateDictDim;

  private boolean isUpdateNoDictDims;
  private int[] dictDimActualPosition;
  private int[] noDictDimActualPosition;

  public TableSpec(CarbonTable carbonTable, boolean keepPartitionColumnsToEnd) {
    this.carbonTable = carbonTable;
    List<CarbonDimension> dimensions = carbonTable.getVisibleDimensions();
    List<CarbonMeasure> measures = carbonTable.getVisibleMeasures();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3728
    if (keepPartitionColumnsToEnd && carbonTable.getPartitionInfo() != null) {
      // keep the partition columns in the end
      List<CarbonDimension> reArrangedDimensions = new ArrayList<>();
      List<CarbonMeasure> reArrangedMeasures = new ArrayList<>();
      List<CarbonDimension> partitionDimensions = new ArrayList<>();
      List<CarbonMeasure> partitionMeasures = new ArrayList<>();
      List<ColumnSchema> columnSchemaList = carbonTable.getPartitionInfo().getColumnSchemaList();
      for (CarbonDimension dim : dimensions) {
        if (columnSchemaList.contains(dim.getColumnSchema())) {
          partitionDimensions.add(dim);
        } else {
          reArrangedDimensions.add(dim);
        }
      }
      if (partitionDimensions.size() != 0) {
        reArrangedDimensions.addAll(partitionDimensions);
      }
      for (CarbonMeasure measure : measures) {
        if (columnSchemaList.contains(measure.getColumnSchema())) {
          partitionMeasures.add(measure);
        } else {
          reArrangedMeasures.add(measure);
        }
      }
      if (partitionMeasures.size() != 0) {
        reArrangedMeasures.addAll(partitionMeasures);
      }
      dimensions = reArrangedDimensions;
      measures = reArrangedMeasures;
    }
    // first calculate total number of columnar field considering column group and complex column
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1268
    numSimpleDimensions = 0;
    for (CarbonDimension dimension : dimensions) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2720
      if (!dimension.isComplex()) {
        numSimpleDimensions++;
      }
    }
    dimensionSpec = new DimensionSpec[dimensions.size()];
    measureSpec = new MeasureSpec[measures.size()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2926
    noDictionaryDimensionSpec = new ArrayList<>();
    addDimensions(dimensions);
    addMeasures(measures);
  }

  private void addDimensions(List<CarbonDimension> dimensions) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3335
    List<DimensionSpec> dictSortDimSpec = new ArrayList<>();
    List<DimensionSpec> noSortDictDimSpec = new ArrayList<>();
    List<DimensionSpec> noSortNoDictDimSpec = new ArrayList<>();
    List<DimensionSpec> noDictSortDimSpec = new ArrayList<>();
    List<DimensionSpec> dictDimensionSpec = new ArrayList<>();
    int dimIndex = 0;
    DimensionSpec spec;
    short dictActualPosition = 0;
    short noDictActualPosition = 0;
    // sort step's output is based on sort column order i.e sort columns data will be present
    // ahead of non sort columns, so table spec also need to add dimension spec in same manner
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dimension = dimensions.get(i);
      if (dimension.isComplex()) {
        spec = new DimensionSpec(ColumnType.COMPLEX, dimension, noDictActualPosition++);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3335
        dimensionSpec[dimIndex++] = spec;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2926
        noDictionaryDimensionSpec.add(spec);
        noSortNoDictDimSpec.add(spec);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
      } else if (dimension.getDataType() == DataTypes.TIMESTAMP) {
        spec = new DimensionSpec(ColumnType.PLAIN_VALUE, dimension, noDictActualPosition++);
        dimensionSpec[dimIndex++] = spec;
        noDictionaryDimensionSpec.add(spec);
        if (dimension.isSortColumn()) {
          noDictSortDimSpec.add(spec);
        } else {
          noSortNoDictDimSpec.add(spec);
        }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3674
      } else if (dimension.getDataType() == DataTypes.DATE) {
        spec = new DimensionSpec(ColumnType.DIRECT_DICTIONARY, dimension, dictActualPosition++);
        dimensionSpec[dimIndex++] = spec;
        dictDimensionSpec.add(spec);
        if (dimension.isSortColumn()) {
          dictSortDimSpec.add(spec);
        } else {
          noSortDictDimSpec.add(spec);
        }
      } else {
        spec = new DimensionSpec(ColumnType.PLAIN_VALUE, dimension, noDictActualPosition++);
        dimensionSpec[dimIndex++] = spec;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2926
        noDictionaryDimensionSpec.add(spec);
        if (dimension.isSortColumn()) {
          noDictSortDimSpec.add(spec);
        } else {
          noSortNoDictDimSpec.add(spec);
        }
      }
    }
    noDictSortDimSpec.addAll(noSortNoDictDimSpec);
    dictSortDimSpec.addAll(noSortDictDimSpec);

    this.dictDimActualPosition = new int[dictSortDimSpec.size()];
    this.noDictDimActualPosition = new int[noDictSortDimSpec.size()];
    for (int i = 0; i < dictDimActualPosition.length; i++) {
      dictDimActualPosition[i] = dictSortDimSpec.get(i).getActualPostion();
    }
    for (int i = 0; i < noDictDimActualPosition.length; i++) {
      noDictDimActualPosition[i] = noDictSortDimSpec.get(i).getActualPostion();
    }
    isUpdateNoDictDims = !noDictSortDimSpec.equals(noDictionaryDimensionSpec);
    isUpdateDictDim = !dictSortDimSpec.equals(dictDimensionSpec);
  }

  private void addMeasures(List<CarbonMeasure> measures) {
    for (int i = 0; i < measures.size(); i++) {
      CarbonMeasure measure = measures.get(i);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
      measureSpec[i] = new MeasureSpec(measure.getColName(), measure.getDataType());
    }
  }

  public int[] getDictDimActualPosition() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3335
    return dictDimActualPosition;
  }

  public int[] getNoDictDimActualPosition() {
    return noDictDimActualPosition;
  }

  public boolean isUpdateDictDim() {
    return isUpdateDictDim;
  }

  public boolean isUpdateNoDictDims() {
    return isUpdateNoDictDims;
  }

  /**
   * No dictionary and complex dimensions of the table
   *
   * @return
   */
  public DimensionSpec[] getNoDictAndComplexDimensions() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
    List<DimensionSpec> noDictAndComplexDimensions = new ArrayList<>();
    for (int i = 0; i < dimensionSpec.length; i++) {
      if (dimensionSpec[i].getColumnType() == ColumnType.PLAIN_VALUE
          || dimensionSpec[i].getColumnType() == ColumnType.COMPLEX_PRIMITIVE
          || dimensionSpec[i].getColumnType() == ColumnType.COMPLEX) {
        noDictAndComplexDimensions.add(dimensionSpec[i]);
      }
    }
    return noDictAndComplexDimensions.toArray(new DimensionSpec[noDictAndComplexDimensions.size()]);
  }

  public DimensionSpec getDimensionSpec(int dimensionIndex) {
    return dimensionSpec[dimensionIndex];
  }

  public List<DimensionSpec> getNoDictionaryDimensionSpec() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2926
    return noDictionaryDimensionSpec;
  }

  public MeasureSpec getMeasureSpec(int measureIndex) {
    return measureSpec[measureIndex];
  }

  public int getNumSimpleDimensions() {
    return numSimpleDimensions;
  }

  public int getNumDimensions() {
    return dimensionSpec.length;
  }

  /**
   * return number of measures
   */
  public int getNumMeasures() {
    return measureSpec.length;
  }

  public CarbonTable getCarbonTable() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1543
    return carbonTable;
  }

  public static class ColumnSpec implements Writable {
    // field name of this column
    private String fieldName;

    // data type of this column
    private DataType schemaDataType;

    // dimension type of this dimension
    private ColumnType columnType;

    public ColumnSpec() {
    }

    private ColumnSpec(String fieldName, DataType schemaDataType, ColumnType columnType) {
      this.fieldName = fieldName;
      this.schemaDataType = schemaDataType;
      this.columnType = columnType;
    }

    public static ColumnSpec newInstance(String fieldName, DataType schemaDataType,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
        ColumnType columnType) {
      return new ColumnSpec(fieldName, schemaDataType, columnType);
    }

    public static ColumnSpec newInstanceLegacy(String fieldName, DataType schemaDataType,
        ColumnType columnType) {
      // for backward compatibility as the precision and scale is not stored, the values should be
      // initialized with -1 for both precision and scale
      if (schemaDataType instanceof DecimalType) {
        ((DecimalType) schemaDataType).setPrecision(-1);
        ((DecimalType) schemaDataType).setScale(-1);
      }
      return new ColumnSpec(fieldName, schemaDataType, columnType);
    }

    public DataType getSchemaDataType() {
      return schemaDataType;
    }

    public String getFieldName() {
      return fieldName;
    }

    public ColumnType getColumnType() {
      return columnType;
    }

    public int getScale() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
      if (DataTypes.isDecimal(schemaDataType)) {
        return ((DecimalType) schemaDataType).getScale();
      } else if (schemaDataType == DataTypes.BYTE_ARRAY) {
        return -1;
      }
      throw new UnsupportedOperationException();
    }

    public int getPrecision() {
      if (DataTypes.isDecimal(schemaDataType)) {
        return ((DecimalType) schemaDataType).getPrecision();
      } else if (schemaDataType == DataTypes.BYTE_ARRAY) {
        return -1;
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(fieldName);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
      out.writeByte(schemaDataType.getId());
      out.writeByte(columnType.ordinal());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
      if (DataTypes.isDecimal(schemaDataType)) {
        DecimalType decimalType = (DecimalType) schemaDataType;
        out.writeInt(decimalType.getScale());
        out.writeInt(decimalType.getPrecision());
      } else {
        out.writeInt(-1);
        out.writeInt(-1);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.fieldName = in.readUTF();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1539
      this.schemaDataType = DataTypes.valueOf(in.readByte());
      this.columnType = ColumnType.valueOf(in.readByte());
      int scale = in.readInt();
      int precision = in.readInt();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
      if (DataTypes.isDecimal(this.schemaDataType)) {
        DecimalType decimalType = (DecimalType) this.schemaDataType;
        decimalType.setPrecision(precision);
        decimalType.setScale(scale);
      }
    }
  }

  public class DimensionSpec extends ColumnSpec implements Writable {

    // indicate whether this dimension is in sort column
    private boolean inSortColumns;

    // indicate whether this dimension need to do inverted index
    private boolean doInvertedIndex;

    // indicate the actual postion in blocklet
    private short actualPostion;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3333
    DimensionSpec(ColumnType columnType, CarbonDimension dimension, short actualPostion) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
      super(dimension.getColName(), dimension.getDataType(), columnType);
      this.inSortColumns = dimension.isSortColumn();
      this.doInvertedIndex = dimension.isUseInvertedIndex();
      this.actualPostion = actualPostion;
    }

    public boolean isInSortColumns() {
      return inSortColumns;
    }

    public boolean isDoInvertedIndex() {
      return doInvertedIndex;
    }

    public short getActualPostion() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3333
      return actualPostion;
    }

    @Override
    public void write(DataOutput out) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1400
      super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }
  }

  public class MeasureSpec extends ColumnSpec implements Writable {

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
    MeasureSpec(String fieldName, DataType dataType) {
      super(fieldName, dataType, ColumnType.MEASURE);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
    }
  }
}
