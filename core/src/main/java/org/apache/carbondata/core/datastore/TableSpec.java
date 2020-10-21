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
    numSimpleDimensions = 0;
    for (CarbonDimension dimension : dimensions) {
      if (!dimension.isComplex()) {
        numSimpleDimensions++;
      }
    }
    dimensionSpec = new DimensionSpec[dimensions.size()];
    measureSpec = new MeasureSpec[measures.size()];
    noDictionaryDimensionSpec = new ArrayList<>();
    addDimensions(dimensions);
    addMeasures(measures);
  }

  private void addDimensions(List<CarbonDimension> dimensions) {
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
        dimensionSpec[dimIndex++] = spec;
        noDictionaryDimensionSpec.add(spec);
        noSortNoDictDimSpec.add(spec);
      } else if (dimension.getDataType() == DataTypes.TIMESTAMP) {
        spec = new DimensionSpec(ColumnType.PLAIN_VALUE, dimension, noDictActualPosition++);
        dimensionSpec[dimIndex++] = spec;
        noDictionaryDimensionSpec.add(spec);
        if (dimension.isSortColumn()) {
          noDictSortDimSpec.add(spec);
        } else {
          noSortNoDictDimSpec.add(spec);
        }
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
      dictDimActualPosition[i] = dictSortDimSpec.get(i).getActualPosition();
    }
    for (int i = 0; i < noDictDimActualPosition.length; i++) {
      noDictDimActualPosition[i] = noDictSortDimSpec.get(i).getActualPosition();
    }
    isUpdateNoDictDims = !noDictSortDimSpec.equals(noDictionaryDimensionSpec);
    isUpdateDictDim = !dictSortDimSpec.equals(dictDimensionSpec);
  }

  private void addMeasures(List<CarbonMeasure> measures) {
    for (int i = 0; i < measures.size(); i++) {
      CarbonMeasure measure = measures.get(i);
      measureSpec[i] = new MeasureSpec(measure.getColName(), measure.getDataType());
    }
  }

  public int[] getDictDimActualPosition() {
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
    List<DimensionSpec> noDictAndComplexDimensions = new ArrayList<>();
    for (DimensionSpec spec : dimensionSpec) {
      if (spec.getColumnType() == ColumnType.PLAIN_VALUE
          || spec.getColumnType() == ColumnType.COMPLEX_PRIMITIVE
          || spec.getColumnType() == ColumnType.COMPLEX) {
        noDictAndComplexDimensions.add(spec);
      }
    }
    return noDictAndComplexDimensions.toArray(new DimensionSpec[noDictAndComplexDimensions.size()]);
  }

  public DimensionSpec getDimensionSpec(int dimensionIndex) {
    return dimensionSpec[dimensionIndex];
  }

  public List<DimensionSpec> getNoDictionaryDimensionSpec() {
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
      out.writeByte(schemaDataType.getId());
      out.writeByte(columnType.ordinal());
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
      this.schemaDataType = DataTypes.valueOf(in.readByte());
      this.columnType = ColumnType.valueOf(in.readByte());
      int scale = in.readInt();
      int precision = in.readInt();
      if (DataTypes.isDecimal(this.schemaDataType)) {
        DecimalType decimalType = (DecimalType) this.schemaDataType;
        decimalType.setPrecision(precision);
        decimalType.setScale(scale);
      }
    }
  }

  public static class DimensionSpec extends ColumnSpec implements Writable {

    // indicate whether this dimension is in sort column
    private boolean inSortColumns;

    // indicate whether this dimension need to do inverted index
    private boolean doInvertedIndex;

    // indicate the actual position in blocklet
    private short actualPosition;
    DimensionSpec(ColumnType columnType, CarbonDimension dimension, short actualPosition) {
      super(dimension.getColName(), dimension.getDataType(), columnType);
      this.inSortColumns = dimension.isSortColumn();
      this.doInvertedIndex = dimension.isUseInvertedIndex();
      this.actualPosition = actualPosition;
    }

    DimensionSpec(String columnName, DataType dataType, ColumnType columnType) {
      super(columnName, dataType, columnType);
    }

    public static DimensionSpec newInstance(String columnName, DataType dataType,
        ColumnType columnType) {
      return new DimensionSpec(columnName, dataType, columnType);
    }

    public boolean isInSortColumns() {
      return inSortColumns;
    }

    public boolean isDoInvertedIndex() {
      return doInvertedIndex;
    }

    public short getActualPosition() {
      return actualPosition;
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

  public static class MeasureSpec extends ColumnSpec implements Writable {

    MeasureSpec(String fieldName, DataType dataType) {
      super(fieldName, dataType, ColumnType.MEASURE);
    }

    public static MeasureSpec newInstance(String columnName, DataType dataType) {
      return new MeasureSpec(columnName, dataType);
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