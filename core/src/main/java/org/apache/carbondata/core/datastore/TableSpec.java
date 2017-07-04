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

import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

public class TableSpec {

  // contains name and type for each dimension
  private DimensionSpec dimensionSpec;
  // contains name and type for each measure
  private MeasureSpec measureSpec;

  public TableSpec(List<CarbonDimension> dimensions, List<CarbonMeasure> measures) {
    dimensionSpec = new DimensionSpec(dimensions);
    measureSpec = new MeasureSpec(measures);
  }

  public DimensionSpec getDimensionSpec() {
    return dimensionSpec;
  }

  public MeasureSpec getMeasureSpec() {
    return measureSpec;
  }

  public class DimensionSpec {

    // field name of each dimension, in schema order
    private String[] fieldName;

    // encoding type of each dimension, in schema order
    private DimensionType[] types;

    // number of simple dimensions
    private int numSimpleDimensions;

    // number of complex dimensions
    private int numComplexDimensions;

    // number of dimensions after complex column expansion
    private int numDimensionExpanded;

    DimensionSpec(List<CarbonDimension> dimensions) {
      // first calculate total number of columnar field considering column group and complex column
      numDimensionExpanded = 0;
      numSimpleDimensions = 0;
      numComplexDimensions = 0;
      boolean inColumnGroup = false;
      for (CarbonDimension dimension : dimensions) {
        if (dimension.isColumnar()) {
          if (inColumnGroup) {
            inColumnGroup = false;
          }
          if (dimension.isComplex()) {
            numDimensionExpanded += dimension.getNumDimensionsExpanded();
            numComplexDimensions++;
          } else {
            numDimensionExpanded++;
            numSimpleDimensions++;
          }
        } else {
          // column group
          if (!inColumnGroup) {
            inColumnGroup = true;
            numDimensionExpanded++;
            numSimpleDimensions++;
          }
        }
      }

      // then extract dimension name and type for each column
      fieldName = new String[numDimensionExpanded];
      types = new DimensionType[numDimensionExpanded];
      inColumnGroup = false;
      int index = 0;
      for (CarbonDimension dimension : dimensions) {
        if (dimension.isColumnar()) {
          if (inColumnGroup) {
            inColumnGroup = false;
          }
          if (dimension.isComplex()) {
            int count = addDimension(index, dimension);
            index += count;
          } else if (dimension.getDataType() == DataType.TIMESTAMP ||
                     dimension.getDataType() == DataType.DATE) {
            addSimpleDimension(index++, dimension.getColName(), DimensionType.DIRECT_DICTIONARY);
          } else if (dimension.isGlobalDictionaryEncoding()) {
            addSimpleDimension(index++, dimension.getColName(), DimensionType.GLOBAL_DICTIONARY);
          } else {
            addSimpleDimension(index++, dimension.getColName(), DimensionType.PLAIN_VALUE);
          }
        } else {
          // column group
          if (!inColumnGroup) {
            addSimpleDimension(index++, dimension.getColName(), DimensionType.COLUMN_GROUP);
            inColumnGroup = true;
          }
        }
      }
    }

    private void addSimpleDimension(int index, String name, DimensionType type) {
      fieldName[index] = name;
      types[index] = type;
    }

    // add dimension and return number of columns added
    private int addDimension(int index, CarbonDimension dimension) {
      switch (dimension.getDataType()) {
        case ARRAY:
          addSimpleDimension(index, dimension.getColName() + ".offset", DimensionType.COMPLEX);
          List<CarbonDimension> arrayChildren = dimension.getListOfChildDimensions();
          int count = 1;
          for (CarbonDimension child : arrayChildren) {
            count += addDimension(index + count, child);
          }
          return count;
        case STRUCT:
          addSimpleDimension(index, dimension.getColName() + ".empty", DimensionType.COMPLEX);
          List<CarbonDimension> structChildren = dimension.getListOfChildDimensions();
          count = 1;
          for (CarbonDimension child : structChildren) {
            count += addDimension(index + count, child);
          }
          return count;
        case TIMESTAMP:
        case DATE:
          addSimpleDimension(index, dimension.getColName(), DimensionType.DIRECT_DICTIONARY);
          return 1;
        default:
          addSimpleDimension(index, dimension.getColName(),
              dimension.isGlobalDictionaryEncoding() ?
                  DimensionType.GLOBAL_DICTIONARY : DimensionType.PLAIN_VALUE);
          return 1;
      }
    }


    /**
     * return the dimension type of index'th dimension. index is from 0 to numDimensions
     */
    public DimensionType getType(int index) {
      assert (index >= 0 && index < types.length);
      return types[index];
    }

    /**
     * return number of dimensions
     */
    public int getNumSimpleDimensions() {
      return numSimpleDimensions;
    }

    public int getNumComplexDimensions() {
      return numComplexDimensions;
    }

    public int getNumExpandedDimensions() {
      return numDimensionExpanded;
    }

  }

  public class MeasureSpec {

    // field name of each measure, in schema order
    private String[] fieldName;

    // data type of each measure, in schema order
    private DataType[] types;

    MeasureSpec(List<CarbonMeasure> measures) {
      fieldName = new String[measures.size()];
      types = new DataType[measures.size()];
      int i = 0;
      for (CarbonMeasure measure: measures) {
        add(i++, measure.getColName(), measure.getDataType());
      }
    }

    private void add(int index, String name, DataType type) {
      fieldName[index] = name;
      types[index] = type;
    }

    /**
     * return the data type of index'th measure. index is from 0 to numMeasures
     */
    public DataType getType(int index) {
      assert (index >= 0 && index < types.length);
      return types[index];
    }

    /**
     * return number of measures
     */
    public int getNumMeasures() {
      return types.length;
    }
  }
}