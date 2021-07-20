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

package org.apache.carbondata.core.scan.result.vector;

import java.util.BitSet;
import java.util.List;
import java.util.Stack;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.format.Encoding;

public class ColumnVectorInfo implements Comparable<ColumnVectorInfo> {
  public int offset;
  public int size;
  public CarbonColumnVector vector;
  public int vectorOffset;
  public String carbonDataFileWrittenVersion;
  public ProjectionDimension dimension;
  public ProjectionMeasure measure;
  public int ordinal;
  public DirectDictionaryGenerator directDictionaryGenerator;
  public MeasureDataVectorProcessor.MeasureVectorFiller measureVectorFiller;
  public GenericQueryType genericQueryType;
  public int[] invertedIndex;
  public BitSet deletedRows;
  public DecimalConverterFactory.DecimalConverter decimalConverter;
  // Vector stack is used in complex column vectorInfo to store all the children vectors.
  public Stack<CarbonColumnVector> vectorStack = new Stack<>();
  // store the encoding of the column, used while decoding the page for filling the vector
  public List<Encoding> encodings;

  @Override
  public int compareTo(ColumnVectorInfo o) {
    return ordinal - o.ordinal;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (null == obj) {
      return false;
    }

    if (!(obj instanceof ColumnVectorInfo)) {
      return false;
    }

    return ordinal == ((ColumnVectorInfo) obj).ordinal;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (ordinal);
    return result;
  }

  public static Integer getUpdatedPageSizeForChildVector(ColumnVectorInfo vectorInfo,
      int parentPageSize) {
    int newPageSize = 0;
    CarbonColumnVector vector = vectorInfo.vector;
    if (DataTypes.isArrayType(vector.getType())) {
      // If it is array children vector,
      // page size will not same as parent pageSize, so need to re calculate.
      List<Integer> childElementsCountForEachRow =
          ((CarbonColumnVectorImpl) vector.getColumnVector())
              .getNumberOfChildrenElementsInEachRow();
      for (int childElementsCount : childElementsCountForEachRow) {
        newPageSize += childElementsCount;
      }
      return newPageSize;
    } else {
      return parentPageSize;
    }
  }
}
