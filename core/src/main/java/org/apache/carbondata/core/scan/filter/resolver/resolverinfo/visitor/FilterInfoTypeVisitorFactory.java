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

package org.apache.carbondata.core.scan.filter.resolver.resolverinfo.visitor;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.logical.RangeExpression;

public class FilterInfoTypeVisitorFactory {

  /**
   * This factory method will be used in order to get the visitor instance based on the
   * column expression metadata where filters has been applied.
   *
   * @param columnExpression
   * @return
   */
  public static ResolvedFilterInfoVisitorIntf getResolvedFilterInfoVisitor(
      ColumnExpression columnExpression, Expression exp) {
    if (exp instanceof RangeExpression) {
      if (columnExpression.getDimension().getDataType() == DataTypes.DATE) {
        return new RangeDirectDictionaryVisitor();
      } else if (columnExpression.getDimension().hasEncoding(Encoding.IMPLICIT)) {
        return new ImplicitColumnVisitor();
      } else if (columnExpression.getDimension().getDataType() != DataTypes.DATE) {
        return new RangeNoDictionaryTypeVisitor();
      }
    }
    else {
      if (null != columnExpression.getDimension()) {
        if (columnExpression.getDimension().getDataType() == DataTypes.DATE) {
          return new CustomTypeDictionaryVisitor();
        } else if (columnExpression.getDimension().hasEncoding(Encoding.IMPLICIT)) {
          return new ImplicitColumnVisitor();
        } else if (columnExpression.getDimension().getDataType() != DataTypes.DATE) {
          return new NoDictionaryTypeVisitor();
        }
      } else if (columnExpression.getMeasure().isMeasure()) {
        return new MeasureColumnVisitor();
      }
    }
    return null;
  }
}
