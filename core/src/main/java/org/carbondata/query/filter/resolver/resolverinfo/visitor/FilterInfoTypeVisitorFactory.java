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
package org.carbondata.query.filter.resolver.resolverinfo.visitor;

import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.query.expression.ColumnExpression;

public class FilterInfoTypeVisitorFactory {

  /**
   * This factory method will be used in order to get the visitor instance based on the
   * column expression metadata where filters has been applied.
   *
   * @param columnExpression
   * @return
   */
  public static ResolvedFilterInfoVisitorIntf getResolvedFilterInfoVisitor(
      ColumnExpression columnExpression) {
    if (columnExpression.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      return new CustomTypeDictionaryVisitor();
    } else if (!columnExpression.getDimension().hasEncoding(Encoding.DICTIONARY)) {
      return new NoDictionaryTypeVisitor();
    } else if (columnExpression.getDimension().hasEncoding(Encoding.DICTIONARY)) {
      return new DictionaryColumnVisitor();
    }

    return null;
  }
}
