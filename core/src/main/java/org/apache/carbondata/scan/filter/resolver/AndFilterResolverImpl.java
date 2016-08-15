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
package org.apache.carbondata.scan.filter.resolver;

import java.util.List;
import java.util.SortedMap;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.expression.BinaryExpression;
import org.apache.carbondata.scan.filter.intf.ExpressionType;

public class AndFilterResolverImpl extends LogicalFilterResolverImpl {

  /**
   *i
   */
  private static final long serialVersionUID = -761688076874662001L;

  public AndFilterResolverImpl(FilterResolverIntf leftEvalutor, FilterResolverIntf rightEvalutor,
      ExpressionType filterExpressionType,BinaryExpression expression) {
    super(leftEvalutor, rightEvalutor, expression);
  }

  @Override public void getStartKey(long[] startKeys,
      SortedMap<Integer, byte[]> noDicStartKeys, List<long[]> startKeyList)
      throws QueryExecutionException {
    leftEvalutor.getStartKey(startKeys, noDicStartKeys, startKeyList);
    rightEvalutor.getStartKey(startKeys, noDicStartKeys, startKeyList);
  }

  @Override public void getEndKey(SegmentProperties segmentProperties,
      AbsoluteTableIdentifier tableIdentifier, long[] endKeys,
      SortedMap<Integer, byte[]> noDicEndKeys, List<long[]> endKeyList)
      throws QueryExecutionException {
    leftEvalutor.getEndKey(segmentProperties, tableIdentifier, endKeys, noDicEndKeys, endKeyList);
    rightEvalutor.getEndKey(segmentProperties, tableIdentifier, endKeys, noDicEndKeys, endKeyList);
  }
}
