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
package org.carbondata.query.filter.resolver;

import java.util.SortedMap;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbonfilterinterface.ExpressionType;

public class AndFilterResolverImpl extends LogicalFilterResolverImpl {

  /**
   *i
   */
  private static final long serialVersionUID = -761688076874662001L;

  public AndFilterResolverImpl(FilterResolverIntf leftEvalutor, FilterResolverIntf rightEvalutor,
      ExpressionType filterExpressionType) {
    super(leftEvalutor, rightEvalutor, filterExpressionType);
  }

  @Override public void getStartKey(SegmentProperties segmentProperties, long[] startKeys,
      SortedMap<Integer, byte[]> noDicStartKeys) {
    leftEvalutor.getStartKey(segmentProperties, startKeys, noDicStartKeys);
    rightEvalutor.getStartKey(segmentProperties, startKeys, noDicStartKeys);
  }

  @Override public void getEndKey(SegmentProperties segmentProperties,
      AbsoluteTableIdentifier tableIdentifier, long[] endKeys,
      SortedMap<Integer, byte[]> noDicEndKeys) throws QueryExecutionException {
    leftEvalutor.getEndKey(segmentProperties, tableIdentifier, endKeys, noDicEndKeys);
    rightEvalutor.getEndKey(segmentProperties, tableIdentifier, endKeys, noDicEndKeys);
  }
}
