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

import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.filter.resolver.metadata.FilterResolverMetadata;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

public interface ResolvedFilterInfoVisitorIntf {

  /**
   * Visitor pattern is been used in this scenario inorder to populate the
   * dimColResolvedFilterInfo visitable object with filter member values based
   * on the visitor type, currently there 3 types of visitors custom,direct
   * and no dictionary, all types of visitor populate the visitable instance
   * as per its buisness logic which is different for all the visitors.
   *
   * @param visitableObj
   * @param metadata
   * @throws QueryExecutionException
   */
  void populateFilterResolvedInfo(DimColumnResolvedFilterInfo visitableObj,
      FilterResolverMetadata metadata) throws QueryExecutionException;
}
