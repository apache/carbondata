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
package org.apache.carbondata.scan.executor;

import java.util.List;

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.scan.executor.impl.DetailQueryExecutor;
import org.apache.carbondata.scan.executor.impl.VectorDetailQueryExecutor;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.model.QueryMeasure;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.scan.result.vector.impl.CarbonColumnVectorImpl;

/**
 * Factory class to get the query executor from RDD
 * This will return the executor based on query type
 */
public class QueryExecutorFactory {

  public static QueryExecutor getQueryExecutor(QueryModel queryModel) {
    if (queryModel.isVectorReader()) {
      return new VectorDetailQueryExecutor();
    } else {
      return new DetailQueryExecutor();
    }
  }
}
