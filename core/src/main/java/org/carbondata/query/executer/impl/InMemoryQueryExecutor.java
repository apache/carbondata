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

package org.carbondata.query.executer.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.query.executer.AbstractCarbonExecutor;
import org.carbondata.query.executer.CarbonQueryExecutorModel;
import org.carbondata.query.holders.CarbonResultHolder;
import org.carbondata.query.queryinterface.filter.CarbonFilterInfo;

//import mondrian.carbon.SqlStatement;
//import mondrian.carbon.SqlStatement.Type;
//import mondrian.carbon.ResourceLimitExceededException;

/**
 * This class is the concrete implementation for CARBON Query Execution.
 * It is responsible for handling the query execution.
 */
public class InMemoryQueryExecutor extends AbstractCarbonExecutor implements RowCounterListner {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(InMemoryQueryExecutor.class.getName());

  @Override public void execute(CarbonQueryExecutorModel queryModel)
      throws IOException, KeyGenException {
    LOGGER.error("UNSUPPORT Operation");
  }

  @Override
  public void executeHierarichies(String hName, final int[] dims, List<Dimension> dimNames,
      Map<Dimension, CarbonFilterInfo> constraints, CarbonResultHolder hIterator)
      throws IOException, KeyGenException {
    LOGGER.error("UNSUPPORT Operation");

  }

  @Override public void executeDimensionCount(Dimension dimension, CarbonResultHolder hIterator)
      throws IOException {
    LOGGER.error("UNSUPPORT Operation");

  }

  @Override public void executeAggTableCount(String table, CarbonResultHolder hIterator)
      throws IOException {
    LOGGER.error("UNSUPPORT Operation");

  }

  @Override public long executeTableCount(String table) throws IOException {
    LOGGER.error("UNSUPPORT Operation");
    return 0;
  }

  @Override public void executeDimension(String hName, Dimension dim, final int[] dims,
      Map<Dimension, CarbonFilterInfo> constraints, CarbonResultHolder hIterator)
      throws IOException {
    LOGGER.error("UNSUPPORT Operation");

  }

  @Override public void interruptExecutor() {
    LOGGER.error("UNSUPPORT Operation");

  }

  @Override public void rowLimitExceeded() throws Exception {
    LOGGER.error("UNSUPPORT Operation");

  }

  @Override protected Long getMaxValue(Dimension dim) throws IOException {
    LOGGER.error("UNSUPPORT Operation");
    return 0L;
  }

  @Override public long[] getSurrogates(List<String> dimMem, Dimension dimName) throws IOException {
    LOGGER.error("UNSUPPORT Operation");
    return new long[0];
  }
}
