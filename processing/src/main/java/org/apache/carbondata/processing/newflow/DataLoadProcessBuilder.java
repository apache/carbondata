/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.processing.newflow;

import java.util.Iterator;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.newflow.steps.DataConverterProcessorStepImpl;
import org.apache.carbondata.processing.newflow.steps.DataWriterProcessorStepImpl;
import org.apache.carbondata.processing.newflow.steps.InputProcessorStepImpl;
import org.apache.carbondata.processing.newflow.steps.SortProcessorStepImpl;

/**
 * It builds the pipe line of steps for loading data to carbon.
 */
public final class DataLoadProcessBuilder {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataLoadProcessBuilder.class.getName());

  public AbstractDataLoadProcessorStep build(CarbonLoadModel loadModel, String storeLocation,
      Iterator[] inputIterators) throws Exception {
    CarbonDataLoadConfiguration configuration =
        NewFlowDataLoadUtil.createConfiguration(loadModel, storeLocation);
    // 1. Reads the data input iterators and parses the data.
    AbstractDataLoadProcessorStep inputProcessorStep =
        new InputProcessorStepImpl(configuration, inputIterators);
    // 2. Converts the data like dictionary or non dictionary or complex objects depends on
    // data types and configurations.
    AbstractDataLoadProcessorStep converterProcessorStep =
        new DataConverterProcessorStepImpl(configuration, inputProcessorStep);
    // 3. Sorts the data which are part of key (all dimensions except complex types)
    AbstractDataLoadProcessorStep sortProcessorStep =
        new SortProcessorStepImpl(configuration, converterProcessorStep);
    // 4. Writes the sorted data in carbondata format.
    AbstractDataLoadProcessorStep writerProcessorStep =
        new DataWriterProcessorStepImpl(configuration, sortProcessorStep);
    return writerProcessorStep;
  }

}
