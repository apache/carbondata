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

package org.carbondata.processing.globalsurrogategenerator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDef.Cube;
import org.carbondata.core.carbon.CarbonDef.CubeDimension;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.util.CarbonSchemaParser;

public class GlobalSurrogateGenerator {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(GlobalSurrogateGenerator.class.getName());
  private GlobalSurrogateGeneratorInfo generatorInfo;
  /**
   * carbon schema object
   */
  private Schema schema;

  /**
   * carbon cube object
   */
  private Cube cube;

  public GlobalSurrogateGenerator(GlobalSurrogateGeneratorInfo generatorInfo) {
    this.generatorInfo = generatorInfo;
    schema = generatorInfo.getSchema();
    cube = CarbonSchemaParser.getMondrianCube(schema, generatorInfo.getCubeName());
  }

  public void generateGlobalSurrogates(int currentRestructNumber) {
    String hdfsLocation = generatorInfo.getStoreLocation();
    LOGGER.info("HDFS Location: " + hdfsLocation);
    int numberOfPartition = generatorInfo.getNumberOfPartition();
    String[][] partitionLocation = new String[numberOfPartition][];
    for (int i = 0; i < numberOfPartition; i++) {
      StringBuffer storeLocation = new StringBuffer();
      storeLocation.append(hdfsLocation);
      storeLocation.append('/');
      storeLocation.append(schema.name);
      storeLocation.append('_');
      storeLocation.append(i);
      storeLocation.append('/');
      storeLocation.append(cube.name);
      storeLocation.append('_');
      storeLocation.append(i);

      int restrctFolderCount = currentRestructNumber;
      if (restrctFolderCount == -1) {
        restrctFolderCount = 0;
      }
      storeLocation.append('/');
      storeLocation.append(CarbonCommonConstants.RESTRUCTRE_FOLDER);
      storeLocation.append(restrctFolderCount);
      storeLocation.append('/');
      storeLocation.append(generatorInfo.getTableName());

      partitionLocation[i] = CarbonUtil
          .getSlices(storeLocation.toString(), FileFactory.getFileType(storeLocation.toString()));
    }
    ExecutorService writerExecutorService = Executors.newFixedThreadPool(20);
    LevelGlobalSurrogateGeneratorThread generatorThread = null;
    CubeDimension[] cubeDims = generatorInfo.getCubeDimensions();
    for (int i = 0; i < cubeDims.length; i++) {
      generatorThread =
          new LevelGlobalSurrogateGeneratorThread(partitionLocation, cubeDims[i], schema,
              generatorInfo.getTableName(), generatorInfo.getPartiontionColumnName());
      writerExecutorService.submit(generatorThread);
    }
    writerExecutorService.shutdown();
    try {
      writerExecutorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      LOGGER.error(e, e.getMessage());
    }
  }
}
