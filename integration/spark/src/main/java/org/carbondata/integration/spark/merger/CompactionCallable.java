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

package org.carbondata.integration.spark.merger;

import java.util.List;
import java.util.concurrent.Callable;

import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.spark.load.CarbonLoadModel;
import org.carbondata.spark.rdd.Compactor;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.command.Partitioner;

/**
 *
 */
public class CompactionCallable implements Callable<Void> {

  private final String hdfsStoreLocation;
  private final Partitioner partitioner;
  private final String storeLocation;
  private final CarbonTable carbonTable;
  private final String kettleHomePath;
  private final Long cubeCreationTime;
  private final List<LoadMetadataDetails> loadsToMerge;
  private final SQLContext sqlContext;
  private final CarbonLoadModel carbonLoadModel;

  public CompactionCallable(String hdfsStoreLocation, CarbonLoadModel carbonLoadModel,
      Partitioner partitioner, String storeLocation, CarbonTable carbonTable, String kettleHomePath,
      Long cubeCreationTime, List<LoadMetadataDetails> loadsToMerge, SQLContext sqlContext) {

    this.hdfsStoreLocation = hdfsStoreLocation;
    this.carbonLoadModel = carbonLoadModel;
    this.partitioner = partitioner;
    this.storeLocation = storeLocation;
    this.carbonTable = carbonTable;
    this.kettleHomePath = kettleHomePath;
    this.cubeCreationTime = cubeCreationTime;
    this.loadsToMerge = loadsToMerge;
    this.sqlContext = sqlContext;
  }

  @Override public Void call() throws Exception {

    Compactor.triggerCompaction(hdfsStoreLocation, carbonLoadModel, partitioner, storeLocation,
        carbonTable, kettleHomePath, cubeCreationTime, loadsToMerge, sqlContext);
    return null;

  }
}
