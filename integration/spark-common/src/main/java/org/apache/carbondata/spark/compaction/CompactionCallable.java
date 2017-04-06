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

package org.apache.carbondata.spark.compaction;

import java.util.concurrent.Callable;

import org.apache.carbondata.spark.rdd.Compactor;

import org.apache.spark.sql.execution.command.CompactionCallableModel;

/**
 * Callable class which is used to trigger the compaction in a separate callable.
 */
public class CompactionCallable implements Callable<Void> {

  private final CompactionCallableModel compactionCallableModel;

  public CompactionCallable(CompactionCallableModel compactionCallableModel) {

    this.compactionCallableModel = compactionCallableModel;
  }

  @Override public Void call() throws Exception {

    Compactor.triggerCompaction(compactionCallableModel);
    return null;

  }
}
