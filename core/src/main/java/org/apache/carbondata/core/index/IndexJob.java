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

package org.apache.carbondata.core.index;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.indexstore.BlockletIndexWrapper;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Distributable index job to execute the #IndexInputFormat in cluster. it prunes the
 * indexes distributably and returns the final blocklet list
 */
public interface IndexJob extends Serializable {

  void execute(CarbonTable carbonTable, FileInputFormat<Void, BlockletIndexWrapper> format);

  List<ExtendedBlocklet> execute(IndexInputFormat indexInputFormat);

  Long executeCountJob(IndexInputFormat indexInputFormat);

}
