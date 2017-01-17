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

package org.apache.carbondata.hadoop.internal.index;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * An index is associate with one segment, it is used for filtering on files in the segment.
 */
public interface Index {

  /**
   * Index name
   * @return index name
   */
  String getName();

  /**
   * Used to filter blocks based on filter
   * @param job job
   * @param filter filter
   * @return filtered block
   * @throws IOException
   */
  List<Block> filter(JobContext job, FilterResolverIntf filter) throws IOException;

}
