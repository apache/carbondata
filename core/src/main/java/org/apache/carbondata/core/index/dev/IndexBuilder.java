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

package org.apache.carbondata.core.index.dev;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;

/**
 * IndexBuilder is used to implement REFRESH INDEX command, it reads all existing
 * data in main table and load them into the Index. All existing index data will be deleted
 * if there are existing data in the index.
 */
@InterfaceAudience.Developer("Index")
public interface IndexBuilder {
  void initialize() throws IOException;

  void addRow(int blockletId, int pageId, int rowId, Object[] values) throws IOException;

  void finish() throws IOException;

  void close() throws IOException;

  /**
   * whether create index on internal carbon bytes (such as dictionary encoded) or original value
   */
  boolean isIndexForCarbonRawBytes();
}
