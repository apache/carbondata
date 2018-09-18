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

package org.apache.carbondata.processing.store.writer;

import java.io.IOException;

import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.store.TablePage;

public interface CarbonFactDataWriter {

  /**
   * write a encoded table page
   * @param tablePage
   */
  void writeTablePage(TablePage tablePage) throws CarbonDataWriterException, IOException;

  /**
   * Below method will be used to write the leaf meta data to file
   *
   * @throws CarbonDataWriterException
   */
  void writeFooter() throws CarbonDataWriterException;

  /**
   * Below method will be used to initialise the writer
   */
  void initializeWriter() throws CarbonDataWriterException;

  /**
   * Below method will be used to close the writer
   */
  void closeWriter() throws CarbonDataWriterException;

}
