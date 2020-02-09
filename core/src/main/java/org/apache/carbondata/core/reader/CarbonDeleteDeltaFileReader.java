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

package org.apache.carbondata.core.reader;

import java.io.IOException;

import org.apache.carbondata.core.mutate.DeleteDeltaBlockDetails;

/**
 * CarbonDeleteDeltaFileReader contains all methods to read delete delta file data
 */
public interface CarbonDeleteDeltaFileReader {

  /**
   * This method will be used to read complete delete delta file.
   * scenario:
   * Whenever a query is executed then read the delete delta file
   * and exclude the deleted data.
   *
   * @return All deleted records for specified block
   * @throws IOException if an I/O error occurs
   */
  String read() throws IOException;

  DeleteDeltaBlockDetails readJson();

}
