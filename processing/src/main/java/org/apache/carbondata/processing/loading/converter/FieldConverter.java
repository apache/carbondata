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

package org.apache.carbondata.processing.loading.converter;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

/**
 * This interface converts/transforms the column field.
 */
public interface FieldConverter {

  /**
   * It converts the column field and updates the data in same location/index in row.
   * @param row
   * @return the status whether it could be loaded or not, usually when record is added
   * to bad records then it returns false.
   * @throws CarbonDataLoadingException
   */
  void convert(CarbonRow row, BadRecordLogHolder logHolder) throws CarbonDataLoadingException;

  /**
   * It convert the literal value to carbon internal value
   */
  Object convert(Object value, BadRecordLogHolder logHolder) throws RuntimeException;

  /**
   * This method clears all the dictionary caches being acquired.
   */
  void clear();
}
