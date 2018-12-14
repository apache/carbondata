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
package org.apache.carbondata.core.constants;

import org.apache.carbondata.core.util.Property;

/**
 * Constants for V3 data format
 */
public interface CarbonV3DataFormatConstants {

  Property BLOCKLET_SIZE_IN_MB = Property.buildIntProperty()
      .key("carbon.blockletgroup.size.in.mb")
      .defaultValue(64)
      .minValue(1)
      .dynamicConfigurable(true)
      .doc("each blocklet group size in mb")
      .build();

  Property NUMBER_OF_COLUMN_TO_READ_IN_IO = Property.buildIntProperty()
      .key("number.of.column.to.read.in.io")
      .defaultValue(10)
      .minValue(1)
      .maxValue(20)
      .doc("number of column to be read in one IO in query")
      .build();

  /**
   * number of rows per blocklet column page default value
   */
  short NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT = 32000;

}
