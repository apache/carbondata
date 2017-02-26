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

/**
 * Constants for V3 data format
 */
public interface CarbonV3DataFormatConstants {

  /**
   * number of page per blocklet column
   */
  String NUMBER_OF_PAGE_IN_BLOCKLET_COLUMN = "carbon.number.of.page.in.blocklet.column";

  /**
   * number of page per blocklet column default value
   */
  String NUMBER_OF_PAGE_IN_BLOCKLET_COLUMN_DEFAULT_VALUE = "10";

  /**
   * number of page per blocklet column max value
   */
  short NUMBER_OF_PAGE_IN_BLOCKLET_COLUMN_MAX = 20;

  /**
   * number of page per blocklet column min value
   */
  short NUMBER_OF_PAGE_IN_BLOCKLET_COLUMN_MIN = 1;

  /**
   * number of column to be read in one IO in query
   */
  String NUMBER_OF_COLUMN_TO_READ_IN_IO = "number.of.column.to.read.in.io";

  /**
   * number of column to be read in one IO in query default value
   */
  String NUMBER_OF_COLUMN_TO_READ_IN_IO_DEFAULTVALUE = "10";

  /**
   * number of column to be read in one IO in query max value
   */
  short NUMBER_OF_COLUMN_TO_READ_IN_IO_MAX = 20;

  /**
   * number of column to be read in one IO in query min value
   */
  short NUMBER_OF_COLUMN_TO_READ_IN_IO_MIN = 1;

  /**
   * number of rows per blocklet column page
   */
  String NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE = "number.of.rows.per.blocklet.column.page";

  /**
   * number of rows per blocklet column page default value
   */
  String NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT = "32000";

  /**
   * number of rows per blocklet column page max value
   */
  short NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_MAX = 32000;

  /**
   * number of rows per blocklet column page min value
   */
  short NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_MIN = 8000;

}
