/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.unibi.molap.constants;

import com.huawei.unibi.molap.util.MolapProperties;

public final class DataSetUtil {
  public static final String DATA_SET_LOCATION =
      MolapProperties.getInstance().getProperty("spark.dataset.location", "../datasets/");

  public static final String DP_LOCATION =
      MolapProperties.getInstance().getProperty("spark.dp.location", "../datapipelines/");

  public static final String DATA_SOURCE_LOCATION = MolapProperties.getInstance()
      .getProperty("spark.sqlconnections.location",
          "../unibi-solutions/system/dbconnection/sqlconnections.xml");

  private DataSetUtil() {
  }

}
