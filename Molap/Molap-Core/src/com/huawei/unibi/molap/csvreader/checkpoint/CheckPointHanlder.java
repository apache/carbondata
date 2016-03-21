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

package com.huawei.unibi.molap.csvreader.checkpoint;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapProperties;

import java.util.HashMap;
import java.util.Map;

public final class CheckPointHanlder {
  /**
   * IS_CHECK_POINT_NEEDED
   */
  public static final boolean IS_CHECK_POINT_NEEDED = Boolean.valueOf(MolapProperties.getInstance()
      .getProperty(MolapCommonConstants.MOLAP_DATALOAD_CHECKPOINT,
          MolapCommonConstants.MOLAP_DATALOAD_CHECKPOINT_DEFAULTVALUE));
  /**
   * dummyCheckPoint
   */
  private static final CheckPointInterface DUMMYCHECKPOINT = new DummyCheckPointHandler();
  /**
   * check point cache
   */
  private static Map<String, CheckPointInterface> checkpoints =
      new HashMap<String, CheckPointInterface>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

  private CheckPointHanlder() {

  }

  /**
   * Below method will be used to initialise the check point
   *
   * @param checkPointType
   * @return check point
   */
  public static void initializeCheckpoint(String transName, CheckPointType checkPointType,
      String checkPointFileLocation, String checkPointFileName) {
    // if check is not true
    if (IS_CHECK_POINT_NEEDED) {
      // if present in case return else create and return
      CheckPointInterface checkPoint = checkpoints.get(transName);
      if (null == checkPoint && checkPointType.equals(CheckPointType.CSV)) {
        checkPoint = new CSVCheckPointHandler(checkPointFileLocation, checkPointFileName);
        checkpoints.put(transName, checkPoint);
      }
    }
  }

  /**
   * Below method will be used to get the check point from cache
   *
   * @param transPath
   * @return CheckPoint
   */
  public static CheckPointInterface getCheckpoint(String transPath) {
    if (!IS_CHECK_POINT_NEEDED) {
      return DUMMYCHECKPOINT;
    }
    CheckPointInterface checkPoint = checkpoints.get(transPath);
    return null != checkPoint ? checkPoint : DUMMYCHECKPOINT;
  }

  /**
   * Below method will be used to get the dummy check point
   *
   * @return dummy check point
   */

  public static CheckPointInterface getDummyCheckPoint() {
    return DUMMYCHECKPOINT;
  }
}
