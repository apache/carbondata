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

package org.apache.carbondata.store;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

public class TestUtil {

  /**
   * verify whether the file exists
   * if delete the file success or file not exists, then return true; otherwise return false
   *
   * @return boolean
   */
  public static boolean cleanMdtFile() {
    String fileName = CarbonProperties.getInstance().getSystemFolderLocation()
        + CarbonCommonConstants.FILE_SEPARATOR + "datamap.mdtfile";
    try {
      if (FileFactory.isFileExist(fileName)) {
        File file = new File(fileName);
        file.delete();
        return true;
      } else {
        return true;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  /**
   * verify whether the mdt file exists
   * if the file exists, then return true; otherwise return false
   *
   * @return boolean
   */
  public static boolean verifyMdtFile() {
    String fileName = CarbonProperties.getInstance().getSystemFolderLocation()
        + CarbonCommonConstants.FILE_SEPARATOR + "datamap.mdtfile";
    try {
      if (FileFactory.isFileExist(fileName)) {
        return true;
      }
      return false;
    } catch (IOException e) {
      throw new RuntimeException("IO exception:", e);
    }
  }

}
