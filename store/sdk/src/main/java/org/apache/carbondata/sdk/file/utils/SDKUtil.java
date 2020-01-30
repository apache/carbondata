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

package org.apache.carbondata.sdk.file.utils;

import java.util.ArrayList;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.conf.Configuration;

public class SDKUtil {
  public static ArrayList listFiles(String sourceImageFolder, final String suf) throws Exception {
    return listFiles(sourceImageFolder, suf, new Configuration(true));
  }

  public static ArrayList listFiles(String sourceImageFolder,
                                    final String suf, Configuration conf) throws Exception {
    final String sufImageFinal = suf;
    ArrayList result = new ArrayList();
    CarbonFile[] fileList = FileFactory.getCarbonFile(sourceImageFolder, conf).listFiles();
    for (int i = 0; i < fileList.length; i++) {
      if (fileList[i].isDirectory()) {
        result.addAll(listFiles(fileList[i].getCanonicalPath(), sufImageFinal, conf));
      } else if (fileList[i].getCanonicalPath().endsWith(sufImageFinal)) {
        result.add(fileList[i].getCanonicalPath());
      }
    }
    return result;
  }

}
