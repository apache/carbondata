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
import java.util.List;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.hadoop.conf.Configuration;

public class SDKUtil {
  public static ArrayList listFiles(String sourceImageFolder, final String suf) {
    return listFiles(sourceImageFolder, suf, new Configuration(true));
  }

  public static ArrayList listFiles(String sourceImageFolder,
                                    final String suf, Configuration conf) {
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

  public static Object[] getSplitList(String path, String suf,
                                      int numOfSplit, Configuration conf) {
    List fileList = listFiles(path, suf, conf);
    List splitList = new ArrayList<List>();
    if (numOfSplit < fileList.size()) {
      // If maxSplits is less than the no. of files
      // Split the reader into maxSplits splits with each
      // element containing >= 1 CarbonRecordReader objects
      float filesPerSplit = (float) fileList.size() / numOfSplit;
      for (int i = 0; i < numOfSplit; ++i) {
        splitList.add(fileList.subList(
            (int) Math.ceil(i * filesPerSplit),
            (int) Math.ceil(((i + 1) * filesPerSplit))));
      }
    } else {
      // If maxSplits is greater than the no. of files
      // Split the reader into <num_files> splits with each
      // element contains exactly 1 CarbonRecordReader object
      for (int i = 0; i < fileList.size(); ++i) {
        splitList.add((fileList.subList(i, i + 1)));
      }
    }
    return splitList.toArray();
  }

  public static Object[] getSplitList(String path, String suf,
                                      int numOfSplit) {
    return getSplitList(path, suf, numOfSplit, new Configuration());
  }

}
