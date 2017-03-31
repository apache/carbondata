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

package org.apache.carbondata.processing.store.file;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

public class FileManager implements IFileManagerComposite {
  /**
   * listOfFileData, composite parent which holds the different objects
   */
  protected List<IFileManagerComposite> listOfFileData =
      new ArrayList<IFileManagerComposite>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  protected String fileName;

  @Override public void add(IFileManagerComposite customData) {
    listOfFileData.add(customData);
  }

  @Override public void remove(IFileManagerComposite customData) {
    listOfFileData.remove(customData);

  }

  @Override public IFileManagerComposite get(int i) {
    return listOfFileData.get(i);
  }

  @Override public void setName(String name) {
    this.fileName = name;
  }

  /**
   * Return the size
   */
  public int size() {
    return listOfFileData.size();
  }

}

