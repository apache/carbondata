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

package org.apache.carbondata.processing.mdkeygen.file;

import org.apache.carbondata.core.writer.HierarchyValueWriterForCSV;

public class FileData extends FileManager {

  /**
   * Store Path
   */
  private String storePath;

  /**
   * hierarchyValueWriter
   */
  private HierarchyValueWriterForCSV hierarchyValueWriter;

  public FileData(String fileName, String storePath) {
    this.fileName = fileName;
    this.storePath = storePath;
  }

  /**
   * @return Returns the carbonDataFileTempPath.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @return Returns the storePath.
   */
  public String getStorePath() {
    return storePath;
  }

  /**
   * get Hierarchy Value writer
   *
   * @return
   */
  public HierarchyValueWriterForCSV getHierarchyValueWriter() {
    return hierarchyValueWriter;
  }

  /**
   * Set Hierarchy Value Writer.
   *
   * @param hierarchyValueWriter
   */
  public void setHierarchyValueWriter(HierarchyValueWriterForCSV hierarchyValueWriter) {
    this.hierarchyValueWriter = hierarchyValueWriter;
  }

}

