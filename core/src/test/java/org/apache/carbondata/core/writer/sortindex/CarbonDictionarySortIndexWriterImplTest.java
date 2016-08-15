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
package org.apache.carbondata.core.writer.sortindex;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnIdentifier;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReader;
import org.apache.carbondata.core.reader.sortindex.CarbonDictionarySortIndexReaderImpl;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * class contains the unit test cases of the dictionary sort index & sort index inverted writing
 */
public class CarbonDictionarySortIndexWriterImplTest {

  private String hdfsStorePath;

  @Before public void setUp() throws Exception {
    hdfsStorePath = "target/carbonStore";
  }

  @After public void tearDown() throws Exception {

    //deleteStorePath();
  }

  /**
   * s
   * Method to test the write of sortIndex file.
   *
   * @throws Exception
   */
  @Test public void write() throws Exception {
    String storePath = hdfsStorePath;
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("testSchema", "carbon", UUID.randomUUID().toString());
    ColumnIdentifier columnIdentifier = new ColumnIdentifier("Name", null, null);

    String metaFolderPath =hdfsStorePath+File.separator+carbonTableIdentifier.getDatabaseName()+File.separator+carbonTableIdentifier.getTableName()+File.separator+"Metadata";
    CarbonUtil.checkAndCreateFolder(metaFolderPath);
    CarbonDictionaryWriter dictionaryWriter = new CarbonDictionaryWriterImpl(hdfsStorePath,
    	       carbonTableIdentifier, columnIdentifier);
    CarbonDictionarySortIndexWriter dictionarySortIndexWriter =
        new CarbonDictionarySortIndexWriterImpl(carbonTableIdentifier, columnIdentifier, storePath);
    List<int[]> indexList = prepareExpectedData();
    int[] data = indexList.get(0);
    for(int i=0;i<data.length;i++) {
    	dictionaryWriter.write(String.valueOf(data[i]));
    }
    dictionaryWriter.close();
    dictionaryWriter.commit();
    
    List<Integer> sortIndex = Arrays.asList(ArrayUtils.toObject(indexList.get(0)));
    List<Integer> invertedSortIndex = Arrays.asList(ArrayUtils.toObject(indexList.get(1)));
    dictionarySortIndexWriter.writeSortIndex(sortIndex);
    dictionarySortIndexWriter.writeInvertedSortIndex(invertedSortIndex);
    dictionarySortIndexWriter.close();
    CarbonDictionarySortIndexReader carbonDictionarySortIndexReader =
        new CarbonDictionarySortIndexReaderImpl(carbonTableIdentifier, columnIdentifier, storePath);
    List<Integer> actualSortIndex = carbonDictionarySortIndexReader.readSortIndex();
    List<Integer> actualInvertedSortIndex = carbonDictionarySortIndexReader.readInvertedSortIndex();
    for (int i = 0; i < actualSortIndex.size(); i++) {
      Assert.assertEquals(sortIndex.get(i), actualSortIndex.get(i));
      Assert.assertEquals(invertedSortIndex.get(i), actualInvertedSortIndex.get(i));
    }

  }

  /**
   * @throws Exception
   */
  @Test public void writingEmptyValue() throws Exception {
    String storePath = hdfsStorePath;
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("testSchema", "carbon", UUID.randomUUID().toString());
    ColumnIdentifier columnIdentifier = new ColumnIdentifier("Name", null, null);

    CarbonDictionarySortIndexWriter dictionarySortIndexWriter =
        new CarbonDictionarySortIndexWriterImpl(carbonTableIdentifier, columnIdentifier, storePath);
    List<Integer> sortIndex = new ArrayList<>();
    List<Integer> invertedSortIndex = new ArrayList<>();
    dictionarySortIndexWriter.writeSortIndex(sortIndex);
    dictionarySortIndexWriter.writeInvertedSortIndex(invertedSortIndex);
    dictionarySortIndexWriter.close();
    CarbonDictionarySortIndexReader carbonDictionarySortIndexReader =
        new CarbonDictionarySortIndexReaderImpl(carbonTableIdentifier, columnIdentifier, storePath);
    List<Integer> actualSortIndex = carbonDictionarySortIndexReader.readSortIndex();
    List<Integer> actualInvertedSortIndex = carbonDictionarySortIndexReader.readInvertedSortIndex();
    for (int i = 0; i < actualSortIndex.size(); i++) {
      Assert.assertEquals(sortIndex.get(i), actualSortIndex.get(i));
      Assert.assertEquals(invertedSortIndex.get(i), actualInvertedSortIndex.get(i));
    }

  }

  private List<int[]> prepareExpectedData() {
    List<int[]> indexList = new ArrayList<>(2);
    int[] sortIndex = { 0, 3, 2, 4, 1 };
    int[] sortIndexInverted = { 0, 2, 4, 1, 2 };
    indexList.add(0, sortIndex);
    indexList.add(1, sortIndexInverted);
    return indexList;
  }

  /**
   * this method will delete the store path
   */
  private void deleteStorePath() {
    FileFactory.FileType fileType = FileFactory.getFileType(this.hdfsStorePath);
    CarbonFile carbonFile = FileFactory.getCarbonFile(this.hdfsStorePath, fileType);
    deleteRecursiveSilent(carbonFile);
  }

  /**
   * this method will delete the folders recursively
   */
  private static void deleteRecursiveSilent(CarbonFile f) {
    if (f.isDirectory()) {
      if (f.listFiles() != null) {
        for (CarbonFile c : f.listFiles()) {
          deleteRecursiveSilent(c);
        }
      }
    }
    if (f.exists() && !f.delete()) {
      return;
    }
  }
}
