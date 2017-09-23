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
package org.apache.carbondata.core.reader.sortindex;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonTestUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.writer.CarbonDictionaryWriter;
import org.apache.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriter;
import org.apache.carbondata.core.writer.sortindex.CarbonDictionarySortIndexWriterImpl;
import org.apache.commons.lang.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class CarbonDictionarySortIndexReaderImplTest {
  private String storePath;

  @Before public void setUp() throws Exception {
      storePath = "target/carbonStore";
  }

  @After public void tearDown() throws Exception {

    deleteStorePath();
  }

  /**
   * Test to read the data from dictionary sort index file
   *
   * @throws Exception
   */
  @Test public void read() throws Exception {
    deleteStorePath();
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("testSchema", "carbon",
    		UUID.randomUUID().toString());
    ColumnIdentifier columnIdentifier = new ColumnIdentifier("Name", null, null);
    DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(carbonTableIdentifier, columnIdentifier, columnIdentifier.getDataType(),
        CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier,
            CarbonTestUtil.configuration));
    CarbonDictionaryWriter dictionaryWriter = new CarbonDictionaryWriterImpl(storePath,
       carbonTableIdentifier, dictionaryColumnUniqueIdentifier, CarbonTestUtil.configuration);
    String metaFolderPath =storePath+File.separator+carbonTableIdentifier.getDatabaseName()+File.separator+carbonTableIdentifier.getTableName()+File.separator+"Metadata";
    CarbonUtil.checkAndCreateFolder(CarbonTestUtil.configuration, metaFolderPath);
    CarbonDictionarySortIndexWriter dictionarySortIndexWriter =
        new CarbonDictionarySortIndexWriterImpl(carbonTableIdentifier,
            dictionaryColumnUniqueIdentifier, storePath, CarbonTestUtil.configuration);
    List<int[]> expectedData = prepareExpectedData();
    int[] data = expectedData.get(0);
    for(int i=0;i<data.length;i++) {
    	dictionaryWriter.write(String.valueOf(data[i]));
    }
    dictionaryWriter.close();
    dictionaryWriter.commit();
    List<Integer> sortIndex = Arrays.asList(ArrayUtils.toObject(expectedData.get(0)));
    List<Integer> invertedSortIndex = Arrays.asList(ArrayUtils.toObject(expectedData.get(1)));
    dictionarySortIndexWriter.writeSortIndex(sortIndex);
    dictionarySortIndexWriter.writeInvertedSortIndex(invertedSortIndex);
    dictionarySortIndexWriter.close();
    CarbonDictionarySortIndexReader dictionarySortIndexReader =
        new CarbonDictionarySortIndexReaderImpl(carbonTableIdentifier, dictionaryColumnUniqueIdentifier, storePath, CarbonTestUtil.configuration);
    List<Integer> actualSortIndex = dictionarySortIndexReader.readSortIndex();
    List<Integer> actualInvertedSortIndex = dictionarySortIndexReader.readInvertedSortIndex();
    for (int i = 0; i < actualSortIndex.size(); i++) {
      Assert.assertEquals(sortIndex.get(i), actualSortIndex.get(i));
      Assert.assertEquals(invertedSortIndex.get(i), actualInvertedSortIndex.get(i));
    }

  }

  /**
   * Method return the list of sortIndex and sortIndexInverted array
   *
   * @return
   */
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
    FileFactory.FileType fileType = FileFactory.getFileType(this.storePath);
    CarbonFile carbonFile =
        FileFactory.getCarbonFile(CarbonTestUtil.configuration, this.storePath, fileType);
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
